# rrsync

rrsync synchronizes directory contents over Reticulum on Linux, including ARM.
It connects to an **already running rnsd-rs shared instance**. The application
has a server role exporting one directory and a client role performing push or
pull. Synchronization is one-way.

## Configuration

`--config DIRECTORY` selects a configuration directory. The default is
`~/.rsSync`, containing `config.yaml` and the persistent private-key file `identity`.
On a normal first run the default directory, a minimal configuration and an identity
are created automatically. Existing files are preserved. An explicitly selected
directory must already contain `config.yaml`; its identity is created if missing.
Dry-run requires existing configuration and identity and creates neither.

The minimal configuration denies incoming access to everyone. Client push/pull
commands are authorized by the remote server, not by the client's own `permits`:

```yaml
permits:
  - others: deny
```

Optional settings:

```yaml
reticulum_config: /home/user/.rsReticulum
timeout_seconds: 600
announce_seconds: 600
permits:
  - "0123456789abcdef0123456789abcdef": full
  - "fedcba9876543210fedcba9876543210": read
  - others: deny
```

The Reticulum configuration directory belongs to the already running daemon and is
independent of the application directory. Omit `reticulum_config` to use the
Reticulum library's default. The identity filename is fixed; no `identity` setting
is supported in YAML. Unknown configuration keys and invalid permissions cause
startup to fail. Configuration changes take effect after restarting the command.

`permits` is an ordered list of single-entry mappings. Address keys are client
identity hashes (32 hexadecimal characters), not server destination hashes.

- `full`: allow push and pull.
- `read`: allow pull only, including pull with `--delete`; reject all push requests,
  including dry-run. Pull deletion only affects the client's local destination.
- `deny`: reject access.

Address rules are checked in order and the **first matching address wins**.
`others` is used only if no specific address matches, regardless of its position.
It accepts the same three permissions and can grant access to otherwise unlisted
identities. If repeated, the first `others` rule wins. With no match and no `others`,
access is denied. Every peer must authenticate, including peers covered by `others`.

`timeout_seconds` defaults to 600: it sets client operation deadlines and the
server's inactivity timeout for abandoned sessions. Increase it for slow transfers.
`announce_seconds` defaults to 600 and controls the server announcement interval.
Both values must be positive integers.

Display the client identity address to add to the server configuration. This command
does not require a daemon connection:

```sh
rrsync identity
rrsync --config /path/to/client-config identity
```

To prepare an alternative configuration directory, save the example as
`config.yaml`, then generate its identity:

```sh
mkdir -p ./client-config
cp rrsync.example.yaml ./client-config/config.yaml
rrsync --config ./client-config identity
```

## Synchronization

On the client, run `rrsync identity`. On the server, run the same command to initialize
its own configuration, then add the **client identity hash** to the server's
`~/.rsSync/config.yaml` with `full` or `read` access. The existing rnsd-rs must be
running on each host before serving or synchronizing.

Start the server with an existing export directory:

```sh
rrsync serve /srv/files
```

Copy the server's printed destination hash into these commands, replacing
`<destination>`:

```sh
rrsync push ./data '<destination>:/backup'
rrsync pull '<destination>:/backup' ./download
rrsync push --dry-run --delete ./data '<destination>:/backup'
rrsync push --checksum --delete ./data '<destination>:/backup'
```

Use `--config /path/to/config-directory` with any command to select another
application configuration. Client and server processes on the same host can use
separate configuration directories and identities while sharing the same daemon.

The server prints its destination address. `/backup` refers to a directory inside
its export root. The contents of `./data` are copied into that directory; the name
`data` is not added. Missing destination directories are created. Source directories
and the server export root must exist. `<destination>:/` refers to the export root
itself. Remote paths cannot contain `.` or `..` components, repeated slashes or a trailing
slash after a directory name. A server accepts one synchronization session at a
time; another simultaneous session receives a busy error.

Both push and pull print the plan before transferring data, using `Skip`, `Mkdir`,
`Create`, `Update` and `Delete` entries.

- `--dry-run` prints the plan without creating, replacing or deleting synchronized
  objects. An existing identity and initialized Reticulum configuration/storage
  are required so dry-run does not initialize persistent application state.
- `--delete` also removes destination entries absent from the source, after all
  files have transferred successfully. Without it, extra entries are retained and
  conflicts between files and directories are errors.
- `--checksum` checks SHA-256 content hashes in addition to path, size and mtime.
- `-v` / `--verbose` enables diagnostic logging.

Regular files and directories, including empty files, empty directories and
Unicode names, are supported. Modification times of synchronized files and
subdirectories are preserved; the synchronization root's own mtime is not copied.
By default, regular files are skipped when path, type, size and mtime (including
nanoseconds) match. Content changes with identical size and mtime require
`--checksum` to detect. Changed files are sent in full using bounded memory.

Symlinks and special objects are skipped with warnings and never followed.
Corresponding paths and descendants are protected from deletion. Paths beginning
with `.rrsync-` in any component are reserved for temporary files and are also
protected. These exclusions apply on both sides; an excluded destination path is
not overwritten. Non-UTF-8 filenames cause an error. Other Unix metadata,
permissions, ownership, ACLs and xattrs are not copied.

## Failure behavior and limits

Received files are verified in temporary storage before installation. Replacing a
regular file is atomic. An interrupted transfer leaves the previous file intact.
A failed run may have installed earlier files; rerunning skips completed unchanged
files and retransmits the unfinished file. After an abrupt client exit, wait for
the server inactivity timeout before retrying. Source changes detected during a run
cause failure. Chunk resume, delta transfer and automatic session reconnection are
not implemented.

With `--delete`, replacing a directory by a file first receives and verifies the
file, then removes the conflicting directory contents. This type change is not an
atomic directory transaction. Replacing a file by a directory also requires removal
of the conflicting file. Protected objects prevent destructive type replacement.

Individual files are limited to 134,217,727 bytes by the current Reticulum library.
Each scanned directory tree is limited to 16,384 entries and its
encoded control message to 8 MiB. Relative paths must be UTF-8, at most 4,096 bytes,
with at most 128 components, each no longer than 255 bytes. Linux 5.6 or newer
and a mounted `/proc` are required. Failed runs
exit with a nonzero status. Abrupt process termination can leave reserved temporary
files; remove them manually when no synchronization is running.
