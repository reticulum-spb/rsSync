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

## Resumable transfers

Version 1 remains the default. To send files in resumable chunks, configure both
peers with `resume` and select `protocol: 2` on the client:

```yaml
protocol: 2
resume:
  chunk_size: 1048576
  directory: transfers
  max_bytes: 536870912
  max_transfers: 128
  retention_seconds: 604800
```

The client selects `chunk_size` for both directions. It is required and has no
default; the example uses 1 MiB. Allowed sizes
are 4 KiB–16 MiB. The file limit is 134,217,727 bytes regardless of chunk size;
4 KiB chunks can therefore cover 32,768 chunks in one file. Choose a size appropriate for
your link; no universally optimal size is assumed. `directory` defaults to
`transfers`, relative to the application configuration directory; an absolute path
is also accepted. It must be outside the synchronized tree and cannot contain it.
The directory is created on the first real receive, never by dry-run.

A server with a `resume` section accepts both versions. The `protocol` setting
selects the version for outgoing push/pull and defaults to 1. Version 2 requires
`resume`; unsupported peers produce an error, with no automatic fallback. Both
versions share the server's single-session limit.

After an interruption, rerun the same command. The receiver verifies cached blocks
and requests only missing or damaged ones. Changed source content, chunk size,
peer identity or path starts separate state. Every received block and the assembled
file are checked with SHA-256, including when `--checksum` is absent. The flag
still controls the initial comparison of existing files. Logs report cached and
missing chunk counts per page. Hashes and missing indices are negotiated in pages
of at most 128 chunks; the full hash table is kept in an anonymous temporary file,
not an in-memory vector. Each cache subdirectory holds at most 256 chunk payloads.
Smaller chunks reduce retransmission after interruption but increase hashing,
control exchanges and disk operations. Delta transfer is not implemented.

Both peers must use the current paged v2 format. There is no unpaged-v2 fallback
or old-cache migration. Stop cache users and clear the old `resume.directory`
contents before upgrading from the flat cache format.

To reconnect automatically, add this top-level section to the client config
(requires `protocol: 2`):

```yaml
reconnect:
  attempts: 3
  delay_seconds: 5
  max_delay_seconds: 60
  max_elapsed_seconds: 3600
```

`attempts` counts additional sessions (0 by default, at most 32). Delay doubles
until `max_delay_seconds`; the defaults are 5 and 60 seconds. With retries enabled,
`max_elapsed_seconds` bounds the initial attempt, waits and subsequent sessions
(default 3600 seconds). This asynchronous deadline cannot interrupt synchronous
filesystem work. Delays must be positive, the maximum delay at least the initial
delay and at most 86400 seconds; the elapsed budget must be 1–604800 seconds.

Connection loss, discovery/operation timeouts and a busy export trigger a fresh
Link, authentication and directory scan. Explicit access denial, invalid protocol,
changed files, verification failures and local file/cache errors stop the command.
Each retry compares current state and verifies cached chunks; uncertain mutations
are never blindly replayed. The server may remain busy until its inactivity lease
expires, so allow enough retry time. This recovers application sessions through
the existing runtime; daemon restart recovery has not been tested.

`max_bytes` defaults to 512 MiB and limits logical cached payload/staging bytes,
reserving room for the missing blocks of each incoming file. It excludes filesystem
allocation overhead, lock/activity records, native Resource temporary files, temporary hash tables and assembled snapshots.
`max_transfers` defaults to 128 and limits retained transfer directories, including
empty lock directories after successful transfers. Both limits must be positive;
`max_transfers` must be below 16,384. Exceeding a limit fails without evicting other
transfers. One receiving transfer owns a cache directory at a time.

Successful installation removes cached blocks. `retention_seconds` defaults to
604800 (7 days); 0 disables automatic expiry. Before opening a receiving transfer,
rrsync removes expired state for other transfers, including abandoned staging files
and empty lock directories, before checking quotas. This is cleanup on use, not a
background timer: an idle cache is left alone, and dry-run never cleans it. The
currently requested transfer is retained and its chunks are verified for resume.

Activity is recorded atomically in file contents on open, successful chunk receipt,
clear and normal close. It does not depend on filesystem timestamps, including on
VFAT. Missing or malformed records get a full retention period on discovery;
future timestamps are retained until the clock catches up. A forward clock jump
can expire inactive cache early, requiring those bytes to be transferred again.
Active stores are protected by cache and transfer locks. Unexpected objects stop
cleanup of the affected transfer.

Stop all cache users before upgrading from versions without this locking scheme.
For manual cleanup, stop every rrsync process using that cache, remove its contents,
then restart. Never remove lock files while a cache user is running. Use a private
cache location (appropriate mount permissions on VFAT).

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
files. Version 1 retransmits the unfinished file; version 2 reuses its verified
cached blocks. After an abrupt client exit, wait for
the server inactivity timeout before retrying. If a commit or final response is
lost, the receiving side may already have completed that operation. Rerun the
synchronization to compare actual directory contents; commands are not automatically
replayed after an uncertain result. Source changes detected during a run
cause failure. Optional v2 reconnect performs that fresh synchronization automatically.
Delta transfer is not implemented.

After a server process exits, restart `serve` with the same configuration directory
and export directory. An enabled v2 reconnect can recover within its retry budget;
otherwise rerun the client command. The saved identity preserves
the server destination; completed files remain available for the new comparison.

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

On Linux VFAT (FAT with long filenames), file replacement uses the same
same-directory staging and rename operations. The filesystem must support file
and directory `fsync` and advisory locks; errors are reported rather than ignored.
FAT rounds modification times to two-second precision, so exact source mtimes
cannot be preserved and unchanged files can be transferred again, including with
`--checksum` under the current comparison rules. Use names representable on the
destination filesystem and avoid names differing only in case. Atomic replacement
during normal operation does not guarantee recovery from power loss on FAT.
