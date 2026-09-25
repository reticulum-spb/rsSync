# rrsync Reticulum interoperability specification

This document describes the Reticulum interaction and application wire protocol
for an independent Python implementation. Protocol version 1 is implemented by the
Rust client and server. Filesystem operations remain the responsibility of each
implementation; native Reticulum delivers encrypted, verified Resources.

## Runtime, identity and destination

Both application roles attach as clients to an **existing shared rnsd-rs**. They do
not start a daemon or configure radio/network interfaces. The Rust dependency is
`rns-runtime` with `default-features = false, features = ["client"]`, from the local
`../rsReticulum` checkout. `reticulum-client` names this operating mode, not a crate.
The shared endpoint is taken from the Reticulum `config.yaml` (Unix abstract socket
or loopback TCP). Normal client startup may initialize local configuration/storage;
dry-run requires those paths to exist already.

The application configuration directory is selected with `--config DIRECTORY`,
defaulting to `~/.rsSync`. It contains `config.yaml` and the fixed private-key file
`identity`. Normal first startup bootstraps the default directory with
`permits: [{others: deny}]` and a new identity; dry-run requires existing files.
Explicit configuration directories require an existing `config.yaml`.

Each application keeps a persistent Reticulum Identity. Identity files are raw
64-byte private keys, compatible with Python RNS.Identity. Never transmit these
private keys. The server destination is inbound SINGLE with full name `rrsync.sync`.
In Python terms: `RNS.Destination(identity, RNS.Destination.IN,
RNS.Destination.SINGLE, "rrsync", "sync")`. Announce on startup and periodically
(default 600 seconds), with no application data. Respond to native path requests.

Clients accept a 16-byte destination hash (32 hex characters in the CLI), discover
its path/public identity, establish a Link and identify with their persistent client
identity. The server authorizes the authenticated remote identity hash: the first
16 bytes of SHA-256 of the 64-byte public identity key. This is **not** the client's
application destination hash. The YAML `permits` list contains single-entry mappings from an identity hash to
`full`, `read` or `deny`. Evaluate specific address rules in order; the first match
wins. If there is no specific match, use the first `others` rule regardless of its
position. With neither a match nor `others`, deny access. `full` admits push and
pull; `read` admits pull only; `deny` rejects the peer. `others` may grant either
full or read access to authenticated identities not explicitly listed. Denied or
unauthenticated peers cannot access the export. The server filters unauthenticated
application packets and Resource advertisements
before passing them to the native manager. Ordinary incoming Resources are admitted
only for the pending push file, with its exact advertised size and no metadata.
At most 16 inbound Links are admitted. The Rust manager
uses its synchronous identity gate and checks its authenticated identity map again
on every application request.

## Control and data carriers

Use standard Reticulum Link requests to path `/rrsync/v1`. Python uses
`link.request("/rrsync/v1", data=encoded_bytes, ...)` and a matching request handler.
The request path hash is Reticulum's truncated SHA-256 of the UTF-8 path. Native
request IDs and request/response envelopes are supplied by Reticulum; they are not
part of the binary application payload below. Large requests and responses are
carried using native request/response Resources automatically. The Rust adapter uses
`LinkSession::request_with_metadata_limit` and `LinkManager::set_request_handler_ex`.

File bytes are **ordinary standalone Resources** on the same Link, with no metadata,
no application framing and automatic compression disabled by the Rust sender. Use
streaming/file-backed Resource APIs. Receivers must accept valid native compressed
Resources subject to uncompressed size limits. Native Resource hashes/proofs are
not SHA-256 hashes of the original file; do not substitute one for the other.

No Channel message types, custom packet acknowledgements, packet fragmentation or
application retransmission are defined. Requests execute sequentially, with one
file Resource outstanding per synchronization session. Reticulum handles Resource
segmentation, proofs and recovery within the Link. A filesystem commit response
has different meaning from a Resource delivery proof.

## Binary encoding

All integers are fixed-width **big-endian**. Each application request and response
starts with `version:u8 = 1`, then `tag:u8`. Reject other versions, unknown tags,
unknown flag bits, invalid booleans, truncated fields and trailing bytes. There is
no magic prefix or application length prefix; Reticulum supplies payload framing.

`string` means `byte_length:u16` followed by exactly that many UTF-8 bytes. The
maximum is 4,096 bytes. A boolean is exactly 0 or 1. `index` is a zero-based `u32`
index into the **source manifest**, including directory and protected entries.

| Tag | Message | Fields after tag |
| --- | --- | --- |
| 1 | START | `push:u8`, `flags:u8`, `path:string`, `manifest` |
| 2 | MANIFEST | `manifest` |
| 3 | BEGIN | `index:u32` |
| 4 | COMMIT | `index:u32`, `resource_hash:32 bytes` |
| 5 | GET | `index:u32` |
| 6 | VERIFIED | `index:u32` |
| 7 | FINISH | none |
| 8 | OK | none |
| 9 | ERROR | `code:u8`, `text:string` |

START flags: bit 0 = delete, bit 1 = checksum, bit 2 = dry-run. `push` is 1 for
client-to-server and 0 for server-to-client. All START paths are relative to the
export root; empty means the export root itself. A CLI leading `/` is removed once
before encoding. Wire absolute paths, empty internal components, `.`, `..`, NUL and
components exceeding 255 bytes are invalid.

A manifest is `entry_count:u32`, followed by this many entries:

```
path:string
kind:u8           # 0 regular file, 1 directory, 2 protected exclusion
size:u64          # source byte count for files; zero otherwise
mtime_seconds:i64 # Unix timestamp, signed, two's complement
mtime_nanoseconds:u32 # 0..999999999
has_hash:u8       # boolean
hash:32 bytes     # present only when has_hash = 1; SHA-256 of file contents
```

Entries are strictly sorted by UTF-8 path bytes, unique, and include all ancestor
directories before their children. The root itself is not an entry. A protected
entry stands for an unsupported object or reserved `.rrsync-` name; no children
may follow under it. SHA-256 is mandatory for every regular file in checksum mode.
Nanosecond mtime comparison is exact. Hashes do not replace size/mtime comparison.

Limits: 16,384 entries, 8,388,608 application control bytes, 4,096 path bytes,
128 directory levels, 134,217,727 bytes per file Resource. Error text is at most
1,024 UTF-8 bytes. Validate lengths/counts before allocation; reject invalid
manifest parents, unsorted/duplicate paths and oversized files. Native control
request/response Resource limits allow 1,024 extra bytes for Reticulum envelopes.

Error codes: 1 permission denied, 2 invalid path, 3 changed source/destination,
4 file hash mismatch, 5 protocol/plan/state error, 6 filesystem error, 7 transport
error, 8 configuration error. The Rust server truncates error descriptions to 240
Unicode characters. Errors abort that Link's application session; earlier committed
files are not rolled back. Busy errors from other Links leave the active session
intact. Callers must fail the run on any ERROR or unexpected response.

Golden payloads (hex): `0107` FINISH; `0108` OK; `010300000002` BEGIN index 2;
`010200000000` empty MANIFEST.

## Session and plan

Before scanning an export for START, check operation authorization: push requires
`full` even with the dry-run flag; pull accepts `full` or `read`. Unauthorized START
returns permission-denied without creating a session or touching the export. Read
access must never authorize a later BEGIN/COMMIT or incoming file Resource: the
session direction and pending plan remain mandatory checks.

START always carries the **client's local manifest**. In push this is the source;
in pull this is the destination. The server scans the requested export subtree,
builds/validates a plan, and returns its own manifest in MANIFEST. Both peers derive
the same plan; no separate plan is transmitted. Finish plan construction before
any data transfer. Only one active session is admitted per exported server root.

A regular file is skipped if path, type, size, seconds and nanoseconds match, and
in checksum mode both SHA-256 hashes also match. Other regular files are transferred.
Create missing directories. Without delete, type conflicts fail and extra entries
remain. With delete, type conflicts may be replaced; extra entries are removed only
after all required transfers succeed. Protected paths on either side exclude that
path and all descendants. A destination directory containing a protected descendant
cannot be removed or replaced. Directory mtimes are restored after file operations,
from deepest to shallowest. Protected-entry mtimes do not participate in comparison.

Dry-run uses START/MANIFEST then FINISH/OK only. Do not create destination roots,
temporary receive files or persistent identities during dry-run. Native runtime
startup must also avoid initialization writes by requiring existing local state.

## Push exchange

1. Client identifies, sends START with `push=1`, and receives MANIFEST.
2. For each planned file, in manifest order, send BEGIN(index); wait for OK.
   The server checks state and creates necessary destination directories.
3. Send the file as one ordinary native Resource (possibly multiple native
   segments). Wait for native completion/proof. The server receives it into
   temporary file-backed storage, not the final destination.
4. Check that the source has not changed during the transfer. Send
   COMMIT(index, original_resource_hash); wait for OK. For split Resources this is
   the native **first segment's resource hash**, repeated as original_hash in later
   advertisements. It is not a hash computed over concatenated segment hashes.
5. The server matches the pending index, size, missing metadata and Resource hash,
   verifies the optional content hash, stages the file on the destination
   filesystem, sets mtime and atomically replaces a regular-file target. It returns
   OK only after installation. An unsolicited or mismatched Resource cannot be
   installed.
6. After all files, check source stability again. Send FINISH. The server requires
   every planned transfer to be committed, removes extra entries and restores
   directory mtimes, then returns OK and releases the session.

## Pull exchange

1. Client identifies, sends START with `push=0` and its destination manifest, and
   receives the source MANIFEST. The client builds the plan and prepares directories.
2. For each planned file send GET(index). The server checks source stability and
   creates a file-backed snapshot. Respond OK first, then send an ordinary Resource
   containing snapshot bytes on the same Link.
3. The client receives a file-backed Resource with a size bound equal to the
   manifest size, and rejects metadata or a different size. Send VERIFIED(index).
   The server checks the original source metadata again and returns OK.
4. The client verifies the optional SHA-256 and installs the file, setting mtime.
5. After all files send FINISH. The server checks source stability, returns OK and
   releases the session. Only then may the client delete extras and restore
   directory mtimes.

Python server implementations must deliver the GET response before starting the
Resource advertisement. Do not put file bytes in the request response itself.
The Rust server uses `RequestOutcome::ReplyWithFile`; push uses
`LinkSession::send_resource_reader`; both receiving roles use file-backed APIs.

## Failure and recovery

Close the Link on completion or failure. A closed Link releases server session and
pending receive state. A timeout or lost final response is not proof that a commit
failed: reconnect and rescan instead of blindly replaying mutations. Repeating a
run is the MVP recovery mechanism. Native Resource segmentation is not persistent
chunk resume. No offsets, application chunk IDs, delta hashes or resume requests
are defined by v1.

The server also expires sessions after a configurable interval without application
data or Resource traffic, closing the abandoned Link. Keepalives alone do not renew
a session. After an abrupt client exit, a new run may need to wait for this lease.

Use configurable operation deadlines (default 600 seconds). These are total
operation deadlines, so very slow links may require longer values. Shared-instance
reconnection belongs to rsReticulum; it does not resurrect the application session.

Use same-directory staging and atomic rename for regular-file installation. Detect
source changes with metadata checks before/after snapshotting and after transfer;
Rust compares inode, device, size, mtime and ctime locally (only size and mtime travel
in the manifest). This is best-effort detection, not a filesystem-wide snapshot.
Directory-to-file type replacement is a verified-file-then-remove operation and
cannot provide the same atomic guarantee as replacing a regular file. A run is not
an atomic transaction covering the entire tree.

## Python callback mapping

The local Python Reticulum implementation exposes these corresponding hooks:

- `destination.register_request_handler("/rrsync/v1", response_generator=...,
  allow=..., allowed_list=...)`. The generator receives `(path, data, request_id,
  link_id, remote_identity, requested_at)` and returns the encoded response bytes.
  Apply the ordered permits policy even when a native allow-list is also used.
- `link.set_remote_identified_callback(callback)` receives `(link, identity)`.
  Install authentication and session policy before accepting application traffic.
- Set `RNS.Link.ACCEPT_APP` and a `link.set_resource_callback(...)` admission
  callback, not unconditional ACCEPT_ALL. Check the expected session, direction,
  pending index, uncompressed size and absence of metadata at advertisement time.
  The pull client must install its Resource admission callback before issuing GET.
- Send with `RNS.Resource(file_object, link, metadata=None, auto_compress=False,
  callback=...)`, holding the binary file object alive until completion. Verify
  `RNS.Resource.COMPLETE` before sending COMMIT. Keep the original first-segment
  hash for the COMMIT field rather than the current segment hash.
- Use `link.set_resource_concluded_callback(...)` to consume received file data.
  The Python implementation exposes `resource.data` as a binary file during the
  completion callback and closes/removes native temporary storage afterwards.
  Copy into application-owned staging storage or duplicate the descriptor during
  that callback; do not retain the borrowed file object for a later COMMIT request.
  Verify completion status and optional SHA-256 before installing a destination.
- Request response callbacks receive a receipt; decode its response bytes with
  the codec above. Resource callbacks may run on different threads, so serialize
  application state transitions per Link and protect the server export session.

These mappings describe the intended Python port; interoperability with a Python
rrsync implementation has not yet been tested. The currently validated peers are
Rust client/server processes attached to the existing rnsd-rs shared instance.
