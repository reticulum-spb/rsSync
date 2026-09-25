# rrsync Reticulum interoperability specification

This document describes the Reticulum interaction and application wire protocol
for an independent Python implementation. Protocol version 1 is implemented by the
Rust client and server. Filesystem operations remain the responsibility of each
implementation; native Reticulum delivers encrypted, verified Resources.
Version 2 is opt-in and described in the final section; it adds persistent chunk
resume over the same native Link/Resource transport.

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
error, 8 configuration error, 9 busy export. The Rust server truncates error descriptions to 240
Unicode characters. Errors abort that Link's application session; earlier committed
files are not rolled back. Busy errors from other Links leave the active session
intact. Callers must fail the session on any ERROR or unexpected response; v2 may
start a fresh session after busy, under its reconnect policy.

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
run is the MVP recovery mechanism. Never retry COMMIT or FINISH blindly on the
same Link after a timeout. A lost COMMIT response may mean the file is already
installed; a lost FINISH response during push may mean deletion has completed.
During pull, the client must retain extras until it receives FINISH/OK. A new
session compares current manifests and skips files already installed.

After a server process restart, load the same persistent identity and register the
same `rrsync.sync` destination again. Previous Links and in-memory session state
are not restored. The client must establish a fresh authenticated Link and start
with START; unfinished files are transferred in full. Keep the same export root
to let the new manifest reflect previously committed files.

Wire mtimes retain seconds/nanoseconds even when the destination filesystem rounds
them. Report the actual stored metadata in the next manifest; do not pretend that
the requested precision was preserved. In v1, matching checksums do not override
an mtime mismatch, so a coarse-timestamp filesystem such as FAT may require the
same file again. No timestamp tolerance or filesystem capability negotiation is
defined by v1. Filesystem errors must not produce a successful commit response.

Protocol errors abort only the offending connection's session. Out-of-order or
repeated mutation commands are rejected; a busy request from a different Link
must not release the active export session. Discard file completions from another
connection or without a matching pending upload. Dropping an application task
without closing its Link relies on the server inactivity lease for cleanup.

Native Resource segmentation is not persistent
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

## Version 2 chunk extension

The Rust client selects v2 with YAML `protocol: 2`, which requires a `resume`
section containing an explicit `chunk_size`. A server with `resume` configured
accepts both `/rrsync/v1` and `/rrsync/v2`, irrespective of its outgoing `protocol`
setting. With no `resume`, v2 requests are rejected. Version 1 remains the default.

V2 uses request path `/rrsync/v2` and header `version:u8 = 2, tag:u8`.
The destination remains `rrsync.sync`. Version is pinned on the first authenticated
request for a Link and cannot change, including after FINISH. A rejected/busy
request does not release another Link's session. Both engines share one active
export session; separate v1 and v2 sessions may not mutate the export concurrently.
The response/error encoding follows the requested path's version. There is no
capability probing or fallback. Optional client reconnect is described below.

The client recalls the announced public key, verifies that its identity derives
the requested `rrsync.sync` destination, and validates native Link establishment
with that key. Its cache scope uses the remote identity hash from that verified
key. The server uses the authenticated client identity supplied by LinkManager.

All integers remain big-endian. START (1), MANIFEST (2), FINISH (7), OK (8) and
ERROR (9) keep the exact field layouts documented for v1, with version byte 2.
Legacy BEGIN/COMMIT/GET/VERIFIED tags 3–6 are invalid in v2. The control bound is
still 8 MiB and manifest limits, including the 134,217,727-byte whole-file limit,
remain unchanged for this extension. Native Resource segmentation is still owned
by Reticulum; an application chunk is one ordinary standalone Resource.

| Tag | Message | Fields after tag |
| --- | --- | --- |
| 10 | DESCRIBE | `index:u32`, `chunk_size:u32` |
| 11 | DESCRIPTION | `index:u32`, `description` |
| 12 | MISSING | `index:u32`, `count:u32`, `chunk:u32` repeated `count` times |
| 13 | CHUNK_BEGIN | `index:u32`, `chunk:u32` |
| 14 | CHUNK_COMMIT | `index:u32`, `chunk:u32`, `resource_hash:32 bytes` |
| 15 | CHUNK_GET | `index:u32`, `chunk:u32` |
| 16 | CHUNK_VERIFIED | `index:u32`, `chunk:u32` |
| 17 | FILE_COMMIT | `index:u32` |
| 18 | FILE_VERIFIED | `index:u32` |

`index` identifies a file entry in the source manifest, with the same indexing as
v1, and must be less than 16,384. The session must additionally require that the
entry is a pending file in its plan. `chunk` is a zero-based index in that file's
agreed description. Its absolute codec bound is 8,192, exclusive; the session must
also check it against the actual chunk count.

A `description` is:

```text
size:u64
chunk_size:u32
whole_sha256:32 bytes
chunk_count:u32
chunk_sha256:32 bytes repeated chunk_count times
```

`chunk_size` must be between 4,096 and 16,777,216 bytes inclusive. These are safety
bounds, not a performance recommendation; no default has been selected.
`chunk_count = ceil(size / chunk_size)` and must not exceed 8,192. Check geometry,
limits and available payload bytes before allocating the hash list. All chunks
except the last have exactly `chunk_size` bytes. The last contains the remainder,
or `chunk_size` when the division is exact. An empty file has zero chunks and the
SHA-256 of empty bytes. Hashes are SHA-256 of original uncompressed content, never
native Resource hashes. The source manifest size must match the description and
its checksum, when present, must match `whole_sha256`.

MISSING indices must be strictly increasing, unique and below the agreed chunk
count. Empty means all chunks are available, including for an empty file. The
codec enforces its absolute bounds; a sender must additionally validate MISSING
against its own description before transmitting anything.

### V2 transfer ordering

After START/MANIFEST, each pending file is handled sequentially. Only one chunk
Resource may be outstanding. Full/read/deny authorization and dry-run behavior
remain the same as v1; dry-run goes directly from manifest planning to FINISH.

For push:

1. The client snapshots the source and sends DESCRIPTION. The server validates
   it, opens receiver state bound to the authenticated identity and destination,
   rehashes cached chunks and replies MISSING.
2. For each missing chunk, the client sends CHUNK_BEGIN and waits for OK, then
   sends the exact chunk as a Resource without metadata. The receiver admits only
   the pending chunk's exact size on the owning Link.
3. The client sends CHUNK_COMMIT with the completed Resource hash. The server must
   bind that receipt to the pending file/chunk, verify size and SHA-256, persist
   the chunk and return OK only after successful publication and fsync. A native
   Resource proof alone does not mean the chunk is durably cached.
4. The client checks its source and sends FILE_COMMIT. The server revalidates and
   assembles all chunks, verifies the whole hash and installs the file before OK.
   This is the file completion boundary; chunk receipts never mark a file done.

For pull:

1. The client sends DESCRIBE with an explicit chunk size. The server snapshots the
   source and replies DESCRIPTION, keeping that snapshot for chunk reads. The
   client validates the description and rehashes its local receiver state.
2. For each locally missing chunk, the client sends CHUNK_GET. The server delivers
   OK before starting the chunk Resource. The client verifies and persists it,
   then sends CHUNK_VERIFIED and waits for OK before requesting another chunk.
3. After full assembly and whole-hash verification, the client sends FILE_VERIFIED.
   The server must check that its source remains unchanged before replying OK.
   The client then installs the assembled file. This follows v1's source-check
   boundary; loss of the reply must prevent local installation on that run.

FINISH remains responsible for successful session completion and extra-file
removal, never individual chunk commits. Empty/all-cached files still require the
appropriate FILE_COMMIT/FILE_VERIFIED boundary. Metadata is freshly taken from the
manifest, not from cache files. Cache validity does not depend on exact mtime and
therefore supports coarse-timestamp filesystems such as VFAT.

On reconnect or process restart, authenticate/authorize again, rescan, and exchange
fresh descriptions. The receiver reports only cached chunks that pass size/hash
checks. Do not persist/replay pending Resource receipts or session commands.
Changed identities, paths, source bytes or chunk sizes must not accidentally reuse
another transfer's state. A lost chunk or file acknowledgement is resolved by
fresh negotiation, not blind mutation replay. Receiver-state configuration and quotas
are described below. Timed retention is not implemented.

### Automatic session reconnect

The optional top-level `reconnect` policy is client-local and requires outgoing
protocol 2. `attempts` is the number of additional sessions (default 0, maximum 32).
`delay_seconds` defaults to 5; double the delay after each failure, capped by
`max_delay_seconds` (default 60). Delays must be positive, with initial <= maximum
and maximum <= 86400 seconds. `max_elapsed_seconds` defaults to 3600, must be
1–604800, and covers the initial attempt, waits and retries when enabled. This is
a cooperative asynchronous deadline; synchronous filesystem work is not preempted.

Retry connection loss, discovery/operation timeouts, exhausted native Resource
sender retries and ERROR code 9 (busy). Other remote codes, explicit access denial,
invalid Link proofs, protocol/hash/source-change errors and local I/O/config/cache
failures are terminal. Preserve numeric remote codes; do not classify human error
text. A peer that reports busy using legacy code 5 is treated as terminal. Native
Link closure may not reveal its reason, so indistinguishable closures consume the
bounded retry budget even if the peer rejected authentication.

Every attempt reopens the local root, rescans it, rediscovers/verifies the remote
identity, opens a new Link, identifies, and starts fresh negotiation with START.
Do not reuse in-memory manifests, pending receipts or mutation commands. Source
changes between attempts are reflected in the new scan. Preserve verified cache
blocks across failures. A lost FILE_COMMIT or FINISH acknowledgement is resolved
by this comparison of actual state. An exhausted deadline is not evidence that
the last mutation failed; a later invocation must also rescan.

Retain one Reticulum client runtime across attempts; this policy does not restart
the daemon. On cancellation, release local pending files/cache locks. Abrupt Link
deregistration may leave the server busy until its inactivity lease expires.

The engine receives authenticated peer identity and current permission from its
adapter on every request. An identity change or revoked write permission aborts
the owning session. Requests from another connection cannot release the active
session. One described file and one pending chunk are allowed; repeated BEGIN,
COMMIT or verification commands in the wrong state are rejected. File completion
cannot proceed while a chunk is pending; push additionally requires all missing
chunks to have been persisted. Pull permits FILE_VERIFIED without CHUNK_GET when
the receiver already has every chunk in its verified cache.

An error or disconnect releases pending files and cache locks while preserving
committed chunks. Lost close notifications rely on the inactivity lease; native
traffic should refresh it as in v1. Successful installation triggers best-effort
chunk eviction. Eviction failure is logged and does not undo file installation or
turn its acknowledgement into a failure. Empty cache lock directories remain.

### Receiver cache policy

Rust `resume.directory` defaults to `transfers` relative to the app configuration
directory. Resolve the root and cache to absolute paths and reject overlap before
connecting/serving. Create state lazily on a real receive, outside the sync tree.
Dry-run must not create a cache or acquire persistent cache locks.

The default limits are `max_bytes: 536870912` and `max_transfers: 128`. Under an
exclusive cache-wide lock, count existing payload/staging bytes and reserve the
space needed for missing chunks before accepting new data. Reject quota violations
without deleting another transfer's data. An additional per-transfer lock protects
the individual description. Hold both until file completion or session teardown;
process termination releases them. The byte limit excludes native Resource and
assembly/snapshot temporary storage and filesystem allocation overhead.

Directory-count limits include retained empty lock directories. Successful file
installation evicts its chunks; stale state and lock directories currently require
manual cleanup with all cache users stopped. No filesystem timestamps are used to
validate chunks or determine expiry. All adapter cache users must obey the same
cache-wide lock; do not mix unrestricted storage-library access with live sessions.

### Fixed v2 encoding examples

These examples are also checked by `tests/protocol_v2.rs`. Spaces/newlines below
are for readability and are not transmitted.

```text
DESCRIBE(index=7, chunk_size=4096):
020a0000000700001000

MISSING(index=7, chunks=[0,2,6]):
020c0000000700000003000000000000000200000006

CHUNK_GET(index=7, chunk=2):
020f0000000700000002

FILE_COMMIT(index=7):
021100000007

DESCRIPTION(index=7, content="hello", chunk_size=4096):
020b00000007000000000000000500001000
2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
00000001
2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
```
