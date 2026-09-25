# rrsync Reticulum interoperability specification

This document specifies the Reticulum carriers, binary messages and session
semantics required for interoperable rrsync implementations. Version 1 transfers
whole files; version 2 adds persistent chunk resume over the same native
Link/Resource transport. Local configuration, storage layouts, diagnostics,
benchmarks and development status are outside this specification.

## Reticulum identity and destination

Both application roles attach to an existing shared Reticulum instance. The
application destination and wire protocol do not depend on the local daemon
endpoint or configuration directory.

Each application uses a persistent Reticulum Identity; private keys are never
transmitted. The server destination is inbound SINGLE with full name `rrsync.sync`.
In Python terms: `RNS.Destination(identity, RNS.Destination.IN,
RNS.Destination.SINGLE, "rrsync", "sync")`. Announce on startup and periodically
with no application data. Respond to native path requests. Announce intervals are
local policy and are not negotiated by this protocol.

Clients use a 16-byte destination hash, discover its path/public identity,
establish a Link and identify with their persistent client identity. The server authorizes the authenticated remote identity hash: the first
16 bytes of SHA-256 of the 64-byte public identity key. This is **not** the client's
application destination hash. Authorization is evaluated against that authenticated
identity. Full access permits push and pull, read access permits pull only, and
denied or unauthenticated peers cannot access the export. The choice of identities
and fallback permissions is server-local policy.

Reject unauthorized application packets and Resource advertisements before accepting
their data. Ordinary incoming Resources are admitted only for the pending push
file, with its exact advertised size and no metadata. Recheck identity and operation
authorization on every application request.

## Control and data carriers

Use standard Reticulum Link requests to path `/rrsync/v1`. Python uses
`link.request("/rrsync/v1", data=encoded_bytes, ...)` and a matching request handler.
The request path hash is Reticulum's truncated SHA-256 of the UTF-8 path. Native
request IDs and request/response envelopes are supplied by Reticulum; they are not
part of the binary application payload below. Large requests and responses are
carried using native request/response Resources automatically.

File bytes are **ordinary standalone Resources** on the same Link, with no metadata,
no application framing. Send with automatic compression disabled. Receivers must
accept valid native compressed Resources subject to uncompressed size limits. Native Resource hashes/proofs are
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
export root; empty means the export root itself. Absolute paths, empty internal
components, `.`, `..`, NUL and components exceeding 255 bytes are invalid.

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
error, 8 configuration error, 9 busy export. Error text is diagnostic; use the
numeric code for classification. Errors abort that Link's application session;
earlier committed files are not rolled back. Busy errors from other Links leave the active session
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
Even an unchanged tree exchanges complete manifests in both directions. Directory
entries count toward the 16,384-entry bound. Version 2 hash pagination applies to
chunks within a file; it does not paginate or suppress this manifest exchange.

A regular file is skipped if path, type, size, seconds and nanoseconds match, and
in checksum mode both SHA-256 hashes also match. Other regular files are transferred.
Create missing directories. Without delete, type conflicts fail and extra entries
remain. With delete, type conflicts may be replaced; extra entries are removed only
after all required transfers succeed. Protected paths on either side exclude that
path and all descendants. A destination directory containing a protected descendant
cannot be removed or replaced. Directory mtimes are restored after file operations,
from deepest to shallowest. Protected-entry mtimes do not participate in comparison.

Dry-run uses START/MANIFEST then FINISH/OK only. Do not create destination roots,
temporary receive files or other persistent state during dry-run.

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

Operation deadlines are local policy and are not exchanged on the wire.
Reconnecting to the shared Reticulum instance does not restore an application
session; establish a new authenticated Link and start with START.

Regular-file installation must atomically replace the destination only after
validation. Detect source changes before/after snapshotting and after transfer;
only size and mtime travel in the manifest. Local source-stability checks do not
provide a filesystem-wide snapshot.
Directory-to-file type replacement is a verified-file-then-remove operation and
cannot provide the same atomic guarantee as replacing a regular file. A run is not
an atomic transaction covering the entire tree.

## Python callback mapping

The following Reticulum API hooks carry the messages and state transitions above:

- `destination.register_request_handler("/rrsync/v1", response_generator=...,
  allow=..., allowed_list=...)`. The generator receives `(path, data, request_id,
  link_id, remote_identity, requested_at)` and returns the encoded response bytes.
  Apply operation authorization even when a native allow-list is also used.
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

## Version 2 chunk extension

A server may support both versions on the same destination. The client selects
the version by request path; support for v2 must be enabled at the receiving peer.

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

Only the paged message layouts below are defined for version 2.

| Tag | Message | Fields after tag |
| --- | --- | --- |
| 10 | DESCRIBE | `index:u32`, `chunk_size:u32` |
| 11 | DESCRIPTION | `index:u32`, `size:u64`, `chunk_size:u32`, `whole_sha256:32 bytes` |
| 12 | MISSING | `index:u32`, `start:u32`, `page_count:u32`, `missing_count:u32`, `chunk:u32` repeated `missing_count` times |
| 13 | CHUNK_BEGIN | `index:u32`, `chunk:u32` |
| 14 | CHUNK_COMMIT | `index:u32`, `chunk:u32`, `resource_hash:32 bytes` |
| 15 | CHUNK_GET | `index:u32`, `chunk:u32` |
| 16 | CHUNK_VERIFIED | `index:u32`, `chunk:u32` |
| 17 | FILE_COMMIT | `index:u32` |
| 18 | FILE_VERIFIED | `index:u32` |
| 19 | HASHES_GET | `index:u32`, `start:u32` |
| 20 | HASHES | `index:u32`, `start:u32`, `count:u32`, `chunk_sha256:32 bytes` repeated `count` times |

`index` identifies a pending file entry in the source manifest and must be less
than 16,384. `chunk` is an absolute zero-based u32 index within that file, not a
page-relative index. Check its range against the negotiated page and description
in the session. There is no independent 8,192-chunk limit.

DESCRIPTION is only the file header: no chunk count or table follows its hash.
Derive `chunk_count = ceil(size / chunk_size)`. `chunk_size` is 4,096–16,777,216
bytes, and `size` must not exceed 134,217,727. Thus even
4 KiB chunks support the full current file limit (32,768 chunks). All chunks except
the last have exactly `chunk_size` bytes; the last holds the remainder or a full
chunk when division is exact. Empty files have zero chunks and SHA-256(empty).
Manifest size and optional checksum must match the header. Hashes cover original
uncompressed content, not native Resource framing.

Hash pages have fixed boundaries: `start` must be a multiple of 128, strictly
below `chunk_count`, with `count = min(128, chunk_count - start)`. Negotiate pages
in increasing order starting at zero, without gaps or repetition. Check count and
available bytes before allocating; a HASHES message is at most 4,110 bytes. Codec
checks page alignment/count/overflow; the session additionally checks exact geometry
and expected offset. MISSING must echo the page's index, start and count. Its
indices are strictly increasing, unique and inside `[start, start + page_count)`;
empty means that entire page is already cached. MISSING is at most 530 bytes.
No empty hash pages are sent, including for empty files.

Each negotiated page contains at most 128 hashes. The complete set of negotiated
hashes must remain available for final content validation; their local storage
representation is not part of the protocol.

### V2 transfer ordering

After START/MANIFEST, handle pending files sequentially. Permissions and dry-run
remain as in v1; dry-run goes directly from planning to FINISH.

For push:

1. Snapshot the source; send DESCRIPTION. The server validates the header, opens
   scoped receiver state and replies OK, without an unpaged missing list.
2. Send HASHES for the next page. The server validates its sequence/geometry,
   rehashes cached chunks in that page and returns MISSING for that page only.
3. For each missing index, send CHUNK_BEGIN, wait for OK, send the exact bytes as
   one Resource without metadata, then send CHUNK_COMMIT with its native Resource
   hash. The server binds the receipt to that pending chunk, verifies size/SHA-256,
   durably publishes the payload before OK. Resource proof alone is insufficient.
4. Finish every missing chunk before the next HASHES. After all pages, check the
   source and send FILE_COMMIT. The server revalidates all chunks, assembles and
   verifies the whole hash, installs the file, then replies OK. Early FILE_COMMIT
   or a next page while a chunk remains missing/pending is rejected.

For pull:

1. Send DESCRIBE with the chunk size. The server snapshots the source and returns
   DESCRIPTION, retaining the snapshot and its chunk hashes. Validate the header
   and open local scoped receiver state.
2. Send HASHES_GET for the next page. Validate the HASHES response's index, start
   and exact count; store its hashes and rehash local chunks in that page.
3. For each missing chunk, send CHUNK_GET. The server replies OK before advertising
   the Resource. Verify and persist it, then send CHUNK_VERIFIED and wait for OK.
   Requests outside the current page or repeated indices are rejected.
4. Request the next page only after all current chunk receipts. After negotiating
   every page, assemble and verify the whole file, then send FILE_VERIFIED. The
   server checks its source is unchanged before OK; only then install locally.
   Early FILE_VERIFIED and changing pages with an outstanding Resource are rejected.

Empty files skip page exchange but still use DESCRIPTION and the appropriate
FILE_COMMIT/FILE_VERIFIED boundary. All-cached files still negotiate every page.

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
fresh negotiation, not blind mutation replay. Requirements for reusable receiver
state are specified below.

### Automatic session reconnect

Automatic retries are optional local policy; no reconnect command or retry
schedule is negotiated. Connection loss, discovery/operation timeouts, exhausted
native Resource retries and ERROR code 9 (busy) may be handled by starting a new
session. Other remote error codes, explicit access denial, invalid Link proofs,
protocol/hash/source-change errors and local failures terminate the operation.
Use numeric ERROR codes, not diagnostic text, for this decision. If a Link closes
without revealing a reason, any retry must still perform fresh authentication
and authorization.

Every attempt reopens the local root, rescans it, rediscovers/verifies the remote
identity, opens a new Link, identifies, and starts fresh negotiation with START.
Do not reuse in-memory manifests, pending receipts or mutation commands. Source
changes between attempts are reflected in the new scan. Preserve verified cache
blocks across failures. A lost FILE_COMMIT or FINISH acknowledgement is resolved
by this comparison of actual state. An exhausted deadline is not evidence that
the last mutation failed; a later invocation must also rescan.

On cancellation, release the local pending transfer state. Abrupt Link closure
may leave the server busy until its inactivity lease expires.

Each request must be associated with its authenticated peer identity and current
permission. An identity change or revoked write permission aborts
the owning session. Requests from another connection cannot release the active
session. One described file and one pending chunk are allowed; repeated BEGIN,
COMMIT or verification commands in the wrong state are rejected. File completion
cannot proceed while a chunk is pending; push additionally requires all missing
chunks to have been persisted. Pull permits FILE_VERIFIED without CHUNK_GET when
the receiver already has every chunk in its verified cache.

An error or disconnect releases pending session state while preserving committed
chunks for resume. Lost close notifications rely on the inactivity lease; native
traffic refreshes it as in v1. After successful installation, local cache cleanup
must not undo the installed file or invalidate its acknowledgement.

### Application control cost per described file

Let N be the chunk count, P = ceil(N / 128), and K the count actually transferred
after cached chunks are checked. Summing the current encoded messages in both
directions (including OK responses) gives:

- Push: `60 + 32*P + 32*N + 60*K` bytes.
- Pull: `68 + 24*P + 32*N + 24*K` bytes.
- Either direction: `2 + P + 2*K` request/response exchanges.

This covers DESCRIPTION/DESCRIBE, all hash/missing pages, chunk commands and final
file verification. It excludes START/MANIFEST/FINISH, Link setup/identification,
Resource framing/proofs, encryption, transport headers and retransmissions. File
payload bytes are separate. These are application-codec sizes, not measured radio
traffic or a latency prediction. K=0 describes a complete cache for a file still
requiring installation; an unchanged installed file is normally skipped by planning.

### Reusable receiver state

Cached chunks are local state, not a shared storage format. Bind reusable data to
the authenticated peer, transfer direction, target root/path, file size, chunk size
and whole-file hash. Reauthenticate and negotiate a fresh description and hash
pages for each new session. A quota reservation, receipt journal or stored bitmap
alone is not evidence of valid data.

Before reporting a chunk as present in MISSING, verify its exact length and SHA-256
against the freshly negotiated page. Otherwise report it as missing. Preserve the
chunk publication and acknowledgement ordering specified above; native delivery
proof alone is not a durable application receipt. Before file installation, verify
all chunks and the assembled whole-file hash.

Cache eviction or loss may cause retransmission, but must not allow unverified
bytes to be reused or change file-commit semantics. Storage layout, cache keys,
locking primitives, quotas and expiry scheduling are implementation-local and are
not exchanged or negotiated. The peers do not need to share a cache format.

### Fixed v2 encoding examples

Spaces/newlines below are for readability and are not transmitted.

```text
DESCRIBE(index=7, chunk_size=4096):
020a0000000700001000

HASHES_GET(index=7, start=128):
02130000000700000080

MISSING(index=7, start=128, page_count=3, chunks=[128,130]):
020c000000070000008000000003000000020000008000000082

CHUNK_GET(index=7, chunk=32767):
020f0000000700007fff

FILE_COMMIT(index=7):
021100000007

DESCRIPTION(index=7, content="hello", chunk_size=4096):
020b00000007000000000000000500001000
2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824

HASHES(index=7, start=0, hashes=[SHA256("hello")]):
0214000000070000000000000001
2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
```
