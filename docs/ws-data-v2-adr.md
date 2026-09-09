# ADR: bounded WebSocket data-lane multiplexing and fragmentation

Status: accepted

## Context

The WebSocket data lane must carry interactive checks and multi-megabyte object packs without
letting one bulk transfer monopolize the event loop, socket, or transient-memory budget. The
existing OBW1 framing is deployed and must remain byte-for-byte compatible with clients and
servers that do not advertise the new wire version.

This ADR covers only the authenticated, AEAD-sealed data lane. Content hashes, object encoding,
HTTP bulk semantics, and commit idempotency do not change.

## Decision

OBW2 is an explicitly negotiated inner frame format. A client selects it only when the
authenticated capability bundle contains both `ws-data-v1` and `ws-data-v2`. Every connection
attempt obtains a fresh one-use ticket and a fresh transport session before opening the socket.
There is no in-band version switch on a live socket.

### Handshake and limits

The OBW2 HELLO and HELLO_ACK payloads are exactly 32 bytes. Their magic values are `OWH2` and
`OWA2`, respectively. All integers are little-endian.

| Bytes | Field |
| --- | --- |
| 0..4 | Magic |
| 4..6 | Version, exactly 2 |
| 6..8 | Maximum in-flight requests, `u16` |
| 8..12 | Maximum logical payload bytes, `u32` |
| 12..16 | Reserved, exactly zero |
| 16..24 | Maximum in-flight bytes, `u64` |
| 24..28 | Maximum fragment payload bytes, `u32` |
| 28..30 | Maximum fragments, `u16` |
| 30..32 | Reserved, exactly zero |

The negotiated values may only reduce the client's offered limits. The protocol ceilings are a
64 KiB fragment payload, 64 fragments, and a 4 MiB logical payload. Runtime and server limits may
be lower.

### OBW2 frame

Each plaintext OBW2 fragment has a 30-byte header followed by exactly `fragmentLen` bytes. The
complete frame is independently AEAD-sealed before it is sent.

| Bytes | Field |
| --- | --- |
| 0..4 | Magic `OBW2` |
| 4 | Message kind, using the existing IDs 1 through 10 |
| 5 | Flags: `FIRST = 0x01`, `LAST = 0x02`; all other bits are zero |
| 6..14 | Request ID, `u64` |
| 14..18 | Fragment payload length, `u32` |
| 18..22 | Logical payload length, `u32` |
| 22..26 | Logical offset, `u32` |
| 26..28 | Fragment index, `u16` |
| 28..30 | Fragment count, `u16` |

Fragmentation is canonical. `count = ceil(logicalLen / negotiatedFragmentBytes)`, except that a
zero-length logical message has one zero-length fragment. Offsets are
`index * negotiatedFragmentBytes`; every non-final fragment is full-sized; and FIRST/LAST must
match the index exactly. A decoder rejects unknown flags, gaps, duplicates, restarts, inconsistent
metadata, trailing bytes, over-limit values, and non-canonical short fragments. Reassembly is
strictly ordered per request ID and completes only after the declared final fragment.

OBW1 remains unchanged: its 18-byte `OBW1` frame header and `OWH1`/`OWA1` handshake are encoded
and decoded by the existing path. An OBW1 peer is never sent an OBW2 frame.

### Scheduling and bounded ownership

Request IDs multiplex independent RPCs up to the lower of negotiated credits and the local
resource-governor limit. Logical payload size does not choose semantic priority: callers classify
control, interactive, and bulk work before entering the lane.

Transmit encryption and socket writes are serialized. Bulk logical messages are split into
canonical fragments and cooperatively yield between fragments. Before every next bulk fragment,
queued control frames are drained; in particular, CANCEL can overtake the remaining fragments of
its request. Already-started encryption is not preempted.

Receive decryption is serialized and each request has one strictly bounded reassembly workspace.
Before a request is sent, the lane reserves its admitted transmit buffers, the browser-created
incoming frame, decrypt output, and maximum bounded response/reassembly work. Receive byte and
frame counters are also capped against the active request set before decryption. Timeout,
cancellation, protocol failure, and socket close retain native-task ownership until encryption or
decryption finalizers release the buffers. Credit, send, and terminal ACK have separate deadlines.

Malformed frames, mismatched response kinds, responses without an owner, oversized cancelled
terminals, and invalid Error payloads close the lane. Cancellation leaves a bounded tombstone only
until a validated terminal response or its drain deadline.

### Downgrade, re-upgrade, and rollback

A failed OBW2 attempt has consumed its ticket and AEAD sequence space. The client closes that
attempt and may retry OBW1 only after minting a fresh ticket/session and creating a fresh socket.
It never replays a ticket, sequence, sealed frame, or partially reassembled message across wire
versions.

After downgrade, OBW1 remains stable for an exponential hold: 60 seconds initially, doubling to a
one-hour cap. Clock samples are monotonic-clamped so wall-clock rollback cannot extend the hold
indefinitely; a clock jump merely makes the next idle boundary eligible. Re-upgrade has no
background timer. The next eligible request retires OBW1 only when there are no pending requests,
cancel tombstones, queued controls, or held credits. Concurrent callers share one connection
attempt. A stop cancels an in-progress session/handshake, and a late session result or retired
socket callback cannot replace the current lane. Because the lane cannot forcibly settle an
arbitrary host `openSession` Promise, it retains at most one detached mint tombstone per wire
version and suppresses another mint of that version until the predecessor settles.

Disconnect discards partial reassembly. Callers retry idempotent content-addressed work through a
fresh lane or the existing HTTP fallback. No WebSocket framing or cooldown state is persisted, so
rollback is capability-driven: removing `ws-data-v2` makes new clients open OBW1, while old clients
continue to see only their unchanged OBW1 path.

## Non-goals

- Changing FastCDC boundaries, object hashes, pack encoding, tree semantics, or root commits.
- Providing CRDT or character-level collaborative editing.
- Resuming a partial logical message after disconnect or transferring AEAD state between sockets.
- Replacing the realtime notification socket, HTTP fallback, or transport-router health policy.
- Adding a durable WebSocket queue, unlimited reassembly spool, QUIC, or WebTransport.
- Preempting synchronous browser/WebCrypto work that has already started.
