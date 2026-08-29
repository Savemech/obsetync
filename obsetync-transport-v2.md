# Transport v2 implementation note

The proposal in this file has been implemented for 1.10.1. The normative,
byte-level specification now lives in [`docs/transport.md`](docs/transport.md).

The implementation deliberately changed four details from the early draft
after concurrency, crash-safety, and forward-secrecy review:

1. Requests are not serialized. The server uses a 1024-bit replay window, so
   the plugin can keep parallel content uploads without rejecting reordered
   responses.
2. Both sides reserve sequence durability in blocks of 4096. The client saves
   its high-water mark before using a block; the server fsyncs a block ceiling
   before accepting from it. On restart, unused reserved numbers become safe
   gaps. This avoids one fsync per uploaded chunk.
3. Rotating server private keys are memory-only. A server restart creates a
   fresh key and clients recover through bootstrap automatically; no claim is
   made that an SSD can securely overwrite historical key sectors.
4. Bootstrap carries an empty plaintext—never a bearer or sequence. It is a
   read-only, single-DH authenticated fetch of public key metadata. This keeps
   a future compromise of the long-term server key from exposing credentials
   in captured bootstrap traffic.

Wire v1 is not accepted by the v2 server. Existing devices must re-enroll when
upgrading the server and plugin together.
