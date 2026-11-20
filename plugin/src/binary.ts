/** DataAdapter accepts ArrayBuffer, while Uint8Array may be a view into a
 *  larger allocation. Never write bytes outside the view. */
export function exactArrayBuffer(data: Uint8Array): ArrayBuffer {
    if (data.buffer instanceof ArrayBuffer && data.byteOffset === 0 && data.byteLength === data.buffer.byteLength) {
        return data.buffer;
    }
    return new Uint8Array(data).buffer;
}
