/** DataAdapter accepts ArrayBuffer, while Uint8Array may be a view into a
 *  larger allocation. Never write bytes outside the view. */
export function exactArrayBuffer(data: Uint8Array): ArrayBuffer {
    if (data.byteOffset === 0 && data.byteLength === data.buffer.byteLength) {
        return data.buffer as ArrayBuffer;
    }
    return data.buffer.slice(
        data.byteOffset,
        data.byteOffset + data.byteLength,
    ) as ArrayBuffer;
}
