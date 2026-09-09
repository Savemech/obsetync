import { yieldWork } from "./work-scheduler";

/** Lossless UTF-16 chunk encoding keeps source identity stable even for an
 * unpaired surrogate in a corrupt source string. Each crypto input is bounded;
 * this fingerprints already bounded adapter strings, not native streamed IO. */
export async function fingerprintLegacyCopies(copies: ReadonlyArray<{
    role: string; raw: string | null;
}>): Promise<string> {
    if (copies.length > 16) throw new Error("too many legacy source roles");
    const captured = copies.map(({ role, raw }) => ({ role, raw }));
    const encoder = new TextEncoder();
    const digest = async (bytes: Uint8Array): Promise<Uint8Array> =>
        new Uint8Array(await globalThis.crypto.subtle.digest("SHA-256", bytes as Uint8Array<ArrayBuffer>));
    const hex = (bytes: Uint8Array): string => [...bytes].map(byte => byte.toString(16).padStart(2, "0")).join("");
    const manifest: Array<{ role: string; units: number | null; sha256: string | null }> = [];
    const roles = new Set<string>();
    for (const { role, raw } of captured) {
        if (!/^[a-z0-9-]{1,64}$/.test(role) || roles.has(role)) throw new Error("invalid legacy source role");
        roles.add(role);
        if (raw === null) { manifest.push({ role, units: null, sha256: null }); continue; }
        let hash = await digest(encoder.encode(JSON.stringify(["obsetync-legacy-source-chunks-v1", role, raw.length])));
        for (let offset = 0; offset < raw.length; offset += 16 * 1024) {
            const bytes = encoder.encode(JSON.stringify(raw.slice(offset, offset + 16 * 1024)));
            const input = new Uint8Array(hash.byteLength + bytes.byteLength);
            input.set(hash); input.set(bytes, hash.byteLength);
            hash = await digest(input);
            await yieldWork();
        }
        manifest.push({ role, units: raw.length, sha256: hex(hash) });
    }
    return hex(await digest(encoder.encode(JSON.stringify({ domain: "obsetync-legacy-source-manifest-v1", copies: manifest }))));
}
