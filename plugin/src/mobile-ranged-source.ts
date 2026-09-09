import { reserveTransientWorkset, type TransientWorkScope } from "./transient-memory";

const RANGE_OVERHEAD_BYTES = 64 * 1024;
export const MOBILE_RESOURCE_RANGE_MAX_BYTES = 4 * 1024 * 1024;

export type MobileRangeFailureCode =
    | "UNAVAILABLE"
    | "PROTOCOL"
    | "SOURCE_CHANGED"
    | "CLOSED";

/** Fixed, path-free classification. Host/fetch error text is never retained. */
export class MobileRangeReadError extends Error {
    constructor(readonly code: MobileRangeFailureCode) {
        super(`mobile resource range ${code.toLowerCase().replace("_", " ")}`);
        this.name = "MobileRangeReadError";
    }
}

export interface MobileRangeStat {
    size: number;
    mtime: number;
}

export interface OwnedMobileRange {
    readonly bytes: Uint8Array;
    /** The caller must stop retaining bytes before releasing their admission. */
    release(): void;
}

export interface MobileRangeReader {
    readonly size: number;
    /** Path-free identity of the exact qualified host resource URL. */
    readonly resourceVersion?: string;
    read(offset: number, size: number, signal?: AbortSignal): Promise<OwnedMobileRange>;
    /** Use bytes already charged to a parent upload owner. The returned view
     * must not outlive that scope; owner.close() waits for a started read. */
    readBorrowed(offset: number, size: number, owner: TransientWorkScope,
        signal?: AbortSignal): Promise<Uint8Array>;
    verify(): Promise<void>;
    close(): void;
}

export interface MobileRangeFetchResponse {
    status: number;
    type: string;
    redirected: boolean;
    headers: { get(name: string): string | null };
    body: ReadableStream<Uint8Array> | null;
}

export interface MobileRangeHost {
    available(): boolean;
    fetch(url: string, init: RequestInit): Promise<MobileRangeFetchResponse>;
}

export const browserMobileRangeHost: MobileRangeHost = {
    available: () => typeof globalThis.fetch === "function" &&
        typeof globalThis.ReadableStream === "function",
    fetch: (url, init) => globalThis.fetch(url, init),
};

function validStat(stat: MobileRangeStat): boolean {
    return Number.isSafeInteger(stat.size) && stat.size >= 0 &&
        Number.isFinite(stat.mtime) && stat.mtime >= 0;
}

function sameStat(left: MobileRangeStat, right: MobileRangeStat | null): boolean {
    return right !== null && validStat(right) &&
        left.size === right.size && left.mtime === right.mtime;
}

function abortError(signal?: AbortSignal): Error {
    if (signal?.reason instanceof Error) return signal.reason;
    const error = new Error("mobile range read aborted");
    error.name = "AbortError";
    return error;
}

function throwIfAborted(signal?: AbortSignal): void {
    if (signal?.aborted) throw abortError(signal);
}

function rangeWorkset(size: number): number {
    // Output, one host-delivered chunk and one bridge/copy allowance. Native
    // host buffers remain unmeasured, so qualification is still a capability
    // fact rather than a claim about process RSS.
    return 3 * size + RANGE_OVERHEAD_BYTES;
}

function validateRange(total: number, offset: number, size: number): void {
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(size) || size <= 0 ||
        size > MOBILE_RESOURCE_RANGE_MAX_BYTES || offset > total - size) {
        throw new RangeError("invalid mobile resource range");
    }
}

async function cancelBody(response: MobileRangeFetchResponse | undefined): Promise<void> {
    try { await response?.body?.cancel(); } catch { /* original fixed classification wins */ }
}

function protocolResponse(response: MobileRangeFetchResponse, offset: number,
    size: number, total: number): boolean {
    const end = offset + size - 1;
    let contentRange: string | null;
    let contentLength: string | null;
    let contentEncoding: string | null;
    try {
        contentRange = response.headers.get("content-range");
        contentLength = response.headers.get("content-length");
        contentEncoding = response.headers.get("content-encoding");
    } catch {
        return false;
    }
    return response.status === 206 && response.redirected === false &&
        (response.type === "basic" || response.type === "cors" || response.type === "default") &&
        contentRange === `bytes ${offset}-${end}/${total}` &&
        contentLength === String(size) &&
        (contentEncoding === null || contentEncoding === "identity") &&
        response.body !== null;
}

async function readQualifiedRangeBytes(host: MobileRangeHost, resourceUrl: string,
    total: number, offset: number, size: number, signal?: AbortSignal): Promise<Uint8Array> {
    validateRange(total, offset, size);
    throwIfAborted(signal);
    let response: MobileRangeFetchResponse | undefined;
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
        throwIfAborted(signal);
        try {
            response = await host.fetch(resourceUrl, {
                method: "GET",
                headers: { Range: `bytes=${offset}-${offset + size - 1}`, "Accept-Encoding": "identity" },
                cache: "no-store",
                credentials: "same-origin",
                redirect: "error",
                signal,
            });
        } catch {
            throwIfAborted(signal);
            throw new MobileRangeReadError("UNAVAILABLE");
        }
        throwIfAborted(signal);
        if (!protocolResponse(response, offset, size, total)) {
            await cancelBody(response);
            throw new MobileRangeReadError("PROTOCOL");
        }
        try { reader = response.body!.getReader(); }
        catch {
            await cancelBody(response);
            throw new MobileRangeReadError("PROTOCOL");
        }
        const output = new Uint8Array(size);
        let written = 0;
        for (;;) {
            throwIfAborted(signal);
            let result: ReadableStreamReadResult<Uint8Array>;
            try { result = await reader.read(); }
            catch {
                throwIfAborted(signal);
                throw new MobileRangeReadError("UNAVAILABLE");
            }
            if (result.done) break;
            const chunk = result.value;
            const retained = chunk instanceof Uint8Array
                ? Math.max(chunk.byteLength, chunk.buffer.byteLength) : Number.POSITIVE_INFINITY;
            if (retained > size - written) throw new MobileRangeReadError("PROTOCOL");
            output.set(chunk, written);
            written += chunk.byteLength;
        }
        if (written !== size) throw new MobileRangeReadError("PROTOCOL");
        throwIfAborted(signal);
        return output;
    } finally {
        if (reader) {
            try { await reader.cancel(); } catch { /* fixed error wins */ }
            reader.releaseLock();
        }
    }
}

export async function openQualifiedMobileRangeReader(options: {
    resourceUrl: string;
    expected: MobileRangeStat;
    stat(): Promise<MobileRangeStat | null>;
    currentResourceUrl(): string;
    host?: MobileRangeHost;
    signal?: AbortSignal;
    /** Existing upload owner whose ranged bytes were admitted up front. */
    owner?: TransientWorkScope;
    /** Called when a formerly-valid host response stops satisfying range rules. */
    onCapabilityFailure?: (code: "UNAVAILABLE" | "PROTOCOL") => void;
}): Promise<MobileRangeReader> {
    const host = options.host ?? browserMobileRangeHost;
    if (!validStat(options.expected) || options.expected.size <= 0) {
        throw new RangeError("invalid mobile range source metadata");
    }
    if (typeof options.resourceUrl !== "string" || options.resourceUrl.length === 0 ||
        options.resourceUrl.length > 8192 || options.resourceUrl.includes("\0")) {
        throw new MobileRangeReadError("UNAVAILABLE");
    }
    let resourceVersion: string;
    try {
        // Deliberately digest the complete opaque URL. Hosts which rotate a
        // query/token across restart get a safe prepared-cache miss and fresh
        // FastCDC; normalizing unknown URL fields could falsely reuse bytes.
        const digest = new Uint8Array(await globalThis.crypto.subtle.digest(
            "SHA-256", new TextEncoder().encode(options.resourceUrl),
        ));
        resourceVersion = [...digest].map(byte => byte.toString(16).padStart(2, "0")).join("");
    } catch {
        throw new MobileRangeReadError("UNAVAILABLE");
    }
    throwIfAborted(options.signal);
    let available = false;
    try { available = host.available(); } catch { /* fixed unavailable result */ }
    if (!available) throw new MobileRangeReadError("UNAVAILABLE");
    let closed = false;
    let capabilityCurrent = true;

    const verify = async (): Promise<void> => {
        if (closed) throw new MobileRangeReadError("CLOSED");
        let currentUrl: string;
        try { currentUrl = options.currentResourceUrl(); }
        catch { throw new MobileRangeReadError("SOURCE_CHANGED"); }
        const current = await options.stat();
        if (currentUrl !== options.resourceUrl || !sameStat(options.expected, current)) {
            throw new MobileRangeReadError("SOURCE_CHANGED");
        }
    };
    const readBytes = async (offset: number, size: number, signal?: AbortSignal): Promise<Uint8Array> => {
        if (closed) throw new MobileRangeReadError("CLOSED");
        if (!capabilityCurrent) throw new MobileRangeReadError("UNAVAILABLE");
        throwIfAborted(signal);
        await verify();
        let bytes: Uint8Array;
        try {
            bytes = await readQualifiedRangeBytes(host, options.resourceUrl,
                options.expected.size, offset, size, signal);
        } catch (error) {
            if (error instanceof MobileRangeReadError &&
                (error.code === "UNAVAILABLE" || error.code === "PROTOCOL")) {
                capabilityCurrent = false;
                options.onCapabilityFailure?.(error.code);
            }
            throw error;
        }
        await verify();
        return bytes;
    };
    const reader: MobileRangeReader = {
        size: options.expected.size,
        resourceVersion,
        async read(offset, size, signal): Promise<OwnedMobileRange> {
            throwIfAborted(signal);
            validateRange(options.expected.size, offset, size);
            const lease = await reserveTransientWorkset(rangeWorkset(size), { signal });
            try {
                const bytes = await readBytes(offset, size, signal);
                let released = false;
                return { bytes, release() {
                    if (released) return;
                    released = true;
                    lease.release();
                } };
            } catch (error) { lease.release(); throw error; }
        },
        readBorrowed(offset, size, owner, signal): Promise<Uint8Array> {
            // planPushMemory's ranged ownerBytes includes the retained range
            // queue. track() ties a started fetch/body lifetime to that owner
            // without recursively reserving the same global pool.
            return owner.track(() => readBytes(offset, size, signal));
        },
        verify,
        close() { closed = true; },
    };

    // Qualification is an actual resource request, not inference from fetch
    // existence. Discard the byte while keeping the proved reader open.
    if (options.owner) {
        await reader.readBorrowed(0, 1, options.owner, options.signal);
    } else {
        const probe = await reader.read(0, 1, options.signal);
        probe.release();
    }
    return reader;
}
