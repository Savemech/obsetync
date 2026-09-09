/** Benchmark-only observers. No native/WASM implementation is replaced.
 * Durations include each actual JS/native call and its binding/copy overhead;
 * async observations end only at actual returned-Promise settlement. Methods
 * retain no arguments, returned bytes, paths or per-call history. The caller
 * owns phases, sampling cadence, worker drains and benchmark isolation.
 */
export const NATIVE_ROOT_METRIC_LIMITS = Object.freeze({ methods: 64, sampleValues: 4096 });
const UPPER_MS = Object.freeze([0, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]);
const PROCESS_FIELDS = Object.freeze(["rss", "heapTotal", "heapUsed", "external", "arrayBuffers"]);

function sortedSamples(values) {
    if (!Array.isArray(values) || values.length > NATIVE_ROOT_METRIC_LIMITS.sampleValues) {
        throw new RangeError("metric sample collection exceeds the diagnostic limit");
    }
    const copy = new Array(values.length);
    for (let index = 0; index < values.length; index++) {
        const value = values[index];
        if (!Number.isFinite(value) || value < 0) throw new RangeError("metric samples must be finite and non-negative");
        copy[index] = value;
    }
    return copy.sort((left, right) => left - right);
}
export function median(values) {
    const sorted = sortedSamples(values), middle = Math.floor(sorted.length / 2);
    return sorted.length === 0 ? null : sorted.length % 2 ? sorted[middle]
        : sorted[middle - 1] + (sorted[middle] - sorted[middle - 1]) / 2;
}
/** Exact nearest-rank percentile of the caller's bounded sample list. This is
 * NOT the histogram upper bound returned for instrumented native calls. */
export function nearestRankPercentile(values, fraction) {
    if (!Number.isFinite(fraction) || fraction <= 0 || fraction > 1) throw new RangeError("metric percentile must be in (0, 1]");
    const sorted = sortedSamples(values);
    return sorted.length ? sorted[Math.ceil(sorted.length * fraction) - 1] : null;
}

function inheritedDescriptor(target, name) {
    for (let owner = target; owner !== null; owner = Object.getPrototypeOf(owner)) {
        const descriptor = Object.getOwnPropertyDescriptor(owner, name);
        if (descriptor) return descriptor;
    }
    return undefined;
}
function histogramP95(record) {
    if (!record.timedCompletions) return { p95UpperBoundMs: null, p95AboveHistogramRange: false };
    const rank = Math.ceil(record.timedCompletions * 0.95);
    let seen = 0;
    for (let index = 0; index < record.histogram.length; index++) {
        seen += record.histogram[index];
        if (seen >= rank) return { p95UpperBoundMs: UPPER_MS[index] ?? null,
            p95AboveHistogramRange: index === UPPER_MS.length };
    }
    throw new Error("metric histogram count differs");
}

export function createNativeRootMetrics({ now = () => performance.now(), memoryUsage = () => process.memoryUsage() } = {}) {
    if (typeof now !== "function" || typeof memoryUsage !== "function") throw new TypeError("metric observers must be functions");
    const records = new Map(), installed = new Set();
    let observerErrors = 0;
    const memory = { samples: 0, processSamples: 0, wasmSamples: 0, last: null, sampledMax: {} };
    const clock = () => {
        try { const value = now(); if (Number.isFinite(value)) return value; }
        catch { /* A broken observer must not replace a native result/error. */ }
        observerErrors++; return null;
    };
    function finish(record, started, failed) {
        record.active--; record.completed++; if (failed) record.errors++;
        const ended = clock();
        if (started === null || ended === null) return;
        const duration = ended - started;
        if (!Number.isFinite(duration) || duration < 0) { observerErrors++; return; }
        record.timedCompletions++; record.totalMs += duration;
        record.maxMs = record.maxMs === null ? duration : Math.max(record.maxMs, duration);
        let index = 0;
        while (index < UPPER_MS.length && duration > UPPER_MS[index]) index++;
        record.histogram[index]++;
    }
    function wrap(target, names, prefix, asynchronous) {
        if ((typeof target !== "object" && typeof target !== "function") || target === null ||
            !Array.isArray(names) || names.length > NATIVE_ROOT_METRIC_LIMITS.methods ||
            typeof prefix !== "string" || prefix.length > 64 || !/^[A-Za-z0-9_.:-]*$/.test(prefix)) {
            throw new TypeError("invalid bounded metric wrapper declaration");
        }
        const seen = new Set(), prepared = [];
        let additional = 0;
        for (let index = 0; index < names.length; index++) {
            const name = names[index];
            if (typeof name !== "string" || !/^[A-Za-z_][A-Za-z0-9_]{0,79}$/.test(name) || seen.has(name)) {
                throw new TypeError("metric methods must be explicit unique identifiers");
            }
            seen.add(name);
            const own = Object.getOwnPropertyDescriptor(target, name), descriptor = inheritedDescriptor(target, name);
            if (!descriptor || !("value" in descriptor) || typeof descriptor.value !== "function" ||
                (own && !own.configurable) || (!own && !Object.isExtensible(target))) {
                throw new TypeError("metric method cannot be wrapped; use a mutable export facade");
            }
            if ([...installed].some(entry => entry.target === target && entry.name === name)) {
                throw new TypeError("metric method is already wrapped");
            }
            const label = prefix ? `${prefix}.${name}` : name;
            if (!records.has(label)) additional++;
            prepared.push({ target, name, own, descriptor, label });
        }
        if (records.size + additional > NATIVE_ROOT_METRIC_LIMITS.methods ||
            installed.size + prepared.length > NATIVE_ROOT_METRIC_LIMITS.methods) {
            throw new RangeError("metric methods exceed the diagnostic limit");
        }
        const added = [], newLabels = [];
        const restore = () => {
            for (let index = added.length - 1; index >= 0; index--) {
                const entry = added[index];
                // Never overwrite a different wrapper installed by the caller.
                if (Object.getOwnPropertyDescriptor(entry.target, entry.name)?.value === entry.wrapper) {
                    if (entry.own) Object.defineProperty(entry.target, entry.name, entry.own);
                    else delete entry.target[entry.name];
                }
                installed.delete(entry);
            }
            added.length = 0;
        };
        try {
            for (const entry of prepared) {
                let record = records.get(entry.label);
                if (!record) {
                    record = { calls: 0, active: 0, completed: 0, errors: 0, timedCompletions: 0,
                        totalMs: 0, maxMs: null, histogram: new Array(UPPER_MS.length + 1).fill(0) };
                    records.set(entry.label, record); newLabels.push(entry.label);
                }
                const method = entry.descriptor.value;
                entry.wrapper = function (...args) {
                    record.calls++; record.active++;
                    const started = clock();
                    let value;
                    try { value = Reflect.apply(method, this, args); }
                    catch (error) { finish(record, started, true); throw error; }
                    if (asynchronous) {
                        // Preserve the exact returned promise/value. Observing
                        // settlement adds no queue, race, retry or virtual drain.
                        // As with any rejection observer, this marks a native
                        // rejection observed; it is not an unhandled-error gate.
                        Promise.resolve(value).then(() => finish(record, started, false), () => finish(record, started, true));
                    } else finish(record, started, false);
                    return value;
                };
                Object.defineProperty(target, entry.name, { value: entry.wrapper, writable: true,
                    enumerable: entry.descriptor.enumerable, configurable: true });
                installed.add(entry); added.push(entry);
            }
        } catch (error) {
            restore(); for (const label of newLabels) records.delete(label); throw error;
        }
        return restore;
    }
    function sampleMemory(wasmMemory) {
        const current = {};
        try {
            const usage = memoryUsage();
            for (const field of PROCESS_FIELDS) {
                const value = usage[field];
                if (!Number.isSafeInteger(value) || value < 0) throw new TypeError("invalid process memory observation");
                current[field] = value;
            }
            memory.processSamples++;
        } catch { for (const field of PROCESS_FIELDS) delete current[field]; observerErrors++; }
        if (wasmMemory !== undefined) {
            try {
                // Read the CURRENT buffer each time: memory.grow detaches the
                // previous view. Retain only its capacity, never the buffer.
                const capacity = wasmMemory.buffer.byteLength;
                if (!Number.isSafeInteger(capacity) || capacity < 0) throw new TypeError("invalid WASM capacity observation");
                current.wasmLinearCapacityBytes = capacity; memory.wasmSamples++;
            } catch { observerErrors++; }
        }
        memory.samples++;
        memory.last = current;
        for (const [field, value] of Object.entries(current)) memory.sampledMax[field] = Math.max(memory.sampledMax[field] ?? 0, value);
        return { ...current };
    }
    return {
        wrapSync: (target, names, prefix = "") => wrap(target, names, prefix, false),
        wrapAsync: (target, names, prefix = "") => wrap(target, names, prefix, true),
        sampleMemory,
        snapshot() {
            return { observerErrors, durationBucketUpperBoundsMs: [...UPPER_MS, null],
                methods: Object.fromEntries([...records].map(([label, record]) => [label, {
                    ...record, histogram: [...record.histogram], ...histogramP95(record),
                }])), memory: { ...memory, last: memory.last && { ...memory.last }, sampledMax: { ...memory.sampledMax } },
                limitations: ["call p95 is a fixed-histogram upper bound, not an exact percentile",
                    "process memory maxima are observed samples, not transient peaks or an allocation bound",
                    "WASM bytes are linear-memory capacity, not allocator live/retained bytes; grow need not shrink after free",
                    "RSS, external, arrayBuffers and WASM may overlap and must not be added",
                    "RSS includes this process's workers; JS heap/external counters are not a per-worker breakdown",
                    "the explicit WASM sample is only the supplied instance, not worker instances or a separate server/Obsidian host"] };
        },
        restore() {
            for (const entry of [...installed].reverse()) {
                if (Object.getOwnPropertyDescriptor(entry.target, entry.name)?.value === entry.wrapper) {
                    if (entry.own) Object.defineProperty(entry.target, entry.name, entry.own);
                    else delete entry.target[entry.name];
                }
                installed.delete(entry);
            }
        },
    };
}
