/**
 * W1 transport-envelope benchmark for Slice 8.
 *
 * This imports the production bulk planner/codecs and both production crypto
 * paths. It deliberately excludes network and server storage: the measured
 * delta is the per-pack client cost removed by keeping one ticket-derived
 * AES-GCM session instead of deriving an HTTP message key for every pack.
 */
import { arch, cpus, platform } from "node:os";
import { performance } from "node:perf_hooks";
import { x25519 } from "@noble/curves/ed25519";
import {
    BulkObjectKind,
    BULK_MAX_OBJECT_BYTES,
    BULK_MOBILE_MAX_BYTES,
    BULK_SERVER_MAX_BYTES,
    encodeBulkCheckRequest,
    encodeBulkUploadPack,
    planBulkUploadSteps,
    type BulkCodecLimits,
    type BulkUploadRecord,
} from "./bulk-codec";
import { ObsetyncSecureChannel, ObsetyncWsSession } from "./secure";
import { WsDataFrameType, encodeWsDataFrame } from "./ws-data-codec";

const FILES = 100_000;
const RUNS = 3;
const payloads = Array.from({ length: 16 }, (_, index) =>
    new Uint8Array((index + 1) * 1024));
const records: BulkUploadRecord[] = Array.from({ length: FILES }, (_, index) => ({
    kind: BulkObjectKind.Content,
    hash: index.toString(16).padStart(64, "0"),
    data: payloads[index % payloads.length],
}));
const totalBytes = records.reduce((sum, record) => sum + record.data.byteLength, 0);

function b64(bytes: Uint8Array): string {
    return Buffer.from(bytes).toString("base64");
}

async function transports() {
    const staticPrivate = crypto.getRandomValues(new Uint8Array(32));
    const rotatingPrivate = crypto.getRandomValues(new Uint8Array(32));
    const http = await ObsetyncSecureChannel.create(
        b64(x25519.getPublicKey(staticPrivate)),
        "ab".repeat(32),
        b64(x25519.getPublicKey(rotatingPrivate)),
        Math.floor(Date.now() / 1000) + 7200,
    );
    staticPrivate.fill(0);
    rotatingPrivate.fill(0);

    const clientPrivate = crypto.getRandomValues(new Uint8Array(32));
    const serverPrivate = crypto.getRandomValues(new Uint8Array(32));
    const ws = await ObsetyncWsSession.create(
        clientPrivate,
        b64(x25519.getPublicKey(serverPrivate)),
        "cd".repeat(32),
        "data-v1",
    );
    serverPrivate.fill(0);
    return { http, ws };
}

async function runProfileSample(
    profile: "desktop" | "mobile",
    maxBytes: number,
    wsFrameBytes: number,
) {
    const limits: BulkCodecLimits = {
        maxBytes,
        maxObjects: 256,
        maxObjectBytes: BULK_MAX_OBJECT_BYTES,
    };
    const steps = planBulkUploadSteps(records, limits);
    const { http, ws } = await transports();
    let requestId = 1;
    let httpMs = 0;
    let wsMs = 0;
    let plaintextBytes = 0;
    let httpWireBytes = 0;
    let wsWireBytes = 0;
    let maxBodyBytes = 0;
    let checksum = 0;

    const sealBoth = async (
        body: Uint8Array,
        path: string,
        type: WsDataFrameType.CheckObjects | WsDataFrameType.PutPack,
    ): Promise<void> => {
        const id = requestId++;
        const runHttp = async () => {
            const started = performance.now();
            const wire = await http.encryptRequest("POST", path, body, id);
            httpMs += performance.now() - started;
            httpWireBytes += wire.byteLength;
            checksum ^= wire[wire.byteLength - 1] ?? 0;
        };
        const runWs = async () => {
            const started = performance.now();
            const inner = encodeWsDataFrame(type, id, body, wsFrameBytes);
            const wire = await ws.sealBytes(inner);
            wsMs += performance.now() - started;
            wsWireBytes += wire.byteLength;
            checksum ^= wire[wire.byteLength - 1] ?? 0;
        };
        // Alternate order to reduce a systematic warm-cache/thermal bias.
        if ((id & 1) === 0) {
            await runHttp();
            await runWs();
        } else {
            await runWs();
            await runHttp();
        }
        plaintextBytes += body.byteLength;
        maxBodyBytes = Math.max(maxBodyBytes, body.byteLength);
    };

    for (let offset = 0; offset < records.length; offset += limits.maxObjects) {
        const batch = records.slice(offset, offset + limits.maxObjects);
        await sealBoth(
            encodeBulkCheckRequest(
                BulkObjectKind.Content,
                batch.map((record) => record.hash),
                limits.maxObjects,
            ),
            "/api/v1/bulk/check",
            WsDataFrameType.CheckObjects,
        );
    }
    for (const step of steps) {
        if (step.kind !== "bulk") {
            throw new Error("W1 unexpectedly selected the single-object fallback");
        }
        await sealBoth(
            encodeBulkUploadPack(step.records, limits),
            "/api/v1/bulk/put",
            WsDataFrameType.PutPack,
        );
    }

    return {
        profile,
        limits: {
            bulk_bytes: limits.maxBytes,
            ws_payload_bytes: wsFrameBytes,
            objects: limits.maxObjects,
        },
        data_rpcs: requestId - 1,
        plaintext_bytes: plaintextBytes,
        http_envelope_ms: httpMs,
        ws_data_envelope_ms: wsMs,
        envelope_speedup: httpMs / wsMs,
        http_wire_bytes: httpWireBytes,
        ws_wire_bytes: wsWireBytes,
        metadata_bytes_saved: httpWireBytes - wsWireBytes,
        max_rpc_payload_bytes: maxBodyBytes,
        checksum,
        bounds_held: maxBodyBytes <= wsFrameBytes,
    };
}

function median(values: number[]): number {
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.floor(sorted.length / 2)];
}

async function runProfile(
    profile: "desktop" | "mobile",
    maxBytes: number,
    wsFrameBytes: number,
) {
    const samples = [];
    for (let iteration = 0; iteration < RUNS; iteration++) {
        samples.push(await runProfileSample(profile, maxBytes, wsFrameBytes));
    }
    const first = samples[0];
    if (!samples.every((sample) =>
        sample.data_rpcs === first.data_rpcs &&
        sample.plaintext_bytes === first.plaintext_bytes &&
        sample.http_wire_bytes === first.http_wire_bytes &&
        sample.ws_wire_bytes === first.ws_wire_bytes &&
        sample.max_rpc_payload_bytes === first.max_rpc_payload_bytes &&
        sample.bounds_held)) {
        throw new Error("transport benchmark invariants changed between runs");
    }
    const httpMedian = median(samples.map((sample) => sample.http_envelope_ms));
    const wsMedian = median(samples.map((sample) => sample.ws_data_envelope_ms));
    return {
        profile,
        limits: first.limits,
        data_rpcs: first.data_rpcs,
        plaintext_bytes: first.plaintext_bytes,
        median_http_envelope_ms: httpMedian,
        median_ws_data_envelope_ms: wsMedian,
        median_envelope_speedup: httpMedian / wsMedian,
        http_wire_bytes: first.http_wire_bytes,
        ws_wire_bytes: first.ws_wire_bytes,
        metadata_bytes_saved: first.metadata_bytes_saved,
        max_rpc_payload_bytes: first.max_rpc_payload_bytes,
        bounds_held: true,
        samples: samples.map((sample, iteration) => ({
            iteration: iteration + 1,
            http_envelope_ms: sample.http_envelope_ms,
            ws_data_envelope_ms: sample.ws_data_envelope_ms,
            envelope_speedup: sample.envelope_speedup,
            checksum: sample.checksum,
        })),
    };
}

void (async () => {
    const profiles = [];
    profiles.push(await runProfile("desktop", BULK_SERVER_MAX_BYTES, 4 * 1024 * 1024));
    profiles.push(await runProfile("mobile", BULK_MOBILE_MAX_BYTES, 2 * 1024 * 1024));
    console.log(JSON.stringify({
        schema_version: 1,
        runtime: {
            platform: platform(),
            architecture: arch(),
            node: process.version,
            cpu: cpus()[0]?.model ?? "unknown",
        },
        workload: {
            id: "W1",
            files: FILES,
            bytes: totalBytes,
            size_distribution: "deterministic round-robin 1..16 KiB",
        },
        runs_per_profile: RUNS,
        scope: "production client codecs and AEAD only; excludes socket/HTTP I/O and server work",
        profiles,
        invariants: {
            same_plaintext_pack_per_comparison: true,
            production_http_envelope: true,
            production_ws_data_envelope: true,
            ticket_key_derivation_excluded_from_timed_region: true,
            enrollment_identity_unchanged: true,
        },
    }, null, 2));
})().catch((error) => {
    setTimeout(() => { throw error; }, 0);
});
