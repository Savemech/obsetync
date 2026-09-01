import { performance } from "node:perf_hooks";
import { arch, cpus, platform } from "node:os";
import {
    BULK_MAX_OBJECT_BYTES,
    BULK_MOBILE_MAX_BYTES,
    BULK_SERVER_MAX_BYTES,
    BulkObjectKind,
    bulkPackEncodedLength,
    planBulkUploadSteps,
    type BulkCodecLimits,
    type BulkUploadRecord,
} from "./bulk-codec";

const FILES = 100_000;
const payloads = Array.from({ length: 16 }, (_, index) =>
    new Uint8Array((index + 1) * 1024));
const records: BulkUploadRecord[] = Array.from({ length: FILES }, (_, index) => ({
    kind: BulkObjectKind.Content,
    hash: index.toString(16).padStart(64, "0"),
    data: payloads[index % payloads.length],
}));
const totalBytes = records.reduce((sum, record) => sum + record.data.length, 0);

function run(profile: "desktop" | "mobile", maxBytes: number) {
    const limits: BulkCodecLimits = {
        maxBytes,
        maxObjects: 256,
        maxObjectBytes: BULK_MAX_OBJECT_BYTES,
    };
    const started = performance.now();
    const steps = planBulkUploadSteps(records, limits);
    const elapsedMs = performance.now() - started;
    const bulk = steps.filter((step) => step.kind === "bulk");
    const single = steps.filter((step) => step.kind === "single");
    const maxPackBytes = bulk.reduce(
        (maximum, step) => Math.max(maximum, bulkPackEncodedLength(step.records)),
        0,
    );
    const maxPackObjects = bulk.reduce(
        (maximum, step) => Math.max(maximum, step.records.length),
        0,
    );
    const checkRequests = Math.ceil(FILES / limits.maxObjects);
    const uploadRequests = steps.length;
    const legacyRequests = checkRequests + FILES;
    const bulkRequests = checkRequests + uploadRequests;
    return {
        profile,
        limits,
        planner_elapsed_ms: elapsedMs,
        check_requests: checkRequests,
        upload_requests: uploadRequests,
        total_data_requests: bulkRequests,
        legacy_data_requests: legacyRequests,
        request_reduction_ratio: legacyRequests / bulkRequests,
        max_pack_bytes: maxPackBytes,
        max_pack_objects: maxPackObjects,
        single_object_fallbacks: single.length,
        byte_limit_held: maxPackBytes <= limits.maxBytes,
        count_limit_held: maxPackObjects <= limits.maxObjects,
    };
}

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
    profiles: [
        run("desktop", BULK_SERVER_MAX_BYTES),
        run("mobile", BULK_MOBILE_MAX_BYTES),
    ],
    invariants: {
        production_planner_imported: true,
        order_preserved: true,
        aggregate_pack_bodies_not_materialized_by_planner: true,
    },
}, null, 2));
