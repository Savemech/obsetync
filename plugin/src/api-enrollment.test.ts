import { strict as assert } from "node:assert";
import { ObsetyncApi } from "./api";

const pin = btoa(String.fromCharCode(...new Uint8Array(32).fill(1)));
const bundle = () => ({ device_id: "a".repeat(32), device_name: "fixture",
    bearer_token: "b".repeat(64), server_box_pub: pin, wire_version: "0x02",
    eph_endpoint: "/api/v1/eph", Es_pub_initial: pin, Es_pub_valid_until: 2_000_000_000 });
const routes: Map<string, unknown> = (globalThis as any).__obsetyncTestRequestUrlRoutes ??= new Map();

async function explicitEndpoints(): Promise<void> {
    for (const [server, admin, expected] of [
        ["http://enrollment.invalid:27182/", "", "http://enrollment.invalid:27183/admin/enrollment/ABCD-1234"],
        ["https://enrollment.invalid/proxy/", "", "https://enrollment.invalid/proxy/admin/enrollment/ABCD-1234"],
        ["https://enrollment.invalid:8443/proxy/", "", "https://enrollment.invalid:8443/proxy/admin/enrollment/ABCD-1234"],
        ["http://enrollment.invalid:8080/proxy/", "", "http://enrollment.invalid:8080/proxy/admin/enrollment/ABCD-1234"],
        ["http://enrollment.invalid:28000", "http://enrollment.invalid:28001", "http://enrollment.invalid:28001/admin/enrollment/ABCD-1234"],
        ["https://enrollment.invalid/sync", "https://admin-enrollment.invalid/prefix/", "https://admin-enrollment.invalid/prefix/admin/enrollment/ABCD-1234"],
        ["http://[::1]:27182", "", "http://[::1]:27183/admin/enrollment/ABCD-1234"],
    ]) {
        const api = new ObsetyncApi(server, "", ""), calls: string[] = [];
        const origin = new URL(expected).origin;
        routes.set(origin, async (params: any) => { calls.push(params.url); return { status: 200, json: bundle() }; });
        try {
            assert.equal(api.baseUrl, server.replace(/\/$/, ""), "constructor changed the selected protocol");
            assert.deepEqual(await api.claimEnrollment("ABCD-1234", admin), bundle());
            assert.deepEqual(calls, [expected]);
        } finally { routes.delete(origin); }
    }
}

async function httpsHealth(): Promise<void> {
    const origin = "https://health-enrollment.invalid", calls: string[] = [];
    routes.set(origin, async (params: any) => { calls.push(params.url); return { status: 200 }; });
    try {
        const api = new ObsetyncApi(origin + "/prefix/", "", "");
        assert.deepEqual(await api.ping(), { serverUrl: origin + "/prefix", ok: true, transport: "https + AEAD envelope" });
        assert.deepEqual(calls, [origin + "/prefix/health"]);
    } finally { routes.delete(origin); }
}

async function malformedBundlesDoNotBecomeCredentials(): Promise<void> {
    const origin = "https://validation-enrollment.invalid";
    const api = new ObsetyncApi(origin, "", "");
    let response: unknown;
    routes.set(origin, async () => ({ status: 200, json: response }));
    try {
        for (response of [null, [], {}, ...[
            ["wire_version", "0x01"], ["device_id", "../outside"], ["device_id", ""],
            ["bearer_token", "x".repeat(64)], ["bearer_token", "b".repeat(63)],
            ["server_box_pub", "A".repeat(42) + "B="], ["server_box_pub", "bad"],
            ["Es_pub_initial", "bad"], ["Es_pub_valid_until", -1],
            ["Es_pub_valid_until", Number.MAX_SAFE_INTEGER + 1], ["Es_pub_valid_until", "tomorrow"],
        ].map(([key, value]) => ({ ...bundle(), [key as string]: value }))]) {
            await assert.rejects(api.claimEnrollment("ABCD-1234"), /enrollment/i);
        }
        response = bundle();
        assert.deepEqual(await api.claimEnrollment("ABCD-1234"), bundle());
    } finally { routes.delete(origin); }
}

async function invalidInputDoesNotIssueRequests(): Promise<void> {
    const origin = "http://input-enrollment.invalid";
    let calls = 0;
    routes.set(origin, async () => { calls++; return { status: 200, json: bundle() }; });
    try {
        const api = new ObsetyncApi(origin, "", "");
        for (const code of ["", "../devices", "ABCD-1234?secret", "ABCD-12345"]) {
            await assert.rejects(api.claimEnrollment(code), /enrollment/i);
        }
        for (const admin of [origin + "?secret", origin + "#secret", "ftp://input-enrollment.invalid",
            "http://secret@input-enrollment.invalid", "http:input-enrollment.invalid", origin + "\\other", origin + "/\n"]) {
            await assert.rejects(api.claimEnrollment("ABCD-1234", admin), /enrollment/i);
        }
        assert.equal(calls, 0);
    } finally { routes.delete(origin); }
}

async function run(): Promise<void> {
    await explicitEndpoints();
    await httpsHealth();
    await malformedBundlesDoNotBecomeCredentials();
    await invalidInputDoesNotIssueRequests();
    console.log("api-enrollment.test: endpoint, HTTPS, bundle and input validation suites passed");
}
void run().catch(error => { setTimeout(() => { throw error; }, 0); });
