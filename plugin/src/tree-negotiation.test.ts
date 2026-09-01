import { decodeTreeNegotiation } from "./tree-negotiation";

let assertions = 0;
function ok(condition: unknown, message: string): void {
    assertions++;
    if (!condition) throw new Error(message);
}
function rejects(bundle: unknown, message: string): void {
    let rejected = false;
    try {
        decodeTreeNegotiation(bundle);
    } catch {
        rejected = true;
    }
    ok(rejected, message);
}

const legacy = decodeTreeNegotiation({ capabilities: ["bulk-http-v1"] });
ok(legacy.currentVersion === 1, "old server did not fall back to Tree v1");
ok(legacy.activation === "blocked", "old server invented activation readiness");

const active = decodeTreeNegotiation({
    capabilities: ["tree-v2"],
    tree: {
        current_version: 2,
        fleet_ready: true,
        ready_devices: 3,
        enrolled_devices: 3,
        activation: "active",
    },
});
ok(active.currentVersion === 2, "authenticated active Tree v2 was lost");
ok(active.fleetReady && active.readyDevices === 3, "fleet readiness was decoded incorrectly");

rejects({
    capabilities: [],
    tree: {
        current_version: 2,
        fleet_ready: true,
        ready_devices: 1,
        enrolled_devices: 1,
        activation: "active",
    },
}, "Tree v2 without the server capability was accepted");
rejects({
    capabilities: ["tree-v2"],
    tree: {
        current_version: 2,
        fleet_ready: true,
        ready_devices: 2,
        enrolled_devices: 1,
        activation: "active",
    },
}, "ready device count above fleet size was accepted");
rejects({ tree: { current_version: 3 } }, "unknown Tree version was accepted");
rejects({
    capabilities: ["tree-v2"],
    tree: {
        current_version: 1,
        fleet_ready: false,
        ready_devices: 0,
        enrolled_devices: Number.MAX_SAFE_INTEGER + 1,
        activation: "blocked",
    },
}, "unsafe fleet count was accepted");

console.log(`tree-negotiation.test: ${assertions} assertions passed`);
