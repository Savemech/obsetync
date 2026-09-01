export interface TreeNegotiation {
    currentVersion: 1 | 2;
    fleetReady: boolean;
    readyDevices: number;
    enrolledDevices: number;
    activation: "active" | "eligible" | "blocked";
}

/** Strictly decode the authenticated, personalized portion of a capability
 * bundle. Absence means an older server and therefore Tree v1; malformed or
 * internally contradictory data fails closed instead of selecting a format. */
export function decodeTreeNegotiation(bundle: any): TreeNegotiation {
    const tree = bundle?.tree;
    if (tree === undefined) {
        return {
            currentVersion: 1,
            fleetReady: false,
            readyDevices: 0,
            enrolledDevices: 0,
            activation: "blocked",
        };
    }
    const currentVersion = tree?.current_version;
    const readyDevices = tree?.ready_devices;
    const enrolledDevices = tree?.enrolled_devices;
    const activation = tree?.activation;
    if (
        (currentVersion !== 1 && currentVersion !== 2) ||
        typeof tree?.fleet_ready !== "boolean" ||
        !Number.isSafeInteger(readyDevices) || readyDevices < 0 ||
        !Number.isSafeInteger(enrolledDevices) || enrolledDevices < 0 ||
        readyDevices > enrolledDevices ||
        !["active", "eligible", "blocked"].includes(activation)
    ) {
        throw new Error("server returned an invalid Tree capability status");
    }
    if (
        currentVersion === 2 &&
        (!Array.isArray(bundle?.capabilities) || !bundle.capabilities.includes("tree-v2"))
    ) {
        throw new Error("server selected Tree v2 without advertising tree-v2 support");
    }
    return {
        currentVersion,
        fleetReady: tree.fleet_ready,
        readyDevices,
        enrolledDevices,
        activation,
    };
}
