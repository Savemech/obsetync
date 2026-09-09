import { fingerprintLegacyCopies } from "./legacy-source";
import type { PreparedTransferPlan } from "./transfer-plan";
import type { ObjectConfirmationStore } from "./object-confirmation-store";
import type { SyncMemoryArbiter } from "./sync-memory-arbiter";

export interface PreparedTransferRuntime {
    plan: PreparedTransferPlan;
    memoryArbiter: SyncMemoryArbiter;
    confirmations?: ObjectConfirmationStore;
    scopeForTree(version: number): Promise<string>;
}

export interface PreparedScopeIdentity {
    vaultId: string;
    serverUrl: string;
    serverBoxPub: string;
    deviceId: string;
    syncObsidianConfig: boolean;
    ignorePatterns: readonly string[];
}

/** Scope identity is not a server storage epoch or an object-ACK lease.
 * It isolates local preparation across enrollment/policy/format changes;
 * resumed sources and remote object presence must still be revalidated.
 * Capture before any await; store only the digest, never settings or tokens. */
export function preparedScopeForTree(identity: PreparedScopeIdentity): (version: number) => Promise<string> {
    const fields = [identity.vaultId, identity.serverUrl, identity.serverBoxPub, identity.deviceId];
    if (fields.some(value => typeof value !== "string" || value.length > 8192) ||
        typeof identity.syncObsidianConfig !== "boolean" || !Array.isArray(identity.ignorePatterns) ||
        identity.ignorePatterns.length > 4096 ||
        identity.ignorePatterns.some(pattern => typeof pattern !== "string" || pattern.length > 8192)) {
        throw new RangeError("invalid prepared transfer scope");
    }
    // Account bounded individual strings before creating the combined JSON.
    // Escaped controls can cost six code units per input character.
    let encodedBudget = 512;
    for (const value of [...fields, ...identity.ignorePatterns]) {
        encodedBudget += JSON.stringify(value).length + 1;
        if (encodedBudget > 64 * 1024) throw new RangeError("prepared transfer scope exceeds metadata limit");
    }
    const encoded = JSON.stringify({ schema: 1, vaultId: identity.vaultId,
        serverUrl: identity.serverUrl, serverBoxPub: identity.serverBoxPub, deviceId: identity.deviceId,
        syncObsidianConfig: identity.syncObsidianConfig, ignorePatterns: [...identity.ignorePatterns],
        contentFormat: "blake3-fastcdc-256k-1m-4m-manifest-v1" });
    if (encoded.length > 64 * 1024) throw new RangeError("prepared transfer scope exceeds metadata limit");
    const pending = new Map<number, Promise<string>>();
    return version => {
        if (version !== 1 && version !== 2) return Promise.reject(new RangeError("unsupported prepared tree scope"));
        let digest = pending.get(version);
        if (!digest) {
            digest = fingerprintLegacyCopies([{ role: "prepared-identity-v1", raw: encoded },
                { role: "tree-version", raw: String(version) }]);
            pending.set(version, digest);
        }
        return digest;
    };
}
