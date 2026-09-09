export const BUILD_IDENTITY_SCHEMA = "obsetync-build-identity-v1";
export const BUILD_PROTOCOL_SCHEMA = "obsetync-sync-protocol-set-v1";
export const BUILD_PROTOCOL_CAPABILITIES = Object.freeze([
    "bulk-http-v1",
    "paged-diff-v1",
    "root-cancel-v1",
    "root-outcome-v1",
    "tree-v2",
    "ws-data-v1",
    "ws-data-v2",
] as const);

export type BuildSourceState = "clean" | "dirty" | "local-unknown";
export type BuildMode = "production" | "development";

export interface BuildIdentity {
    readonly schema: typeof BUILD_IDENTITY_SCHEMA;
    readonly semver: string;
    readonly gitCommit: string;
    readonly sourceState: BuildSourceState;
    readonly buildMode: BuildMode;
    readonly protocol: {
        readonly schema: typeof BUILD_PROTOCOL_SCHEMA;
        readonly httpWire: "0x02";
        readonly capabilities: typeof BUILD_PROTOCOL_CAPABILITIES;
    };
}

const FULL_GIT_COMMIT = /^[0-9a-f]{40}$/;
const SEMVER = /^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;

export function createBuildIdentity(input: {
    semver: string;
    gitCommit: string;
    sourceState: BuildSourceState;
    buildMode: BuildMode;
}): BuildIdentity {
    if (!SEMVER.test(input.semver)) throw new Error("invalid embedded build semantic version");
    if (input.sourceState !== "clean" && input.sourceState !== "dirty" &&
        input.sourceState !== "local-unknown") throw new Error("invalid embedded build source state");
    if (input.buildMode !== "production" && input.buildMode !== "development") {
        throw new Error("invalid embedded build mode");
    }
    if (input.sourceState === "local-unknown") {
        if (input.gitCommit !== "unknown") throw new Error("local-unknown build has a Git commit");
    } else if (!FULL_GIT_COMMIT.test(input.gitCommit)) {
        throw new Error("embedded build identity requires a full Git commit");
    }
    return Object.freeze({
        schema: BUILD_IDENTITY_SCHEMA,
        semver: input.semver,
        gitCommit: input.gitCommit,
        sourceState: input.sourceState,
        buildMode: input.buildMode,
        protocol: Object.freeze({
            schema: BUILD_PROTOCOL_SCHEMA,
            httpWire: "0x02" as const,
            capabilities: BUILD_PROTOCOL_CAPABILITIES,
        }),
    });
}

export interface BuildIdentityComparison {
    readonly status: "match" | "dirty" | "local-unknown" | "version-mismatch";
    readonly compatible: boolean;
    readonly diagnostic: string;
}

export function compareBuildIdentity(manifestVersion: string,
    identity: BuildIdentity): BuildIdentityComparison {
    if (manifestVersion !== identity.semver) {
        return {
            status: "version-mismatch",
            compatible: false,
            diagnostic: `MISMATCH manifest ${manifestVersion} != bundle ${identity.semver}`,
        };
    }
    if (identity.sourceState === "dirty") {
        return { status: "dirty", compatible: true,
            diagnostic: "DIRTY LOCAL BUILD (commit is exact; source has tracked modifications)" };
    }
    if (identity.sourceState === "local-unknown") {
        return { status: "local-unknown", compatible: true,
            diagnostic: "LOCAL BUILD (Git commit unavailable)" };
    }
    return { status: "match", compatible: true, diagnostic: "MATCH" };
}
