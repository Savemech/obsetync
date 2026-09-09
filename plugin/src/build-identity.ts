import { createBuildIdentity } from "./build-identity-contract";

declare const __OBSETYNC_BUILD_SEMVER__: string;
declare const __OBSETYNC_BUILD_GIT_COMMIT__: string;
declare const __OBSETYNC_BUILD_SOURCE_STATE__: "clean" | "dirty" | "local-unknown";
declare const __OBSETYNC_BUILD_MODE__: "production" | "development";

// The explicit fallbacks keep source-only test harnesses executable. Any such
// bundle is visibly local/unverified and its version intentionally mismatches a
// real release manifest; the production esbuild entry always injects all four.
const semver = typeof __OBSETYNC_BUILD_SEMVER__ === "string"
    ? __OBSETYNC_BUILD_SEMVER__ : "0.0.0-local";
const gitCommit = typeof __OBSETYNC_BUILD_GIT_COMMIT__ === "string"
    ? __OBSETYNC_BUILD_GIT_COMMIT__ : "unknown";
const sourceState = typeof __OBSETYNC_BUILD_SOURCE_STATE__ === "string"
    ? __OBSETYNC_BUILD_SOURCE_STATE__ : "local-unknown";
const buildMode = typeof __OBSETYNC_BUILD_MODE__ === "string"
    ? __OBSETYNC_BUILD_MODE__ : "development";

export const BUNDLE_BUILD_IDENTITY = createBuildIdentity({
    semver,
    gitCommit,
    sourceState,
    buildMode,
});
