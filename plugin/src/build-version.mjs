// Prerelease identifiers may have leading zeros only when they contain a non-digit.
const VERSION = /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

/** @param {unknown} value */
export function isBuildVersion(value) {
    if (typeof value !== "string") return false;
    const match = VERSION.exec(value);
    return match !== null && !/(?:^|\.)0\d+(?:\.|$)/.test(match[1] ?? "");
}
