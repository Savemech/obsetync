import type { RootCandidatePublication } from "../../src/root-sync-runtime";

type MeasuredRootPublication = Pick<RootCandidatePublication, "entries" | "rootExport" | "journalCuts">;

/** Keep the JavaScript native-root fixture bound to the production publication ABI. */
export function sampleNativeRootPublication(candidate: MeasuredRootPublication): {
    entries: number;
    rootBytes: number;
    cuts: number;
} {
    return {
        entries: candidate.entries.length,
        rootBytes: candidate.rootExport.bytes.byteLength,
        cuts: candidate.journalCuts.length,
    };
}
