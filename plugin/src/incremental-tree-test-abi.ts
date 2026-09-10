export function installIncrementalTreeTestAbi(
    tree: any,
    hashRootBytes: (bytes: Uint8Array) => string,
): void {
    type JobKind = "begin" | "chunks" | "update" | "delete" | "root-export";
    type Job = { token: number; kind: JobKind; payload?: string; retiring?: boolean;
        bytes?: Uint8Array; exportStage?: "plan" | "planned" | "build" | "built" };
    let nextToken = 1;
    let active: Job | null = null;
    let candidateRevision = 0;
    const beginCandidate = tree.begin_candidate.bind(tree);
    const updateCandidate = tree.candidate_update_batch.bind(tree);
    const deleteCandidate = tree.candidate_delete_batch.bind(tree);
    const commitCandidate = tree.commit_candidate.bind(tree);
    const abortCandidate = tree.abort_candidate.bind(tree);
    const requireFixture = (condition: unknown, message: string): void => {
        if (!condition) throw new Error(message);
    };
    const begin = (kind: JobKind, payload?: string): number => {
        requireFixture(active === null, "incremental fixture overlapped tree jobs");
        const token = nextToken++;
        active = { token, kind, payload };
        return token;
    };
    const owned = (token: number): Job => {
        requireFixture(active?.token === token, "incremental fixture lost its tree job token");
        return active!;
    };

    tree.candidate_revision = () => candidateRevision;
    tree.begin_candidate_job = () => begin("begin");
    tree.begin_candidate_chunks_job = () => begin("chunks");
    tree.begin_candidate_update_job = (payload: string) => begin("update", payload);
    tree.begin_candidate_delete_job = (payload: string) => begin("delete", payload);
    tree.step_tree_job = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "update" || job.kind === "delete", "unexpected legacy tree-job step");
        const reachable = Math.min(1, tree.candidate_total_files());
        return { done: true, units: reachable, completed: reachable, remaining: 0, reachable };
    };
    tree.finish_candidate_job = () => { throw new Error("deferred fixture used consuming candidate finish"); };
    tree.finish_candidate_chunks_job = () => { throw new Error("deferred fixture used consuming chunk finish"); };
    tree.finish_candidate_mutation_job = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "update" || job.kind === "delete",
            "incremental fixture finished another job kind");
        (job.kind === "update" ? updateCandidate : deleteCandidate)(job.payload!);
        candidateRevision++;
        active = null;
    };
    tree.cancel_tree_job = (token: number) => { owned(token); active = null; };

    tree.step_reachability_job_deferred = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "begin" || job.kind === "chunks",
            "incremental fixture traversed another job kind");
        const reachable = job.kind === "begin" ? Math.min(1, tree.total_files()) : 0;
        return { done: true, units: reachable, completed: reachable, remaining: 0, reachable };
    };
    tree.finish_candidate_job_deferred = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "begin", "incremental fixture finished another reachability job");
        beginCandidate();
        candidateRevision++;
        job.retiring = true;
        return Math.min(1, tree.candidate_total_files());
    };
    tree.finish_candidate_chunks_job_deferred = () => {
        throw new Error("paged fixture used detached chunk arrays");
    };
    tree.cancel_reachability_job_deferred = (token: number) => { owned(token).retiring = true; };
    tree.step_reachability_retirement = (token: number, completed: number) => {
        const job = owned(token);
        requireFixture(job.retiring === true && completed === 0,
            "incremental fixture retired an invalid cursor");
        return { done: true, units: 0, completed: 0 };
    };
    tree.finish_reachability_retirement = (token: number, completed: number) => {
        const job = owned(token);
        requireFixture(job.retiring === true && completed === 0,
            "incremental fixture finalized an invalid cursor");
        active = null;
    };

    tree.candidate_chunks_sort_memory_plan_v1_job = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "chunks" && !job.retiring,
            "incremental fixture planned another cursor");
        return { schema: 1, scope: "candidate-chunk-plan-sort-workspace", hashCount: 0,
            hashSizeBytes: 32, sourceHashesRequestedBytes: 0, scratchHashesRequestedBytes: 0,
            peakAdmissionBytes: 0, reachableSetUnmeasured: true, pageOutputUnmeasured: true,
            sortStrategy: "stable-lsd-radix-v1", pageMaxHashes: 256 };
    };
    tree.resume_candidate_chunks_sort_memory_v1_job = (token: number, source: number, scratch: number) => {
        const job = owned(token);
        requireFixture(job.kind === "chunks" && source === 0 && scratch === 0,
            "incremental fixture resumed an invalid chunk sort");
    };
    tree.step_candidate_chunks_sort_v1_job = () => {
        throw new Error("empty incremental fixture unexpectedly sorted chunk hashes");
    };
    tree.candidate_chunks_plan_info_v1_job = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "chunks", "incremental fixture read another chunk plan");
        return { schema: 1, scope: "candidate-chunk-plan-pages", allCount: 0, pageMaxHashes: 256 };
    };
    tree.read_candidate_chunks_page_v1_job = () => {
        throw new Error("empty incremental fixture unexpectedly read a chunk page");
    };
    tree.finish_candidate_chunks_plan_v1_job = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "chunks", "incremental fixture sealed another chunk plan");
        job.retiring = true;
    };

    const beginRootExport = (bytes: Uint8Array | undefined, maxArenaBytes: number): number => {
        requireFixture(bytes !== undefined && bytes.byteLength > 0 && bytes.byteLength <= maxArenaBytes,
            "incremental fixture received an invalid root export bound");
        const token = begin("root-export");
        active!.bytes = bytes;
        active!.exportStage = "plan";
        return token;
    };
    tree.begin_candidate_root_export_job = (maxArenaBytes: number) =>
        beginRootExport(tree.candidate_root_bytes(), maxArenaBytes);
    tree.begin_committed_root_export_job = (maxArenaBytes: number) =>
        beginRootExport(tree.root_bytes(), maxArenaBytes);
    tree.step_root_export_job = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "root-export" && (job.exportStage === "plan" || job.exportStage === "build"),
            "incremental fixture stepped an invalid root export phase");
        job.exportStage = job.exportStage === "plan" ? "planned" : "built";
        return { done: true, units: 1, bytes: 256, completed: 1, processed: 256 };
    };
    tree.root_export_workset = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "root-export" && job.exportStage === "planned",
            "incremental fixture read an unplanned root export");
        return { max_length: job.bytes!.byteLength, offset_bytes: 0 };
    };
    tree.start_root_export_build_job = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "root-export" && job.exportStage === "planned",
            "incremental fixture built an unplanned root export");
        job.exportStage = "build";
    };
    tree.root_export_info = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "root-export" && job.exportStage === "built",
            "incremental fixture inspected an unfinished root export");
        return { length: job.bytes!.byteLength, version: tree.tree_version(), hash: hashRootBytes(job.bytes!) };
    };
    tree.read_root_export_job = (token: number, offset: number, maxBytes: number) => {
        const job = owned(token);
        requireFixture(job.kind === "root-export" && job.exportStage === "built",
            "incremental fixture read an unfinished root export");
        return job.bytes!.slice(offset, Math.min(job.bytes!.byteLength, offset + maxBytes));
    };
    tree.finish_root_export_job = (token: number) => {
        const job = owned(token);
        requireFixture(job.kind === "root-export" && job.exportStage === "built",
            "incremental fixture finished an incomplete root export");
        active = null;
    };

    tree.commit_candidate = () => { const result = commitCandidate(); candidateRevision++; return result; };
    tree.abort_candidate = () => { const result = abortCandidate(); candidateRevision++; return result; };
}
