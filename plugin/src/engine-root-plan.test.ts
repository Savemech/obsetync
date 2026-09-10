import { strict as assert } from "node:assert";
import { TFile } from "obsidian";
import { fixture, hashFor, pathAt, gate, observe, turns, rows, type Fixture } from "./engine-root-test-fixture";
import { JOURNAL_STORE_PATH } from "./journal";
import { ROOT_INTENT_PATH } from "./root-intent";
import { SYNC_BASE_STORE_PATH } from "./sync-base";
import type { RootCommitIntent } from "./root-outcome";
import type { DirtyFileChange } from "./dirty-set";
import { disposeWorkScheduler } from "./work-scheduler";
import { ROOT_REVIEW_LIMITS } from "./root-reviewed-queue";

/** Actual constructor/drain/root outcome/base/journal paths. Adapter/transport
 * and the transactional JSON tree are synthetic, as documented by the shared
 * fixture. These are deterministic work/ordering assertions, not timing,
 * native-WASM/Obsidian/mobile responsiveness or RSS measurements. */
function observeWork(engine: any) {
    const work = { materializations: [] as number[], stats: 0, statPaths: new Map<string, number>(),
        claims: [] as number[], claimedPaths: 0, takes: [] as number[], restores: [] as number[], continuations: 0 };
    const originals = { stat: engine.io.stat, materialize: engine.materializeDirtyChanges,
        claim: engine.pendingChanges.claimHints, take: engine.pendingChanges.take,
        restore: engine.pendingChanges.restore, heavy: engine.waitForHeavyWork };
    engine.io.stat = async (...args: any[]) => {
        work.stats++; work.statPaths.set(args[0], (work.statPaths.get(args[0]) ?? 0) + 1);
        return originals.stat.apply(engine.io, args);
    };
    engine.materializeDirtyChanges = (...args: any[]) => {
        work.materializations.push(args[0].length);
        return originals.materialize.apply(engine, args);
    };
    engine.pendingChanges.claimHints = (...args: any[]) => {
        work.claims.push(args[0].length);
        const result = originals.claim.apply(engine.pendingChanges, args);
        if (result) work.claimedPaths += result.length;
        return result;
    };
    engine.pendingChanges.take = (...args: any[]) => {
        const result = originals.take.apply(engine.pendingChanges, args); work.takes.push(result.length); return result;
    };
    engine.pendingChanges.restore = (...args: any[]) => {
        work.restores.push(args[0].length); return originals.restore.apply(engine.pendingChanges, args);
    };
    engine.waitForHeavyWork = (...args: any[]) => {
        if (args[0] === "root continuation") work.continuations++;
        return originals.heavy.apply(engine, args);
    };
    return work;
}

const paths = (count: number) => Array.from({ length: count }, (_, index) => pathAt(index));
const sum = (values: number[]) => values.reduce((total, value) => total + value, 0);
function requestHashAt(request: RootCommitIntent, path: string): string | undefined {
    const decoded = JSON.parse(Buffer.from(request.root, "base64").toString("utf8"));
    return decoded.entries.find((entry: [string, { hash: string }]) => entry[0] === path)?.[1].hash;
}
async function assertFinished(f: Fixture, engine: any) {
    assert.equal(f.server.legacy, 0); assert.equal(engine.pendingChanges.size, 0);
    assert.equal(f.journal.unsyncedCount(), 0); assert.equal(engine.rootRuntime.pending(), null);
    assert.equal(f.base.treeBaseRoot, f.server.root);
    const cold = await f.cold();
    assert.equal(cold.pending, null); assert.equal(cold.journal.unsyncedCount(), 0);
    assert.equal(cold.base.treeBaseRoot, f.server.root); assert.equal(cold.sequence, f.server.floor);
}
async function deleteEvent(f: Fixture, engine: any, path: string) {
    f.source.delete(path);
    const ref = engine.eventRefs.find((item: any) => item.event === "delete"); assert(ref);
    await ref.callback(Object.assign(new TFile(), { path, stat: { size: 1, mtime: 2 } }));
}
async function renameEvent(f: Fixture, engine: any, oldPath: string, newPath: string) {
    const version = f.source.get(oldPath)!; f.source.delete(oldPath); f.source.set(newPath, version);
    const ref = engine.eventRefs.find((item: any) => item.event === "rename"); assert(ref);
    await ref.callback(Object.assign(new TFile(), { path: newPath, stat: { size: 1, mtime: version } }), oldPath);
}

async function quietDrainHasOneFullReviewAndBoundedRevalidation() {
    for (const count of [600, 1024, 2500]) {
        const f = await fixture(count), a = await f.create(`quiet-${count}`);
        try {
            const ids = await f.queue(a.engine, paths(count), 2), work = observeWork(a.engine), cut = f.storage.events.length;
            await a.engine.pushPending();
            const batches = Array.from({ length: Math.ceil(count / 256) }, (_, index) => Math.min(256, count - index * 256));
            assert.deepEqual(f.server.publicationSizes, batches);
            assert.deepEqual(work.materializations, [count, ...batches], "full remaining queue was rematerialized between roots");
            assert.equal(work.stats, 2 * count, "quiet source audit/revalidation must visit each path twice");
            assert.equal(work.statPaths.size, count); assert([...work.statPaths.values()].every(visits => visits === 2));
            assert(work.claims.length > 0 && work.claims.every(size => size > 0 && size <= 256));
            assert.equal(work.claimedPaths, count); assert.equal(sum(work.claims), count);
            assert.deepEqual(work.takes, [], "durable review detached the full dirty backlog");
            assert(work.restores.every(size => size <= 256), "durable review restored an unbounded remainder");
            assert.equal(work.continuations, batches.length - 1);
            const publishedCuts = f.server.cuts.flat();
            assert.equal(publishedCuts.length, count);
            assert.deepEqual(new Map(publishedCuts.map(value => [value.path, value.throughId])),
                new Map(paths(count).map((path, index) => [path, ids[index]])));
            for (const path of paths(count)) {
                assert.equal(f.base.getHash(path), hashFor(2)); assert.equal(f.server.entries.get(path)?.hash, hashFor(2));
            }
            const operations = f.storage.events.slice(cut).flatMap(event => rows(event, SYNC_BASE_STORE_PATH));
            assert.equal(operations.filter(row => row.op === "root-publication").length, batches.length);
            assert(!operations.some(row => row.op === "tree-root" || row.op === "timestamp"));
            await assertFinished(f, a.engine);
        } finally { await f.close(); }
    }
}

async function editsDuringReviewAndSelectedStatNeverPublishStaleGeneration() {
    for (const phase of ["review", "selected"] as const) {
        const f = await fixture(300), a = await f.create(phase), held = gate();
        try {
            const old = await f.queue(a.engine, paths(300), 2), target = pathAt(0), stat = a.engine.io.stat.bind(a.engine.io);
            let visits = 0;
            a.engine.io.stat = async (path: string) => {
                if (path === target && ++visits === (phase === "review" ? 1 : 2)) await held.wait();
                return stat(path);
            };
            const pending = observe(a.engine.pushPending()); await held.entered;
            assert.equal(f.server.requests.length, 0);
            const newer = await f.event(a.engine, target, 3); assert(newer > old[0]);
            held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
            // A source-drift attempt may safely end this drain; an explicit
            // retry must retain the new generation rather than the old cut.
            if (a.engine.pendingChanges.size) await a.engine.pushPending();
            assert(f.server.requests.every(request => requestHashAt(request, target) !== hashFor(2)),
                `${phase}: stale reviewed source was published after actual callback`);
            assert.equal(f.base.getHash(target), hashFor(3));
            assert(f.server.cuts.flat().some(cut => cut.path === target && cut.throughId === newer));
            await assertFinished(f, a.engine);
        } finally { held.resolve(); await f.close(); }
    }
}

async function approvedInitialReviewPreemptsIntoOneDurableRecentPrefix() {
    const count = 600, f = await fixture(count), a = await f.create("initial-review-prefix");
    const recent = Array.from({ length: 70 }, (_, index) => `first-review-${index}.md`);
    const generations = new Map<string, number>();
    const heavy = a.engine.waitForHeavyWork.bind(a.engine), work = observeWork(a.engine);
    let injected = false, injectionStats = -1, tailSeenAtFirstRoot: boolean | undefined;
    try {
        await f.queue(a.engine, paths(count), 2);
        f.base.approveBulkChange("vault", count, 0);
        await f.base.save();
        a.engine.waitForHeavyWork = async (...args: any[]) => {
            await heavy(...args);
            if (args[0] !== "push metadata" || injected) return;
            injected = true; injectionStats = work.stats;
            for (const path of recent) {
                f.control.activePath = path;
                generations.set(path, await f.event(a.engine, path, 3));
            }
        };
        f.control.beforeRootAnswer = async () => {
            tailSeenAtFirstRoot ??= work.statPaths.has(pathAt(count - 1));
        };
        await a.engine.pushPending();
        assert(injected); assert.equal(injectionStats, 256,
            "initial review missed the first bounded metadata cut");
        assert.equal(tailSeenAtFirstRoot, false,
            "recent prefix waited for the unreviewed materialization suffix");
        assert.deepEqual(f.server.cuts.slice(0, 2).map(cuts => cuts.length), [64, 6]);
        assert(f.server.cuts.slice(0, 2).flat().every(cut => generations.get(cut.path) === cut.throughId),
            "recent prefix selected bulk, stale or non-durable generations");
        assert(f.server.cuts.slice(2).flat().every(cut => cut.path.startsWith("note-")),
            "one-shot recent prefix leaked into the mandatory whole rebuild");
        assert.equal(work.materializations.filter(size => size === count).length, 2,
            "whole review was not interrupted once and rebuilt once");
        assert.equal(work.materializations.filter(size => size === recent.length).length, 1,
            "recent prefix was rebuilt or rematerialized as a whole review");
        for (const path of recent) assert.equal(f.base.getHash(path), hashFor(3));
        for (const path of paths(count)) assert.equal(f.base.getHash(path), hashFor(2));
        await assertFinished(f, a.engine);
    } finally {
        a.engine.waitForHeavyWork = heavy;
        f.control.beforeRootAnswer = undefined;
        await f.close();
    }
}

async function activeRecentPathBeyondThePrefixLimitStillRunsInThePrefix() {
    const count = 600, f = await fixture(count), a = await f.create("initial-review-active-prefix");
    const recent = Array.from({ length: ROOT_REVIEW_LIMITS.initialRecentPaths + 12 }, (_, index) =>
        `active-prefix-${String(index).padStart(3, "0")}.md`);
    const active = recent.at(-1)!;
    const generations = new Map<string, number>();
    const heavy = a.engine.waitForHeavyWork.bind(a.engine);
    let injected = false;
    try {
        await f.queue(a.engine, paths(count), 2);
        f.base.approveBulkChange("vault", count, 0);
        await f.base.save();
        a.engine.waitForHeavyWork = async (...args: any[]) => {
            await heavy(...args);
            if (args[0] !== "push metadata" || injected) return;
            injected = true;
            for (const path of recent) generations.set(path, await f.event(a.engine, path, 3));
            f.control.activePath = active;
        };
        await a.engine.pushPending();
        assert(injected, "fixture did not inject the oversized recent set");
        const firstPrefixCut = f.server.cuts[0];
        assert.equal(firstPrefixCut.length, ROOT_REVIEW_LIMITS.urgentPaths,
            "recent prefix did not use its bounded urgent publication cut");
        assert(firstPrefixCut.some(cut => cut.path === active && cut.throughId === generations.get(active)),
            "active recent path outside insertion-order prefix was not promoted into the bounded prefix");
        assert(firstPrefixCut.every(cut => generations.get(cut.path) === cut.throughId),
            "bounded recent prefix admitted a bulk or stale generation");
        assert(f.server.cuts.slice(1).flat().some(cut => cut.path === recent[ROOT_REVIEW_LIMITS.initialRecentPaths - 1]),
            "whole review lost a recent path displaced by active-path promotion");
        await assertFinished(f, a.engine);
    } finally {
        a.engine.waitForHeavyWork = heavy;
        await f.close();
    }
}

async function failedRecentPrefixDebtSurvivesDrainAndHandoff() {
    const count = 600, f = await fixture(count), a = await f.create("failed-prefix-debt");
    const recent = "failed-prefix-live.md";
    const injectedFailure = Object.assign(new Error("synthetic recent prefix stat failure"), { code: "EIO" });
    const heavy = a.engine.waitForHeavyWork.bind(a.engine), firstWork = observeWork(a.engine);
    let injected = false;
    try {
        await f.queue(a.engine, paths(count), 2);
        f.base.approveBulkChange("vault", count, 0);
        await f.base.save();
        a.engine.waitForHeavyWork = async (...args: any[]) => {
            await heavy(...args);
            if (args[0] !== "push metadata" || injected) return;
            injected = true;
            await f.event(a.engine, recent, 3);
            f.control.statErrors.set(recent, injectedFailure);
        };

        await a.engine.pushPending();
        assert(injected, "fixture did not inject the failing recent prefix");
        assert.equal(f.server.requests.length, 0, "failed recent prefix published a root");
        assert.deepEqual(firstWork.materializations.slice(0, 2), [count, 1],
            "fixture did not preempt one whole review and fail its bounded prefix");
        assert.equal(a.engine.rootInitialReviewPreempted, true,
            "failed prefix repaid the mandatory whole-review debt");
        assert.equal(a.engine.pendingChanges.size, count + 1,
            "failed prefix lost dirty work before the next drain");

        f.control.statErrors.clear();
        await a.engine.quiesceAndDrain();
        const b = await f.create("failed-prefix-replacement", { active: false });
        await a.engine.handoffCaptureTo(b.engine);
        assert.equal(b.engine.rootInitialReviewPreempted, true,
            "capture handoff discarded the mandatory whole-review debt");
        await b.engine.prepareLocal();
        Object.assign(b.engine, { startupReplayInProgress: false, startupCompleted: true, state: "idle" });
        const secondWork = observeWork(b.engine);

        await b.engine.pushPending();
        assert.equal(secondWork.materializations[0], count + 1,
            "second drain preempted again instead of completing the owed whole review");
        assert.equal(secondWork.materializations.filter(size => size === count + 1).length, 1,
            "second drain abandoned or rebuilt the owed whole review");
        assert.equal(b.engine.rootInitialReviewPreempted, false,
            "completed ordinary owner did not repay whole-review debt");
        assert(f.server.cuts.flat().some(cut => cut.path === recent),
            "owed whole review did not publish the retained recent generation");
        await assertFinished(f, b.engine);
    } finally {
        f.control.statErrors.clear();
        a.engine.waitForHeavyWork = heavy;
        await f.close();
    }
}

async function unsafeInitialReviewCannotUseTheRecentPrefixWithoutPolicyAuthority() {
    for (const approval of ["absent", "exceeded"] as const) {
        const count = 300, f = await fixture(count), a = await f.create(`initial-prefix-${approval}`);
        const heavy = a.engine.waitForHeavyWork.bind(a.engine), work = observeWork(a.engine);
        let injected = false, tailSeenAtFirstRoot: boolean | undefined;
        try {
            await f.queue(a.engine, paths(count), 2);
            if (approval === "exceeded") {
                f.base.approveBulkChange("vault", 100, 0);
                await f.base.save();
            }
            a.engine.waitForHeavyWork = async (...args: any[]) => {
                await heavy(...args);
                if (args[0] !== "push metadata" || injected) return;
                injected = true; f.control.activePath = "policy-live.md";
                await f.event(a.engine, "policy-live.md", 3);
            };
            f.control.beforeRootAnswer = async () => {
                tailSeenAtFirstRoot ??= work.statPaths.has(pathAt(count - 1));
            };
            await a.engine.pushPending();
            assert(injected, `${approval}: policy fixture missed metadata injection`);
            assert.equal(tailSeenAtFirstRoot, true,
                `${approval}: unsafe gross backlog used recent-prefix authority`);
            assert.equal(work.materializations.filter(size => size === count).length, 1,
                `${approval}: whole review was preempted without a valid policy envelope`);
            assert.equal(f.base.getHash("policy-live.md"), hashFor(3));
            await assertFinished(f, a.engine);
        } finally {
            a.engine.waitForHeavyWork = heavy;
            f.control.beforeRootAnswer = undefined;
            await f.close();
        }
    }
}

async function editsDuringActualObjectUploadFenceTheRoot() {
    const f = await fixture(300), a = await f.create("upload-edit"), held = gate();
    try {
        await f.queue(a.engine, paths(300), 2);
        let uploads = 0;
        const present = new Set<string>();
        a.engine.api.checkContent = async (hashes: string[]) => hashes.filter(hash => !present.has(hash));
        a.engine.api.putObjects = async (records: Array<{ hash: string; data: Uint8Array }>) => {
            uploads++;
            for (const record of records) {
                assert.equal(record.data.length, 1); assert.equal(record.hash, hashFor(record.data[0]));
            }
            if (uploads === 1) await held.wait();
            for (const record of records) present.add(record.hash);
        };
        const pending = observe(a.engine.pushPending()); await held.entered;
        assert.equal(f.server.requests.length, 0);
        const newer = await f.event(a.engine, pathAt(0), 3);
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        if (a.engine.pendingChanges.size) await a.engine.pushPending();
        assert(f.server.requests.every(request => requestHashAt(request, pathAt(0)) !== hashFor(2)));
        assert(f.server.cuts.flat().some(cut => cut.path === pathAt(0) && cut.throughId === newer));
        assert.equal(f.base.getHash(pathAt(0)), hashFor(3)); assert(uploads >= 2);
        await assertFinished(f, a.engine);
    } finally { held.resolve(); await f.close(); }
}

async function urgentActiveNoteAndBulkBothMakeProgress() {
    const f = await fixture(600), a = await f.create("fairness"), held = gate();
    try {
        await f.queue(a.engine, paths(600), 2);
        f.control.beforeRootAnswer = async () => { if (f.server.requests.length === 1) await held.wait(); };
        const pending = observe(a.engine.pushPending()); await held.entered;
        const edits = Array.from({ length: 70 }, (_, index) => `live-${String(index).padStart(3, "0")}.md`);
        f.control.activePath = edits.at(-1)!;
        // Real callbacks create durable new generations while an accepted
        // root is still awaiting its response. No manual dirty priority edits.
        for (const path of edits) await f.event(a.engine, path, 3);
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        const cuts = f.server.cuts.map(batch => new Set(batch.map(cut => cut.path)));
        const urgentRoots = cuts.map((cut, index) => ({ cut, index })).filter(({ cut }) => [...cut].some(path => edits.includes(path)));
        assert(urgentRoots.length >= 2, "70 urgent paths must respect the separate 64-path root limit");
        assert(urgentRoots.every(({ cut }) => cut.size <= 64 && [...cut].every(path => edits.includes(path))));
        assert(urgentRoots[0].cut.has(f.control.activePath), "active note was left behind older urgent notes");
        const lastBulk = Math.max(...cuts.map((cut, index) => [...cut].some(path => path.startsWith("note-")) ? index : -1));
        assert(urgentRoots[0].index < lastBulk, "active note waited for the entire static backlog");
        assert(cuts.slice(urgentRoots[0].index + 1, urgentRoots[1].index).some(cut => [...cut].some(path => path.startsWith("note-"))),
            "urgent traffic starved the admitted bulk backlog");
        for (const path of edits) assert.equal(f.base.getHash(path), hashFor(3));
        for (const path of paths(600)) assert.equal(f.base.getHash(path), hashFor(2));
        await assertFinished(f, a.engine);
    } finally { held.resolve(); await f.close(); }
}

async function inFlightUrgentCallbackDoesNotDispatchItsSupersededRow() {
    const f = await fixture(600), a = await f.create("in-flight-urgent"), firstRoot = gate(), callback = gate();
    const active = "live-active.md", peer = "live-peer.md";
    const append = f.journal.append.bind(f.journal);
    let holdNextActive = false, peerAcceptedResolve!: () => void;
    const peerAccepted = new Promise<void>(resolve => { peerAcceptedResolve = resolve; });
    try {
        await f.queue(a.engine, paths(600), 2);
        const work = observeWork(a.engine);
        f.control.beforeRootAnswer = async () => {
            if (f.server.requests.length === 1) await firstRoot.wait();
            if (f.server.cuts.at(-1)?.some(cut => cut.path === peer)) peerAcceptedResolve();
        };
        const drain = observe(a.engine.pushPending());
        await firstRoot.entered;

        await f.event(a.engine, active, 3);
        const peerGeneration = await f.event(a.engine, peer, 3);
        f.control.activePath = active;
        f.journal.append = async record => {
            if (holdNextActive && record.path === active) {
                holdNextActive = false;
                await callback.wait();
            }
            return append(record);
        };
        holdNextActive = true;
        const newerCallback = observe(f.event(a.engine, active, 4));
        await callback.entered;

        firstRoot.resolve();
        await peerAccepted;
        assert.equal(newerCallback.state.settled, false,
            "fixture released the newer active callback before urgent selection");
        assert(f.server.cuts.flat().some(cut => cut.path === peer && cut.throughId === peerGeneration),
            "completed urgent peer did not progress around the in-flight active callback");
        assert(f.server.cuts.flat().every(cut => cut.path !== active),
            "superseded active generation crossed root dispatch while its callback was in flight");

        callback.resolve();
        await newerCallback.done;
        assert.equal(newerCallback.state.error, undefined);
        const activeGeneration = newerCallback.state.value!;
        await drain.done;
        assert.equal(drain.state.error, undefined);
        assert(f.server.cuts.flat().some(cut => cut.path === active && cut.throughId === activeGeneration),
            "completed active callback did not publish its exact durable generation");
        assert(f.server.cuts.flat().filter(cut => cut.path === active)
            .every(cut => cut.throughId === activeGeneration),
        "in-flight scheduling fence published the superseded active generation");
        assert.equal(work.materializations.filter(count => count === 600).length, 1,
            "in-flight urgent callback discarded and rebuilt the whole reviewed backlog");
        assert.equal(a.engine.rootRepairRequired, false,
            "an unstarted in-flight urgent generation created root recovery debt");
        await assertFinished(f, a.engine);
    } finally {
        firstRoot.resolve(); callback.resolve(); f.control.beforeRootAnswer = undefined;
        f.journal.append = append;
        await f.close();
    }
}

async function streamingCallbacksKeepUrgencyAcrossReviewOwnerRebuild() {
    const f = await fixture(600), a = await f.create("streaming-urgency");
    const trigger = "stream-trigger.md";
    const edits = Array.from({ length: 70 }, (_, index) =>
        `stream-${String(index).padStart(3, "0")}.md`);
    const generations = new Map<string, number>();
    const heavy = a.engine.waitForHeavyWork.bind(a.engine);
    let triggerInjected = false, waveInjected = false, injectionRootCount = -1;
    const inject = async (wave: readonly string[], record: boolean) => {
        for (const path of wave) {
            f.control.activePath = path;
            const generation = await f.event(a.engine, path, 3);
            if (record) generations.set(path, generation);
        }
    };
    try {
        await f.queue(a.engine, paths(600), 2);
        // Missing content traverses the aggregate check/read/upload path that
        // owns the <=32-file responsive pre-dispatch boundaries.
        f.control.contentMissing = true;
        f.control.beforeContentCheck = async () => {
            if (triggerInjected) return;
            triggerInjected = true;
            // This single callback forces an ACK-safe bulk prefix, then owns
            // the immediately following urgent root. Its settlement exhausts
            // the old overlay and causes the review owner to be rebuilt.
            await inject([trigger], false);
        };
        a.engine.waitForHeavyWork = async (...args: any[]) => {
            await heavy(...args);
            if (args[0] !== "root review rebuild" || waveInjected) return;
            waveInjected = true;
            injectionRootCount = f.server.cuts.length;
            // The exhausted preempted owner is already disposed and the next
            // owner has not captured pending state yet. These callbacks enter
            // its initial reviewed snapshot, not a live overlay. Volatile
            // recent-local provenance is the only scheduling reason that the
            // safe rebuild may prioritize them over old bulk.
            await inject(edits, true);
        };

        await a.engine.pushPending();
        assert(triggerInjected && waveInjected && injectionRootCount >= 3,
            "fixture did not inject callbacks between exhausted and rebuilt review owners");

        const acceptedRoot = new Map<string, number>();
        f.server.cuts.forEach((cuts, root) => {
            for (const cut of cuts) if (generations.has(cut.path)) {
                assert.equal(cut.throughId, generations.get(cut.path),
                    `streaming edit ${cut.path} published the wrong journal generation`);
                assert(!acceptedRoot.has(cut.path), `streaming edit ${cut.path} was published twice`);
                acceptedRoot.set(cut.path, root);
            }
        });
        assert.equal(acceptedRoot.size, edits.length, "streaming callbacks were not all published");
        const opportunities = edits.map(path => acceptedRoot.get(path)! + 1 - injectionRootCount)
            .sort((left, right) => left - right);
        const p95Opportunity = opportunities[Math.ceil(opportunities.length * 0.95) - 1];
        assert(p95Opportunity <= 3,
            `p95-like callback publication escaped three subsequent root opportunities: ${p95Opportunity}`);
        const p95Path = edits[66];
        assert(acceptedRoot.get(p95Path)! + 1 - injectionRootCount <= 3,
            "the 67th callback waited behind the rebuilt static backlog");

        const bulkRoots = f.server.cuts.map((cuts, root) => ({ root,
            bulk: cuts.some(cut => cut.path.startsWith("note-")) })).filter(row => row.bulk);
        assert(bulkRoots.some(row => row.root < acceptedRoot.get(p95Path)!),
            "urgent callbacks ran without earlier bulk progress");
        assert(bulkRoots.some(row => row.root > acceptedRoot.get(p95Path)!),
            "streaming urgent callbacks starved the remaining bulk backlog");
        for (const path of edits) assert.equal(f.base.getHash(path), hashFor(3));
        for (const path of paths(600)) assert.equal(f.base.getHash(path), hashFor(2));
        await assertFinished(f, a.engine);
    } finally {
        f.control.beforeContentCheck = undefined;
        a.engine.waitForHeavyWork = heavy;
        await f.close();
    }
}

async function selectedRootPreemptsForUrgentSaveAtNextPublicationOpportunity() {
    const f = await fixture(600), a = await f.create("slice-urgent");
    const urgent = pathAt(599);
    let injected = false, urgentGeneration = 0;
    try {
        await f.queue(a.engine, paths(600), 2);
        f.control.beforeContentCheck = async () => {
            if (injected) return;
            injected = true;
            f.control.activePath = urgent;
            urgentGeneration = await f.event(a.engine, urgent, 3);
        };
        await a.engine.pushPending();
        assert(injected, "fixture did not save the active note inside the selected 256-path root");
        assert.equal(f.server.publicationSizes[0], 32,
            "bulk root did not stop at its first ACK-safe preemption boundary");
        assert(f.server.cuts[1]?.some(cut => cut.path === urgent && cut.throughId === urgentGeneration),
            "urgent save did not receive the immediately following root publication opportunity");
        const accepted = f.server.cuts.flat();
        assert.equal(accepted.length, 600, "sliced drain lost or duplicated a durable path generation");
        assert.equal(new Set(accepted.map(cut => cut.path)).size, 600,
            "sliced drain published a path more than once");
        assert.equal(f.base.getHash(urgent), hashFor(3));
        for (const path of paths(599)) assert.equal(f.base.getHash(path), hashFor(2));
        await assertFinished(f, a.engine);
    } finally {
        f.control.beforeContentCheck = undefined;
        await f.close();
    }
}

async function cancelledRootSliceRestoresEveryGenerationForReplacement() {
    const f = await fixture(600), a = await f.create("slice-cancel"), held = gate();
    let intercepted = false;
    try {
        await f.queue(a.engine, paths(600), 2);
        f.control.beforeContentCheck = async () => {
            if (intercepted) return;
            intercepted = true;
            await held.wait();
        };
        const pushing = observe(a.engine.pushPending());
        await held.entered;
        const stopping = observe(a.engine.stopAndDrain());
        await turns();
        assert.equal(stopping.state.settled, false,
            "engine stop released while a selected root still owned its content check");
        held.resolve();
        await pushing.done; await stopping.done;
        assert.equal(pushing.state.error, undefined); assert.equal(stopping.state.error, undefined);
        assert.equal(f.server.requests.length, 0, "cancelled selected root reached publication");
        assert.equal(a.engine.pendingChanges.size, 600, "cancelled selected root lost detached dirty hints");
        assert.equal(f.journal.unsyncedCount(), 600, "cancelled selected root acknowledged WAL generations");

        f.control.beforeContentCheck = undefined;
        const b = await f.create("slice-cancel-retry");
        await b.engine.pushPending();
        const accepted = f.server.cuts.flat();
        assert.equal(accepted.length, 600, "replacement retry lost or duplicated cancelled generations");
        assert.equal(new Set(accepted.map(cut => cut.path)).size, 600,
            "replacement retry published a cancelled path twice");
        await assertFinished(f, b.engine);
    } finally {
        held.resolve(); f.control.beforeContentCheck = undefined;
        await f.close();
    }
}

async function driftedUrgentSelectionKeepsItsReviewedPriority() {
    const f = await fixture(600), a = await f.create("urgent-drift"), held = gate();
    const edits = Array.from({ length: 70 }, (_, index) => `live-race-${String(index).padStart(3, "0")}.md`);
    const active = edits.at(-1)!;
    let injected = false, activeGeneration = 0;
    try {
        await f.queue(a.engine, paths(600), 2);
        f.control.activePath = active;
        f.control.beforeRootAnswer = async () => { if (f.server.requests.length === 1) await held.wait(); };
        const present = new Set<string>();
        a.engine.api.checkContent = async (hashes: string[]) => hashes.filter(hash => hash === hashFor(3) && !present.has(hash));
        a.engine.api.putObjects = async (records: Array<{ hash: string; data: Uint8Array }>) => {
            if (!injected) {
                injected = true;
                // Same bytes and metadata, but a new durable callback owner:
                // only the generation guard can reject this transaction.
                activeGeneration = await f.event(a.engine, active, 3);
            }
            for (const record of records) present.add(record.hash);
        };
        const first = observe(a.engine.pushPending()); await held.entered;
        for (const path of edits) await f.event(a.engine, path, 3);
        held.resolve(); await first.done;
        assert.equal(first.state.error, undefined); assert.equal(injected, true, "fixture did not create selected-source drift");
        assert.equal(a.engine.pendingChanges.size, 0, "bounded in-owner drift retry did not finish the manual drain");
        f.control.beforeRootAnswer = undefined;

        const cuts = f.server.cuts.map(batch => new Set(batch.map(cut => cut.path)));
        const urgentRoots = cuts.map((cut, index) => ({ cut, index })).filter(({ cut }) => [...cut].some(path => edits.includes(path)));
        const acceptedLive = urgentRoots.flatMap(({ cut }) => [...cut].filter(path => edits.includes(path)));
        assert.equal(acceptedLive.length, edits.length, "retry duplicated an accepted live cut");
        assert.equal(new Set(acceptedLive).size, edits.length, "retry lost a live path");
        assert(urgentRoots.length >= 2 && urgentRoots.every(({ cut }) => cut.size <= 64 && [...cut].every(path => edits.includes(path))),
            "drift retry bypassed the bounded independent urgent lane");
        assert(urgentRoots[0].index >= 2 && urgentRoots[0].cut.has(active),
            "active drifted note did not lead the next invocation's urgent selection");
        const lastBulk = Math.max(...cuts.map((cut, index) => [...cut].some(path => path.startsWith("note-")) ? index : -1));
        assert(urgentRoots[0].index < lastBulk, "source drift demoted live work behind the remaining static backlog");
        assert(f.server.cuts.flat().some(cut => cut.path === active && cut.throughId === activeGeneration),
            "retry did not publish the newest active-note journal generation");
        assert(f.server.cuts.flat().filter(cut => cut.path === active).every(cut => cut.throughId === activeGeneration),
            "root dispatch published the superseded active-note journal generation");
        for (const path of edits) assert.equal(f.base.getHash(path), hashFor(3));
        for (const path of paths(600)) assert.equal(f.base.getHash(path), hashFor(2));
        await assertFinished(f, a.engine);
    } finally {
        held.resolve(); f.control.beforeRootAnswer = undefined; await f.close();
    }
}

async function repeatedDriftAndReviewRebuildStayDrainBounded() {
    const f = await fixture(64), a = await f.create("drift-bound");
    const target = pathAt(0), heavy = a.engine.waitForHeavyWork.bind(a.engine);
    let injections = 0, rebuildFences = 0;
    try {
        await f.queue(a.engine, paths(64), 2); await f.event(a.engine, target, 3); f.control.activePath = target;
        f.control.beforeRead = async path => {
            if (path !== target || injections > ROOT_REVIEW_LIMITS.overlayRefreshes) return;
            injections++;
            await f.event(a.engine, target, 3 + injections);
        };
        a.engine.waitForHeavyWork = async (...args: any[]) => {
            await heavy(...args);
            if (args[0] === "root continuation") {
                rebuildFences++;
                a.engine.syncPriority = a.engine.syncPriority === "oldest" ? "newest" : "oldest";
            }
        };

        const bounded = observe(a.engine.pushPending()); await bounded.done;
        assert.equal(bounded.state.error, undefined);
        assert.equal(injections, ROOT_REVIEW_LIMITS.overlayRefreshes + 1,
            "drain did not stop after its finite source-drift continuation budget");
        assert.equal(rebuildFences, ROOT_REVIEW_LIMITS.overlayRefreshes,
            "review-owner rebuild reset or prematurely consumed the drain-wide budget");
        assert.equal(f.server.requests.length, 0, "drifted source generation crossed root dispatch");
        assert.equal(a.engine.pendingChanges.size, 64, "bounded drift exit lost coalesced dirty paths");

        f.control.beforeRead = undefined; a.engine.waitForHeavyWork = heavy;
        await a.engine.pushPending();
        assert.equal(f.base.getHash(target), hashFor(3 + injections));
        await assertFinished(f, a.engine);
    } finally {
        f.control.beforeRead = undefined; a.engine.waitForHeavyWork = heavy; await f.close();
    }
}

async function coolingSourceOutsideSelectedRootStillHoldsDeletes() {
    const f = await fixture(600), a = await f.create("cooling");
    try {
        await f.queue(a.engine, paths(600).slice(1), 2); await deleteEvent(f, a.engine, pathAt(0));
        const cooled = [...a.engine.pendingChanges.iterate()].find((hint: DirtyFileChange) => hint.path === pathAt(599))!;
        // Explicitly seed actual retry metadata; the one-byte fixture does not
        // claim to reproduce a device memory-capacity refusal.
        a.engine.deferredChanges.settle([cooled], [cooled], [{ path: cooled.path, reason: "source-too-large",
            requiredBytes: 128, capacityBytes: 64 }], a.engine.deferredRetryKey(), Date.now());
        await a.engine.pushPending();
        assert(f.server.requests.length >= 2);
        assert(f.server.cuts.flat().every(cut => cut.path !== pathAt(0) && cut.path !== pathAt(599)));
        assert.equal(f.base.getHash(pathAt(0)), hashFor(1)); assert.equal(f.base.getHash(pathAt(599)), hashFor(1));
        assert.equal(a.engine.pendingChanges.size, 2); assert.equal(f.journal.unsyncedCount(), 2);
        assert.equal(f.server.legacy, 0);
    } finally { await f.close(); }
}

async function unexpectedTrackedMissingForcesWholeRemainderAudit() {
    const f = await fixture(600), a = await f.create("missing"), held = gate();
    try {
        await f.queue(a.engine, paths(600), 2); const work = observeWork(a.engine);
        f.control.beforeRootAnswer = async () => { if (f.server.requests.length === 1) await held.wait(); };
        const pending = observe(a.engine.pushPending()); await held.entered;
        // No callback: selected-source revalidation must discover this drift.
        // Two hundred tracked removals cross the actual bulk-deletion guard.
        for (let index = 256; index < 456; index++) f.source.delete(pathAt(index));
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        assert.equal(f.server.requests.length, 1, "new unreviewed deletions crossed a cached review");
        assert.equal(a.engine.bulkChangeReviewRequired, true);
        assert(work.materializations.slice(1).includes(344), "classification drift did not trigger full remaining audit");
        assert.equal(a.engine.pendingChanges.size, 344); assert.equal(f.journal.unsyncedCount(), 344);
        for (let index = 256; index < 456; index++) assert.equal(f.base.getHash(pathAt(index)), hashFor(1));
    } finally { held.resolve(); await f.close(); }
}

async function newRenameDependencyCannotSplitAnOldPlannedCut() {
    const f = await fixture(600), a = await f.create("rename"), held = gate();
    try {
        await f.queue(a.engine, paths(600), 2); const work = observeWork(a.engine);
        f.control.beforeRootAnswer = async () => { if (f.server.requests.length === 1) await held.wait(); };
        const pending = observe(a.engine.pushPending()); await held.entered;
        await renameEvent(f, a.engine, pathAt(256), pathAt(599));
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        for (const cuts of f.server.cuts.slice(1)) {
            const selected = new Set(cuts.map(cut => cut.path));
            assert.equal(selected.has(pathAt(256)), selected.has(pathAt(599)), "new rename peers were split across cached roots");
        }
        assert(work.materializations.slice(1).includes(344), "new dependency reused the old graph without full audit");
        assert.equal(f.base.getHash(pathAt(256)), null); assert.equal(f.base.getHash(pathAt(599)), hashFor(2));
        await assertFinished(f, a.engine);
    } finally { held.resolve(); await f.close(); }
}

async function nativeAckAndStopRetainNewerCaptureForReplacement() {
    const f = await fixture(600), a = await f.create("ack-stop"), held = gate();
    try {
        await f.queue(a.engine, paths(600), 2);
        let intercepted = false;
        f.control.boundary = async event => {
            if (!intercepted && rows(event, JOURNAL_STORE_PATH).some(row => row.op === "ack")) {
                intercepted = true; await held.wait();
            }
        };
        const pending = observe(a.engine.pushPending()); await held.entered;
        assert.equal(f.server.requests.length, 1); assert.equal(f.base.getHash(pathAt(0)), hashFor(2));
        const incoming = observe(f.event(a.engine, pathAt(0), 3));
        await turns(); assert.equal(incoming.state.settled, false, "held ACK unexpectedly let the serialized journal append finish");
        const draining = observe(a.engine.quiesceAndDrain()); await turns(); assert.equal(draining.state.settled, false);
        held.resolve(); await pending.done; await incoming.done; await draining.done; f.control.boundary = undefined;
        assert.equal(incoming.state.error, undefined); const newest = incoming.state.value;
        assert.equal(pending.state.error, undefined); assert.equal(draining.state.error, undefined);
        assert.equal(f.server.requests.length, 1); assert.equal(a.engine.pendingChanges.size, 345);
        assert.equal(f.journal.unsynced().find(row => row.path === pathAt(0))?.id, newest);
        const b = await f.create("replacement", { active: false });
        await a.engine.handoffCaptureTo(b.engine); await b.engine.prepareLocal();
        Object.assign(b.engine, { startupReplayInProgress: false, startupCompleted: true, state: "idle" });
        const work = observeWork(b.engine); await b.engine.pushPending();
        assert.equal(work.materializations[0], 345, "replacement reused the stopped engine's reviewed cache");
        assert.equal(f.base.getHash(pathAt(0)), hashFor(3)); await assertFinished(f, b.engine);
    } finally { held.resolve(); f.control.boundary = undefined; await f.close(); }
}

async function pullBetweenRootsDiscardsThePriorReview() {
    const f = await fixture(600), a = await f.create("pull-between"), held = gate();
    try {
        await f.queue(a.engine, paths(600), 2); const work = observeWork(a.engine), heavy = a.engine.waitForHeavyWork.bind(a.engine);
        let intercepted = false;
        a.engine.waitForHeavyWork = async (...args: any[]) => {
            await heavy(...args);
            if (args[0] === "root continuation" && !intercepted) { intercepted = true; await held.wait(); }
        };
        const pending = observe(a.engine.pushPending()); await held.entered;
        const before = f.server.diffs; await a.engine.pullRemote(); assert.equal(f.server.diffs, before + 1);
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        assert(work.materializations.slice(1).includes(344), "ordinary pull left the prior reviewed cache applicable");
        await assertFinished(f, a.engine);
    } finally { held.resolve(); await f.close(); }
}

async function selectedStatFailureCannotBecomeAnOmissionAck() {
    const f = await fixture(300), a = await f.create("selected-io"), held = gate();
    try {
        await f.queue(a.engine, paths(300), 2); const work = observeWork(a.engine);
        f.control.beforeRootAnswer = async () => { if (f.server.requests.length === 1) await held.wait(); };
        const pending = observe(a.engine.pushPending()); await held.entered;
        f.control.statErrors.set(pathAt(256), Object.assign(new Error("synthetic selected stat unavailable"), { code: "EIO" }));
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        assert.equal(f.server.requests.length, 1); assert.equal(f.journal.unsyncedCount(), 44);
        assert.equal(a.engine.pendingChanges.size, 44); assert.equal(f.base.getHash(pathAt(256)), hashFor(1));
        assert.equal(a.engine.rootRuntime.pending(), null); assert(a.engine.lastError);
        f.control.statErrors.clear(); const before = work.materializations.length;
        await a.engine.pushPending();
        assert.equal(work.materializations[before], 44, "failed selected IO retained the old cache for a blind retry");
        await assertFinished(f, a.engine);
    } finally { held.resolve(); f.control.statErrors.clear(); await f.close(); }
}

async function stopDuringReviewWaitsForNativeStatAndDiscardsOwner() {
    const f = await fixture(300), a = await f.create("review-stop"), held = gate();
    try {
        await f.queue(a.engine, paths(300), 2);
        const stat = a.engine.io.stat.bind(a.engine.io); let intercepted = false;
        a.engine.io.stat = async (path: string) => {
            if (!intercepted) { intercepted = true; await held.wait(); }
            return stat(path);
        };
        const pending = observe(a.engine.pushPending()); await held.entered;
        const stopped = observe(a.engine.stopAndDrain()); await turns();
        assert.equal(stopped.state.settled, false); assert.equal(pending.state.settled, false);
        assert.equal(f.server.requests.length, 0);
        held.resolve(); await pending.done; await stopped.done;
        assert.equal(stopped.state.error, undefined); assert.equal(f.server.requests.length, 0);
        assert.equal(a.engine.pendingChanges.size, 300); assert.equal(f.journal.unsyncedCount(), 300);
        const b = await f.create("review-restart"), work = observeWork(b.engine);
        await b.engine.pushPending();
        assert.equal(work.materializations[0], 300, "replacement inherited the cancelled review owner");
        assert.equal(f.server.requests.length, 2); await assertFinished(f, b.engine);
    } finally { held.resolve(); await f.close(); }
}

async function partitionWaitHasNoPartialEffectsAndRevalidatesLateEdit() {
    const f = await fixture(600), a = await f.create("partition-edit"), held = gate();
    try {
        const old = await f.queue(a.engine, paths(600), 2), work = observeWork(a.engine);
        const diskCut = f.storage.events.length;
        const partition = a.engine.deferredChanges.partitionCooperatively.bind(a.engine.deferredChanges);
        let intercepted = false;
        a.engine.deferredChanges.partitionCooperatively = async (...args: any[]) => {
            const options = args[5];
            return partition(...args.slice(0, 5), {
                ...options,
                cooperate: async () => {
                    if (!intercepted) { intercepted = true; await held.wait(); }
                    await options.cooperate();
                },
            });
        };
        const pending = observe(a.engine.pushPending()); await held.entered;
        assert(intercepted, "fixture did not stop inside cooperative deferred partition");
        assert.deepEqual(work.materializations, [600]);
        assert.deepEqual(work.claims, []); assert.deepEqual(work.takes, []);
        assert.equal(f.server.requests.length, 0); assert.equal(a.engine.rootRuntime.pending(), null);
        assert.deepEqual(new Set(f.journal.unsynced().map(row => row.id)), new Set(old));
        assert.equal(f.base.getHash(pathAt(0)), hashFor(1));
        const beforeRelease = f.storage.events.slice(diskCut);
        assert(!beforeRelease.flatMap(event => rows(event, ROOT_INTENT_PATH)).some(row => row.op === "intent"));
        assert(!beforeRelease.flatMap(event => rows(event, SYNC_BASE_STORE_PATH)).some(row => row.op === "root-publication"));
        assert(!beforeRelease.flatMap(event => rows(event, JOURNAL_STORE_PATH)).some(row => row.op === "ack"));

        const target = pathAt(0), newer = await f.event(a.engine, target, 3);
        assert(newer > old[0]); await turns(); assert.equal(pending.state.settled, false);
        assert.deepEqual(work.claims, []); assert.equal(f.server.requests.length, 0);
        const waitingIds = new Set(f.journal.unsynced().map(row => row.id));
        assert(old.slice(1).every(id => waitingIds.has(id)) && waitingIds.has(newer),
            "partition wait lost an unsuperseded durable generation");
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        if (a.engine.pendingChanges.size) await a.engine.pushPending();
        assert(f.server.requests.every(request => requestHashAt(request, target) !== hashFor(2)),
            "partition wait published the superseded source generation");
        assert.equal(f.base.getHash(target), hashFor(3));
        assert(f.server.cuts.flat().some(cut => cut.path === target && cut.throughId === newer));
        await assertFinished(f, a.engine);
    } finally { held.resolve(); await f.close(); }
}

async function stopDuringPartitionWaitDiscardsOwnerBeforeClaim() {
    const f = await fixture(300), a = await f.create("partition-stop"), held = gate();
    try {
        const ids = await f.queue(a.engine, paths(300), 2), work = observeWork(a.engine);
        const partition = a.engine.deferredChanges.partitionCooperatively.bind(a.engine.deferredChanges);
        let intercepted = false;
        a.engine.deferredChanges.partitionCooperatively = async (...args: any[]) => {
            const options = args[5];
            return partition(...args.slice(0, 5), {
                ...options,
                cooperate: async () => {
                    if (!intercepted) { intercepted = true; await held.wait(); }
                    await options.cooperate();
                },
            });
        };
        const pending = observe(a.engine.pushPending()); await held.entered;
        const stopped = observe(a.engine.stopAndDrain()); await turns();
        assert(intercepted); assert.equal(pending.state.settled, false); assert.equal(stopped.state.settled, false);
        assert.deepEqual(work.claims, []); assert.deepEqual(work.takes, []); assert.equal(f.server.requests.length, 0);
        assert.equal(a.engine.rootRuntime.pending(), null);
        held.resolve(); await pending.done; await stopped.done;
        assert.equal(pending.state.error, undefined); assert.equal(stopped.state.error, undefined);
        assert.equal(f.server.requests.length, 0); assert.equal(a.engine.pendingChanges.size, 300);
        assert.equal(f.journal.unsyncedCount(), 300);
        assert.deepEqual(new Set(f.journal.unsynced().map(row => row.id)), new Set(ids));
        const b = await f.create("partition-restart"), replacement = observeWork(b.engine);
        await b.engine.pushPending();
        assert.equal(replacement.materializations[0], 300, "replacement reused the stopped partition owner");
        await assertFinished(f, b.engine);
    } finally { held.resolve(); await f.close(); }
}

async function dependencyCaptureWaitHasNoPartialEffectsAndRevalidatesLateEdit() {
    const f = await fixture(300), a = await f.create("dependency-capture-edit"), held = gate();
    try {
        for (let index = 0; index < 300; index++) {
            a.engine.deferredChanges.registerLegacyRename(
                `missing-dependency-a/${index}.md`, `missing-dependency-b/${index}.md`, index + 1,
            );
        }
        const old = await f.queue(a.engine, paths(300), 2), work = observeWork(a.engine);
        const diskCut = f.storage.events.length;
        const capture = a.engine.deferredChanges.captureDependenciesCooperatively.bind(a.engine.deferredChanges);
        let intercepted = false;
        a.engine.deferredChanges.captureDependenciesCooperatively = async (options: any) => capture({
            ...options,
            cooperate: async () => {
                if (!intercepted) { intercepted = true; await held.wait(); }
                await options.cooperate();
            },
        });
        const pending = observe(a.engine.pushPending()); await held.entered;
        assert(intercepted, "fixture did not stop inside cooperative dependency capture");
        assert.deepEqual(work.materializations, [300]);
        assert.deepEqual(work.claims, []); assert.deepEqual(work.takes, []);
        assert.equal(f.server.requests.length, 0); assert.equal(a.engine.rootRuntime.pending(), null);
        assert.deepEqual(new Set(f.journal.unsynced().map(row => row.id)), new Set(old));
        assert.equal(f.base.getHash(pathAt(0)), hashFor(1));
        const beforeRelease = f.storage.events.slice(diskCut);
        assert(!beforeRelease.flatMap(event => rows(event, ROOT_INTENT_PATH)).some(row => row.op === "intent"));
        assert(!beforeRelease.flatMap(event => rows(event, SYNC_BASE_STORE_PATH)).some(row => row.op === "root-publication"));
        assert(!beforeRelease.flatMap(event => rows(event, JOURNAL_STORE_PATH)).some(row => row.op === "ack"));

        const target = pathAt(0), newer = await f.event(a.engine, target, 3);
        assert(newer > old[0]); await turns(); assert.equal(pending.state.settled, false);
        assert.deepEqual(work.claims, []); assert.equal(f.server.requests.length, 0);
        const waitingIds = new Set(f.journal.unsynced().map(row => row.id));
        assert(old.slice(1).every(id => waitingIds.has(id)) && waitingIds.has(newer),
            "dependency capture wait lost an unsuperseded durable generation");
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        if (a.engine.pendingChanges.size) await a.engine.pushPending();
        assert(f.server.requests.every(request => requestHashAt(request, target) !== hashFor(2)),
            "dependency capture wait published the superseded source generation");
        assert.equal(f.base.getHash(target), hashFor(3));
        assert(f.server.cuts.flat().some(cut => cut.path === target && cut.throughId === newer));
        await assertFinished(f, a.engine);
    } finally { held.resolve(); await f.close(); }
}

async function prioritySortWaitHasNoPartialEffectsAndRevalidatesLateEdit() {
    for (const [priority, heldCheckpoint] of [["oldest", 1], ["random", 2]] as const) {
        const f = await fixture(300), a = await f.create(`priority-sort-edit-${priority}`), held = gate();
        try {
            a.engine.syncPriority = priority;
            const old = await f.queue(a.engine, paths(300), 2), work = observeWork(a.engine);
            const diskCut = f.storage.events.length;
            const sort = a.engine.sortReviewedChanges.bind(a.engine); let intercepted = false, checkpoints = 0;
            a.engine.sortReviewedChanges = async (changes: any, activePriority: any, options: any) =>
                sort(changes, activePriority, {
                    ...options,
                    cooperate: async () => {
                        if (!intercepted && ++checkpoints === heldCheckpoint) {
                            intercepted = true; await held.wait();
                        }
                        await options.cooperate();
                    },
                });
            const pending = observe(a.engine.pushPending()); await held.entered;
            assert(intercepted, `${priority} fixture did not stop inside cooperative priority sort`);
            assert.equal(checkpoints, heldCheckpoint);
            assert.deepEqual(work.materializations, [300]);
            assert.deepEqual(work.claims, []); assert.deepEqual(work.takes, []);
            assert.equal(f.server.requests.length, 0); assert.equal(a.engine.rootRuntime.pending(), null);
            assert.deepEqual(new Set(f.journal.unsynced().map(row => row.id)), new Set(old));
            assert.equal(f.base.getHash(pathAt(0)), hashFor(1));
            const beforeRelease = f.storage.events.slice(diskCut);
            assert(!beforeRelease.flatMap(event => rows(event, ROOT_INTENT_PATH)).some(row => row.op === "intent"));
            assert(!beforeRelease.flatMap(event => rows(event, SYNC_BASE_STORE_PATH)).some(row => row.op === "root-publication"));
            assert(!beforeRelease.flatMap(event => rows(event, JOURNAL_STORE_PATH)).some(row => row.op === "ack"));

            const target = pathAt(0), newer = await f.event(a.engine, target, 3);
            assert(newer > old[0]); await turns(); assert.equal(pending.state.settled, false);
            assert.deepEqual(work.claims, []); assert.equal(f.server.requests.length, 0);
            const waitingIds = new Set(f.journal.unsynced().map(row => row.id));
            assert(old.slice(1).every(id => waitingIds.has(id)) && waitingIds.has(newer),
                `${priority} sort wait lost an unsuperseded durable generation`);
            held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
            if (a.engine.pendingChanges.size) await a.engine.pushPending();
            assert(f.server.requests.every(request => requestHashAt(request, target) !== hashFor(2)),
                `${priority} sort wait published the superseded source generation`);
            assert.equal(f.base.getHash(target), hashFor(3));
            assert(f.server.cuts.flat().some(cut => cut.path === target && cut.throughId === newer));
            await assertFinished(f, a.engine);
        } finally { held.resolve(); await f.close(); }
    }
}

async function lostAcceptedReplyRequiresRecoveryBeforeANewReviewedCut() {
    const f = await fixture(600), a = await f.create("lost-reply"), held = gate();
    try {
        await f.queue(a.engine, paths(600), 2); const work = observeWork(a.engine);
        f.control.beforeRootAnswer = async () => { throw new Error("synthetic accepted reply lost"); };
        await a.engine.pushPending(); f.control.beforeRootAnswer = undefined;
        assert.equal(f.server.requests.length, 1); assert.equal(a.engine.rootRepairRequired, true);
        assert.equal(f.base.getHash(pathAt(0)), hashFor(1)); assert.equal(f.journal.unsyncedCount(), 600);
        assert(a.engine.rootRuntime.pending());
        f.control.beforeQueryAnswer = held.wait;
        const before = work.materializations.length, pending = observe(a.engine.pushPending());
        await held.entered;
        assert.equal(work.materializations.length, before, "new review crossed an unresolved accepted root");
        assert.equal(f.server.requests.length, 1);
        const newest = await f.event(a.engine, pathAt(599), 3);
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        assert.equal(f.server.queries, 1); assert.deepEqual(f.server.requests.map(request => request.sequence), [1, 2, 3, 4]);
        assert.equal(work.materializations[before], 344, "outcome recovery reused the old reviewed cut");
        assert.equal(f.base.getHash(pathAt(599)), hashFor(3));
        assert.equal(f.server.cuts[1]?.length, 1, "recovered recent generation was not isolated as urgent work");
        assert(f.server.cuts[1]?.some(cut => cut.path === pathAt(599) && cut.throughId === newest),
            "recovery did not publish the newest urgent generation first");
        const laterBulk = f.server.cuts.slice(2).flat();
        assert.equal(laterBulk.length, 343, "recovery lost the remaining bulk reviewed cut");
        assert(laterBulk.every(cut => cut.path !== pathAt(599)), "settled urgent generation leaked into later bulk roots");
        assert(f.server.cuts.flat().some(cut => cut.path === pathAt(599) && cut.throughId === newest));
        await assertFinished(f, a.engine);
    } finally { held.resolve(); f.control.beforeRootAnswer = undefined; f.control.beforeQueryAnswer = undefined; await f.close(); }
}

async function externalBaseMetadataMutationInvalidatesTheCapture() {
    const f = await fixture(600), a = await f.create("base-between"), held = gate();
    try {
        await f.queue(a.engine, paths(600), 2); const work = observeWork(a.engine), heavy = a.engine.waitForHeavyWork.bind(a.engine);
        let intercepted = false;
        a.engine.waitForHeavyWork = async (...args: any[]) => {
            await heavy(...args);
            if (args[0] === "root continuation" && !intercepted) { intercepted = true; await held.wait(); }
        };
        const pending = observe(a.engine.pushPending()); await held.entered;
        // Real public base mutation, but deliberately only a synthetic
        // metadata refresh, not a claimed remote download or tree repair.
        f.base.setEntry(pathAt(599), hashFor(1), 9, 1); await f.base.save();
        held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
        assert(work.materializations.slice(1).includes(344), "base mutation retained stale review authority");
        assert.equal(f.base.getEntry(pathAt(599))?.mtime, 2);
        assert.equal(f.base.getHash(pathAt(599)), hashFor(2)); await assertFinished(f, a.engine);
    } finally { held.resolve(); await f.close(); }
}

async function finalInitialDispatchChecksChangesAcrossEveryPrepareAwait() {
    const cases: Array<{ phase: "hash" | "prepare" | "negotiate"; change: "callback" | "rename" | "policy" | "tree-format" }> = [];
    for (const phase of ["hash", "prepare", "negotiate"] as const) for (const change of ["callback", "rename", "policy"] as const) {
        cases.push({ phase, change });
    }
    cases.push({ phase: "negotiate", change: "tree-format" });
    for (const { phase, change } of cases) {
        const f = await fixture(3), a = await f.create(`${phase}-${change}`), held = gate();
        const originalTreeVersion = a.tree.tree_version;
        try {
            await f.queue(a.engine, paths(3), 2);
            let intercepted = false;
            if (phase === "hash") {
                const hash = a.engine.rootRuntime.hash.bind(a.engine.rootRuntime);
                a.engine.rootRuntime.hash = async (bytes: Uint8Array) => {
                    const result = await hash(bytes);
                    if (!intercepted) { intercepted = true; await held.wait(); }
                    return result;
                };
            } else if (phase === "prepare") {
                f.control.boundary = async event => {
                    if (!intercepted && rows(event, ROOT_INTENT_PATH).some(row => row.op === "intent")) {
                        intercepted = true; await held.wait();
                    }
                };
            } else {
                const negotiate = a.engine.api.negotiateRootOutcomes.bind(a.engine.api); let negotiations = 0;
                a.engine.api.negotiateRootOutcomes = async (...args: any[]) => {
                    const result = await negotiate(...args);
                    if (++negotiations === 2) { intercepted = true; await held.wait(); }
                    return result;
                };
            }
            const pending = observe(a.engine.pushPending()); await held.entered;
            assert(intercepted); assert.equal(f.server.requests.length, 0);
            if (change === "callback") await f.event(a.engine, pathAt(0), 3);
            else if (change === "rename") await renameEvent(f, a.engine, pathAt(0), pathAt(2));
            else if (change === "policy") a.engine.syncPriority = "newest";
            else a.tree.tree_version = () => 2;
            const unacknowledged = structuredClone(f.journal.unsynced());
            held.resolve(); await pending.done; assert.equal(pending.state.error, undefined);
            assert.equal(f.server.requests.length, 0, `${phase}/${change}: stale newly prepared root crossed initial dispatch`);
            assert.equal(f.server.legacy, 0); assert.equal(a.engine.rootRepairRequired, true);
            assert.deepEqual(f.journal.unsynced(), unacknowledged, "refused initial dispatch ACKed local generations");
            for (const path of paths(3)) assert.equal(f.base.getHash(path), hashFor(1));
            const durable = a.engine.rootRuntime.pending(); assert(durable); assert.equal(durable.terminal, null);
            assert.equal(durable.intent.request.sequence, 1); assert.equal((await f.cold()).pending?.intent.request.request_hash,
                durable.intent.request.request_hash, "refused intent was not recoverable from its verified cut");
            // The synthetic tree/repair fixture supports v1 only. Restore it
            // after proving the changed format blocked initial dispatch;
            // do not claim an actual v1→v2 migration from this test.
            a.tree.tree_version = originalTreeVersion;

            // Synthetic server reports the original identity UNKNOWN, then
            // accepts an exact cancellation. Actual runtime/recovery/terminal
            // persistence is unchanged; cancellation must not ACK file work.
            const identity = { sequence: durable.intent.request.sequence, mutation_id: durable.intent.request.mutation_id,
                request_hash: durable.intent.request.request_hash };
            const query = a.engine.api.queryRootOutcome.bind(a.engine.api); let cancels = 0;
            a.engine.api.queryRootOutcome = async (vault: string, requested?: typeof identity) => {
                if (!requested) return query(vault, requested);
                assert.deepEqual(requested, identity); f.server.queries++;
                return { protocol_version: 1, server_incarnation: durable.intent.request.server_incarnation,
                    status: "unknown", last_sequence: 0, current_root_hash: f.server.root };
            };
            a.engine.api.cancelRootOutcome = async (_vault: string, request: any) => {
                assert.deepEqual(request, { protocol_version: 1, server_incarnation: durable.intent.request.server_incarnation, ...identity });
                cancels++; f.server.floor = identity.sequence;
                f.server.receipt = { protocol_version: 1, server_incarnation: request.server_incarnation,
                    status: "cancelled", ...identity, result: { cancelled: true } };
                return structuredClone(f.server.receipt);
            };
            await a.engine.recoverRootBeforeWork();
            assert.equal(cancels, 1); assert.equal(f.server.queries, 1); assert.equal(f.server.requests.length, 0);
            assert.equal(a.engine.rootRuntime.pending(), null); assert.deepEqual(f.journal.unsynced(), unacknowledged);
            for (const path of paths(3)) assert.equal(f.base.getHash(path), hashFor(1));
            assert.equal(a.engine.rootRepairRequired, false);
            await a.engine.pushPending();
            assert(f.server.requests.length > 0); assert.equal(f.server.requests[0].sequence, 2);
            assert.notEqual(f.server.requests[0].request_hash, identity.request_hash, "cancelled intent was blindly resent");
            if (change === "callback") assert.equal(f.base.getHash(pathAt(0)), hashFor(3));
            if (change === "rename") assert.equal(f.base.getHash(pathAt(0)), null);
            await assertFinished(f, a.engine);
        } finally { held.resolve(); a.tree.tree_version = originalTreeVersion; f.control.boundary = undefined; await f.close(); }
    }
}

async function manualRetrySurvivesLocalAckAndNeverPublishesAnUnretriedDelete() {
    for (const metadata of ["unchanged", "changed"] as const) {
    const f = await fixture(600), a = await f.create(`manual-cooling-${metadata}`, { ignorePatterns: ["ignored/"] });
    try {
        const omitted = "ignored/noop.md", source = pathAt(599), deleted = pathAt(0), largeSize = 1024 * 1024 * 1024;
        const hintSize = metadata === "changed" ? largeSize / 2 : largeSize;
        await f.queue(a.engine, [omitted], 2);
        await f.queue(a.engine, paths(600).slice(1, 599), 2);
        f.source.set(source, 2);
        const sourceId = await f.journal.append({ action: "modified", path: source, ts: 2, synced: false });
        a.engine.pendingChanges.add({ action: "modified", path: source, hash: hashFor(2), mtime: 2, size: hintSize }, sourceId);
        await deleteEvent(f, a.engine, deleted);
        const stat = a.engine.io.stat.bind(a.engine.io), read = a.engine.io.readFile.bind(a.engine.io);
        let largeReads = 0;
        // A stat-only change does not prove the formerly cooled source fits
        // now. The changed variant has no callback/new hint: old512MiB grows
        // to fresh1GiB while retaining the same durable owner/retry scope.
        a.engine.io.stat = (path: string) => path === source ? Promise.resolve({ mtime: 2, size: largeSize }) : stat(path);
        a.engine.io.readFile = (path: string) => { if (path === source) largeReads++; return read(path); };
        const cooled = [...a.engine.pendingChanges.iterate()].find((hint: DirtyFileChange) => hint.path === source)!;
        a.engine.deferredChanges.settle([cooled], [cooled], [{ path: source, reason: "source-too-large",
            requiredBytes: 128, capacityBytes: 64 }], a.engine.deferredRetryKey(), Date.now());
        const settle = a.engine.deferredChanges.settle.bind(a.engine.deferredChanges);
        const refused: Array<{ path: string; requiredBytes: number; capacityBytes: number }> = [];
        a.engine.deferredChanges.settle = (...args: any[]) => {
            refused.push(...args[2].filter((detail: any) => detail.path === source && detail.reason === "source-too-large"));
            return settle(...args);
        };
        await a.engine.pushPending(false, true);
        assert(f.server.requests.length >= 2, "independent prefixes did not progress during explicit retry");
        const firstRoot = f.events.findIndex(event => event.endsWith(":root-accepted"));
        assert(f.events.indexOf("journal-ack") >= 0 && f.events.indexOf("journal-ack") < firstRoot,
            "fixture failed to put a local-only ACK before the forced source opportunity");
        assert(f.server.cuts.flat().every(cut => cut.path !== source && cut.path !== deleted && cut.path !== omitted),
            "manual retry published deletion without the still-unadmitted source");
        assert(refused.some(detail => detail.requiredBytes > largeSize && detail.requiredBytes > detail.capacityBytes),
            "manual retry was consumed by local ACK/unrelated roots without an actual source-admission attempt");
        assert.equal(largeReads, 0, "oversized synthetic source was read before admission");
        assert.equal(f.base.getHash(source), hashFor(1)); assert.equal(f.base.getHash(deleted), hashFor(1));
        assert.equal(a.engine.pendingChanges.size, 2); assert.equal(f.journal.unsyncedCount(), 2);
        assert.deepEqual(new Set(f.journal.unsynced().map(row => row.path)), new Set([source, deleted]));
        assert.equal(f.server.legacy, 0); assert.equal(a.engine.rootRuntime.pending(), null);
        for (const path of paths(600).slice(1, 599)) assert.equal(f.base.getHash(path), hashFor(2));
    } finally { await f.close(); }
    }
}

const suites = {
    quiet: quietDrainHasOneFullReviewAndBoundedRevalidation,
    stat_edits: editsDuringReviewAndSelectedStatNeverPublishStaleGeneration,
    initial_review_prefix: approvedInitialReviewPreemptsIntoOneDurableRecentPrefix,
    initial_review_active_prefix: activeRecentPathBeyondThePrefixLimitStillRunsInThePrefix,
    failed_prefix_debt: failedRecentPrefixDebtSurvivesDrainAndHandoff,
    initial_review_policy: unsafeInitialReviewCannotUseTheRecentPrefixWithoutPolicyAuthority,
    upload_edit: editsDuringActualObjectUploadFenceTheRoot,
    fairness: urgentActiveNoteAndBulkBothMakeProgress,
    in_flight_urgent: inFlightUrgentCallbackDoesNotDispatchItsSupersededRow,
    streaming_urgency: streamingCallbacksKeepUrgencyAcrossReviewOwnerRebuild,
    slice_urgent: selectedRootPreemptsForUrgentSaveAtNextPublicationOpportunity,
    slice_cancel: cancelledRootSliceRestoresEveryGenerationForReplacement,
    urgent_drift: driftedUrgentSelectionKeepsItsReviewedPriority,
    drift_bound: repeatedDriftAndReviewRebuildStayDrainBounded,
    cooling: coolingSourceOutsideSelectedRootStillHoldsDeletes,
    missing: unexpectedTrackedMissingForcesWholeRemainderAudit,
    rename: newRenameDependencyCannotSplitAnOldPlannedCut,
    ack_stop: nativeAckAndStopRetainNewerCaptureForReplacement,
    pull: pullBetweenRootsDiscardsThePriorReview,
    selected_io: selectedStatFailureCannotBecomeAnOmissionAck,
    review_stop: stopDuringReviewWaitsForNativeStatAndDiscardsOwner,
    partition_edit: partitionWaitHasNoPartialEffectsAndRevalidatesLateEdit,
    partition_stop: stopDuringPartitionWaitDiscardsOwnerBeforeClaim,
    dependency_capture_edit: dependencyCaptureWaitHasNoPartialEffectsAndRevalidatesLateEdit,
    priority_sort_edit: prioritySortWaitHasNoPartialEffectsAndRevalidatesLateEdit,
    recovery: lostAcceptedReplyRequiresRecoveryBeforeANewReviewedCut,
    base: externalBaseMetadataMutationInvalidatesTheCapture,
    fresh_dispatch: finalInitialDispatchChecksChangesAcrossEveryPrepareAwait,
    forced_cooling: manualRetrySurvivesLocalAckAndNeverPublishesAnUnretriedDelete,
};
let completed = false;
process.once("beforeExit", () => { if (!completed && !process.exitCode) { console.error("engine-root-plan suite did not finish"); process.exitCode = 1; } });
async function run() {
    assert(process.execArgv.includes("--unhandled-rejections=strict"));
    const globals = globalThis as any, previousNotice = globals.__obsetyncTestNotice, previousDebounce = globals.__obsetyncTestDebounce;
    const progressNotices: string[] = [];
    const isProgressNotice = (message: unknown) =>
        /^(?:↑|Obsetync ↓|Scanning vault|Obsetync: scanning|Obsetync: recovering|Obsetync: journal recovery|Obsetync: checking|Reconcile:|reconcile:)/.test(String(message));
    const notice = (message: unknown) => {
        if (isProgressNotice(message)) progressNotices.push(String(message));
        return {
            setMessage(next: unknown) {
                if (isProgressNotice(next)) progressNotices.push(String(next));
            },
            hide() {},
        };
    };
    const debounce = () => () => {};
    globals.__obsetyncTestNotice = notice; globals.__obsetyncTestDebounce = debounce;
    const filter = process.env.OBSETYNC_ENGINE_PLAN_CASE;
    if (filter !== undefined) assert(Object.prototype.hasOwnProperty.call(suites, filter), "unknown engine plan test case");
    const selected = Object.entries(suites).filter(([name]) => !filter || name === filter);
    const timeout = setTimeout(() => { throw new Error("engine-root-plan suite exceeded its bounded fixture watchdog"); }, 45_000);
    try {
        for (const [name, test] of selected) { await test(); console.log(`engine-root-plan.test: ${name} passed`); }
        assert(progressNotices.length === 0, `normal progress opened or updated ${progressNotices.length} Notice instances`);
        console.log(`engine-root-plan.test: ${selected.length} actual engine reviewed-queue suites passed (synthetic tree/transport)`);
        completed = true;
    } finally {
        clearTimeout(timeout); disposeWorkScheduler();
        if (globals.__obsetyncTestNotice === notice) {
            if (previousNotice === undefined) delete globals.__obsetyncTestNotice; else globals.__obsetyncTestNotice = previousNotice;
        }
        if (globals.__obsetyncTestDebounce === debounce) {
            if (previousDebounce === undefined) delete globals.__obsetyncTestDebounce; else globals.__obsetyncTestDebounce = previousDebounce;
        }
    }
}
void run().catch(error => { console.error(error); process.exitCode = 1; });
