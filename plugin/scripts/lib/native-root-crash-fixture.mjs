// Bundled only by the opt-in crash/recovery gate. The host shell is Node, but
// storage, transport, engine, root intent, tree WASM and Rust server are real.
import { strict as assert } from "node:assert";
import { webcrypto } from "node:crypto";
import { ObsetyncApi } from "../../src/api";
import { ObsetyncSyncEngine } from "../../src/sync";
import { ObsetyncDesktopIO } from "../../src/platform";
import { ObsetyncSyncBase } from "../../src/sync-base";
import { ObsetyncJournal } from "../../src/journal";
import { RootIntentStore } from "../../src/root-intent";
import { captureRootStreamScope } from "../../src/root-stream-scope";
import { SerialSettingsWriter } from "../../src/settings-persistence";
import { disposeWorkScheduler } from "../../src/work-scheduler";
import {
    createNativeRootFixtureHost,
    loadNativeRootFixtureWasm,
    configureNativeRootFixtureTuning,
    seedNativeRootFixture,
    nativeRootFixtureCorpus,
    nativeRootFixturePathAt,
    NATIVE_ROOT_FIXTURE_VAULT,
} from "./native-root-fixture.mjs";

const STATE_DIR = ".obsidian/plugins/obsetync";
const STATE_PATH = `${STATE_DIR}/native-crash-transport.json`;
const WITNESS_PATH = `${STATE_DIR}/native-crash-witness.json`;
const COUNT = 32;
const CHANGE_INDEX = 7;

function settings(api, enrollment, serverUrl) {
    return { serverUrl, serverBoxPub: enrollment.server_box_pub,
        vaultId: NATIVE_ROOT_FIXTURE_VAULT, deviceId: enrollment.device_id,
        enrolled: true, bearerToken: enrollment.bearer_token };
}

async function transportOwner(f, enrollment, serverUrl, initialize) {
    await f.adapter.mkdir(STATE_DIR);
    let transport = initialize ? {
        wireVersion: enrollment.wire_version,
        esPub: enrollment.Es_pub_initial,
        esPubValidUntil: enrollment.Es_pub_valid_until,
        lastOutgoingSeq: 0,
    } : JSON.parse(await f.adapter.read(STATE_PATH));
    assert.equal(transport.wireVersion, "0x02");
    assert(Number.isSafeInteger(transport.lastOutgoingSeq) && transport.lastOutgoingSeq >= 0);
    const writer = new SerialSettingsWriter(async value => {
        const raw = JSON.stringify(value);
        await f.adapter.write(`${STATE_PATH}.next`, raw);
        await f.adapter.rename(`${STATE_PATH}.next`, STATE_PATH);
        assert.equal(await f.adapter.read(STATE_PATH), raw);
    });
    if (initialize) await writer.save(transport);
    const api = new ObsetyncApi(serverUrl, enrollment.server_box_pub, enrollment.bearer_token, {
        get: () => ({ ...transport }),
        update: patch => { transport = { ...transport, ...patch }; return writer.save(transport); },
    }, "desktop");
    const rootSettings = settings(api, enrollment, serverUrl);
    const captured = captureRootStreamScope(rootSettings, api, api.baseUrl);
    return { api, writer, rootSettings, captured, transport: () => ({ ...transport }) };
}

function engineFor(f, wasm, api, base, journal, tree, root, enrollment, captured) {
    return new ObsetyncSyncEngine(f.app, api, new ObsetyncDesktopIO(f.app), base, journal, wasm, tree,
        NATIVE_ROOT_FIXTURE_VAULT, 3_600_000, "sequential", () => {}, root, false,
        "native-crash-recovery", false, false, [], undefined, false, null, undefined, undefined, {
            vaultId: NATIVE_ROOT_FIXTURE_VAULT,
            deviceId: enrollment.device_id,
            scopeHash: captured.scopeHash,
            assertApiScope: () => captured.assertCurrent(settings(api, enrollment, api.baseUrl), api, api.baseUrl),
        });
}

function exactPending(pending, expected) {
    assert(pending, "durable root intent was not prepared before root dispatch");
    assert.equal(pending.terminal, null);
    assert.equal(pending.intent.vaultId, NATIVE_ROOT_FIXTURE_VAULT);
    assert.equal(pending.intent.deviceId, expected.deviceId);
    assert.equal(pending.intent.request.protocol_version, 1);
    assert.equal(pending.intent.request.server_incarnation, expected.serverIncarnation);
    assert.equal(pending.intent.request.sequence, 1);
    assert.equal(pending.intent.request.parent_root, expected.baselineRoot);
    assert.equal(pending.intent.publication.candidateRoot, expected.candidateRoot);
    assert.equal(pending.intent.publication.entries.length, 1);
    assert.equal(pending.intent.publication.entries[0].action, "upsert");
    assert.equal(pending.intent.publication.entries[0].path, expected.path);
    assert.equal(pending.intent.publication.entries[0].hash, expected.hash);
    assert.equal(pending.intent.journalCuts.length, 1);
    assert.equal(pending.intent.journalCuts[0].path, expected.path);
    assert(Number.isSafeInteger(pending.intent.journalCuts[0].throughId));
    return pending;
}

/** Die at a deterministic application-root boundary. The first case stops
 * before dispatch. The second waits for the real server's accepted response,
 * then dies before that response is returned to the engine or persisted in
 * the local intent store. */
async function crashAtRootBoundary(options, phase, afterAcceptedResponse) {
    globalThis.window = globalThis; globalThis.crypto ??= webcrypto;
    globalThis.WebSocket = undefined; configureNativeRootFixtureTuning();
    const f = await createNativeRootFixtureHost(options.directory);
    const { wasm } = await loadNativeRootFixtureWasm(options.pluginDirectory);
    const owner = await transportOwner(f, options.enrollment, options.serverUrl, true);
    const { api, writer, captured } = owner;
    let engine, tree, base, journal;
    try {
        assert.equal((await api.ping()).ok, true);
        assert.equal((await api.negotiateTreeVersion(NATIVE_ROOT_FIXTURE_VAULT)).currentVersion, 1);
        const capabilities = await api.negotiateRootOutcomes(true); assert(capabilities);
        base = new ObsetyncSyncBase(f.app); journal = new ObsetyncJournal(f.app);
        await base.load(); await journal.load();
        tree = new wasm.WasmTree(NATIVE_ROOT_FIXTURE_VAULT, options.enrollment.device_id);
        tree.set_tree_version(1);
        phase("seed-v1");
        const baselineRoot = await seedNativeRootFixture(f, wasm, api, base, tree, COUNT);
        engine = engineFor(f, wasm, api, base, journal, tree, baselineRoot, options.enrollment, captured);
        phase("engine-start"); await engine.start();
        assert.equal(engine.getLastError(), null); assert.equal(engine.getPendingChangeCount(), 0);
        assert.equal(journal.unsyncedCount(), 0); assert.equal(engine.hasPendingRootWork(), false);

        const path = nativeRootFixturePathAt(CHANGE_INDEX);
        const bytes = nativeRootFixtureCorpus(CHANGE_INDEX, 2);
        const hash = wasm.wasm_hash(bytes), file = await f.write(path, bytes);
        await f.callback("modify", file);
        assert.equal(journal.unsyncedCount(), 1); assert.equal(engine.getPendingChangeCount(), 1);

        let boundaryCalls = 0;
        const originalCommit = api.commitRootOutcome.bind(api);
        api.commitRootOutcome = async (...args) => {
            boundaryCalls++;
            assert.equal(boundaryCalls, 1, "root dispatch boundary entered more than once");
            const candidateRoot = tree.candidate_root_hash_hex();
            assert(/^[0-9a-f]{64}$/.test(candidateRoot)); assert.notEqual(candidateRoot, baselineRoot);
            const pending = exactPending(engine.rootRuntime.pending(), {
                deviceId: options.enrollment.device_id,
                serverIncarnation: capabilities.serverIncarnation,
                baselineRoot, candidateRoot, path, hash,
            });
            assert.equal(await captured.scopeHash, pending.intent.publication.identity.scopeHash);
            assert.equal(wasm.wasm_root_hash_from_bytes(new Uint8Array(Buffer.from(pending.intent.request.root, "base64"))), candidateRoot);

            // Explicitly prove that every object needed by this candidate was
            // ACKed by the real server before the root method was intercepted.
            assert.deepEqual(await api.checkContent([hash]), []);
            const chunks = wasm.wasm_tree_candidate_chunk_hashes(tree);
            assert(chunks.length > 0); assert.deepEqual(await api.checkChunks(chunks), []);
            let boundary = "objects-acked-before-root";
            if (afterAcceptedResponse) {
                const accepted = await originalCommit(...args);
                assert.equal(accepted.status, "accepted");
                assert.equal(accepted.sequence, pending.intent.request.sequence);
                assert.equal(accepted.mutation_id, pending.intent.request.mutation_id);
                assert.equal(accepted.request_hash, pending.intent.request.request_hash);
                assert.equal(accepted.server_incarnation, pending.intent.request.server_incarnation);
                assert.deepEqual(accepted.result, { accepted: true, root_hash: candidateRoot });
                const exact = await api.queryRootOutcome(NATIVE_ROOT_FIXTURE_VAULT, {
                    sequence: pending.intent.request.sequence,
                    mutation_id: pending.intent.request.mutation_id,
                    request_hash: pending.intent.request.request_hash,
                });
                assert.deepEqual(exact, accepted);
                exactPending(engine.rootRuntime.pending(), {
                    deviceId: options.enrollment.device_id,
                    serverIncarnation: capabilities.serverIncarnation,
                    baselineRoot, candidateRoot, path, hash,
                });
                boundary = "root-accepted-before-local-receipt";
            }
            const remote = await api.getRoot(NATIVE_ROOT_FIXTURE_VAULT); assert(remote);
            assert.equal(wasm.wasm_root_hash_from_bytes(remote), afterAcceptedResponse ? candidateRoot : baselineRoot);
            const stream = await api.queryRootOutcome(NATIVE_ROOT_FIXTURE_VAULT);
            assert.equal(stream.status, "stream"); assert.equal(stream.last_sequence, afterAcceptedResponse ? 1 : 0);

            const witness = { schema: 1, boundary, count: COUNT,
                changeIndex: CHANGE_INDEX, path, hash, baselineRoot, candidateRoot,
                candidateChunks: chunks.length, scopeHash: await captured.scopeHash,
                deviceId: options.enrollment.device_id,
                serverIncarnation: capabilities.serverIncarnation, sequence: 1,
                transportReserved: owner.transport().lastOutgoingSeq };
            await f.adapter.write(WITNESS_PATH, JSON.stringify(witness));
            assert.equal(JSON.parse(await f.adapter.read(WITNESS_PATH)).candidateRoot, candidateRoot);
            phase(boundary);
            process.kill(process.pid, "SIGKILL");
            await new Promise(() => {});
        };
        phase("push-to-crash");
        await engine.pushPending();
        throw new Error("root dispatch unexpectedly returned after crash boundary");
    } finally {
        // SIGKILL deliberately skips this. It exists only for an assertion
        // failure before the boundary, so the child still joins its owners.
        await engine?.stopAndDrain();
        await Promise.all([base?.closeAndDrain(), journal?.closeAndDrain()]);
        api.closeDataLane();
        await writer.closeAndDrain();
        tree?.free(); disposeWorkScheduler();
    }
}

export function crashAfterObjectsBeforeRoot(options, phase) {
    return crashAtRootBoundary(options, phase, false);
}

export function crashAfterRootAcceptedBeforeLocalReceipt(options, phase) {
    return crashAtRootBoundary(options, phase, true);
}

/** Recover the killed client's exact private store in a new process. */
async function recoverRootBoundary(options, phase, expectedBoundary) {
    globalThis.window = globalThis; globalThis.crypto ??= webcrypto;
    globalThis.WebSocket = undefined; configureNativeRootFixtureTuning();
    const f = await createNativeRootFixtureHost(options.directory);
    const { wasm } = await loadNativeRootFixtureWasm(options.pluginDirectory);
    const witness = JSON.parse(await f.adapter.read(WITNESS_PATH));
    assert.deepEqual({ schema: witness.schema, boundary: witness.boundary, count: witness.count,
        changeIndex: witness.changeIndex, sequence: witness.sequence }, {
        schema: 1, boundary: expectedBoundary, count: COUNT,
        changeIndex: CHANGE_INDEX, sequence: 1,
    });
    assert.equal(witness.path, nativeRootFixturePathAt(CHANGE_INDEX));
    for (const value of [witness.hash, witness.baselineRoot, witness.candidateRoot, witness.scopeHash]) {
        assert.equal(typeof value, "string"); assert(/^[0-9a-f]{64}$/.test(value));
    }
    assert.notEqual(witness.baselineRoot, witness.candidateRoot);
    assert(Number.isSafeInteger(witness.candidateChunks) && witness.candidateChunks > 0);
    assert(Number.isSafeInteger(witness.transportReserved) && witness.transportReserved > 0);
    assert.equal(witness.deviceId, options.enrollment.device_id);
    for (let index = 0; index < COUNT; index++) await f.hydrate(nativeRootFixturePathAt(index));
    const owner = await transportOwner(f, options.enrollment, options.serverUrl, false);
    const { api, writer, captured } = owner;
    let engine, tree, preflightIntents, base, journal;
    try {
        assert(owner.transport().lastOutgoingSeq >= witness.transportReserved);
        const scopeHash = await captured.scopeHash; assert.equal(scopeHash, witness.scopeHash);
        base = new ObsetyncSyncBase(f.app); journal = new ObsetyncJournal(f.app);
        preflightIntents = new RootIntentStore(f.adapter, scopeHash, bytes => wasm.wasm_hash(bytes));
        phase("cold-preflight");
        await base.load(); await journal.load(); await preflightIntents.load();
        assert.equal(base.entryCount(), COUNT); assert.equal(base.treeBaseRoot, witness.baselineRoot);
        assert.equal(base.getHash(witness.path), wasm.wasm_hash(nativeRootFixtureCorpus(CHANGE_INDEX, 1)));
        assert.equal(journal.unsyncedCount(), 1);
        const rawPending = preflightIntents.pending();
        const pending = exactPending(rawPending, {
            deviceId: options.enrollment.device_id,
            serverIncarnation: witness.serverIncarnation,
            baselineRoot: witness.baselineRoot, candidateRoot: witness.candidateRoot,
            path: witness.path, hash: witness.hash,
        });
        assert.equal(pending.intent.publication.identity.scopeHash, scopeHash);
        assert.equal(wasm.wasm_root_hash_from_bytes(new Uint8Array(Buffer.from(pending.intent.request.root, "base64"))), witness.candidateRoot);
        assert.equal(preflightIntents.lastSequence, 1);
        const expected = { sequence: pending.intent.request.sequence,
            mutation_id: pending.intent.request.mutation_id, request_hash: pending.intent.request.request_hash };
        const observed = await api.queryRootOutcome(NATIVE_ROOT_FIXTURE_VAULT, expected);
        if (expectedBoundary === "objects-acked-before-root") {
            assert.equal(observed.status, "unknown"); assert.equal(observed.last_sequence, 0);
        } else {
            assert.equal(observed.status, "accepted");
            assert.equal(observed.sequence, expected.sequence);
            assert.equal(observed.mutation_id, expected.mutation_id);
            assert.equal(observed.request_hash, expected.request_hash);
            assert.equal(observed.server_incarnation, witness.serverIncarnation);
            assert.deepEqual(observed.result, { accepted: true, root_hash: witness.candidateRoot });
        }
        assert.deepEqual(await api.checkContent([witness.hash]), []);
        const remoteBefore = await api.getRoot(NATIVE_ROOT_FIXTURE_VAULT); assert(remoteBefore);
        assert.equal(wasm.wasm_root_hash_from_bytes(remoteBefore),
            expectedBoundary === "objects-acked-before-root" ? witness.baselineRoot : witness.candidateRoot);
        await preflightIntents.closeAndDrain(); preflightIntents = undefined;

        tree = new wasm.WasmTree(NATIVE_ROOT_FIXTURE_VAULT, options.enrollment.device_id);
        tree.set_tree_version(1);
        engine = engineFor(f, wasm, api, base, journal, tree, witness.baselineRoot, options.enrollment, captured);
        let sourceReads = 0, uploadedRecords = 0;
        const cancellations = [], commits = [];
        const originalRead = engine.io.readFile.bind(engine.io);
        engine.io.readFile = async (...args) => { sourceReads++; return originalRead(...args); };
        const originalVerifiedRead = engine.io.readFileIdentityVerified?.bind(engine.io);
        if (originalVerifiedRead) engine.io.readFileIdentityVerified = async (...args) => {
            const bytes = await originalVerifiedRead(...args);
            if (bytes !== null) sourceReads++;
            return bytes;
        };
        const originalPutObjects = api.putObjects.bind(api);
        api.putObjects = async (records, ...args) => {
            uploadedRecords += records.length; return originalPutObjects(records, ...args);
        };
        const originalCancel = api.cancelRootOutcome.bind(api);
        api.cancelRootOutcome = async (_vault, request, ...args) => {
            cancellations.push({ sequence: request.sequence, mutation: request.mutation_id,
                requestHash: request.request_hash, serverIncarnation: request.server_incarnation });
            return originalCancel(_vault, request, ...args);
        };
        const originalCommit = api.commitRootOutcome.bind(api);
        api.commitRootOutcome = async (_vault, request, ...args) => {
            commits.push({ sequence: request.sequence, parent: request.parent_root, root: request.root });
            return originalCommit(_vault, request, ...args);
        };
        // Recovery setup may publish segmented base/journal state and admit
        // low-priority reclaim. Join its exact IO before measuring native work.
        await Promise.all([base.runMaintenance(), journal.runMaintenance()]);
        f.native.resetCounters();
        phase("cold-engine-recovery"); await engine.start();
        if (engine.getPendingChangeCount()) await engine.pushPending();

        assert.equal(engine.getLastError(), null); assert.equal(engine.isBulkChangeReviewRequired(), false);
        assert.equal(engine.getPendingChangeCount(), 0); assert.equal(engine.hasPendingRootWork(), false);
        assert.equal(journal.unsyncedCount(), 0); assert.equal(engine.rootRuntime.pending(), null);
        const acceptedBeforeCrash = expectedBoundary === "root-accepted-before-local-receipt";
        assert.equal(engine.rootRuntime.lastSequence, acceptedBeforeCrash ? 1 : 2);
        assert.deepEqual(cancellations.map(row => row.sequence), acceptedBeforeCrash ? [] : [1]);
        if (!acceptedBeforeCrash) {
            assert.equal(cancellations[0].mutation, expected.mutation_id);
            assert.equal(cancellations[0].requestHash, expected.request_hash);
            assert.equal(cancellations[0].serverIncarnation, observed.server_incarnation);
        }
        assert.deepEqual(commits.map(row => row.sequence), acceptedBeforeCrash ? [] : [2]);
        if (!acceptedBeforeCrash) {
            assert.equal(commits[0].parent, witness.baselineRoot);
            assert.equal(wasm.wasm_root_hash_from_bytes(new Uint8Array(Buffer.from(commits[0].root, "base64"))), witness.candidateRoot);
        }
        assert.equal(uploadedRecords, 0, "recovery re-uploaded already ACKed objects");
        assert.equal(sourceReads, acceptedBeforeCrash ? 0 : 1,
            "cold recovery performed unexpected changed-file content reads");
        assert.equal(base.entryCount(), COUNT); assert.equal(base.getHash(witness.path), witness.hash);
        assert.equal(base.treeBaseRoot, witness.candidateRoot);
        assert.equal(tree.root_hash_hex(), witness.candidateRoot); assert.equal(tree.total_files(), COUNT);
        const remoteAfter = await api.getRoot(NATIVE_ROOT_FIXTURE_VAULT); assert(remoteAfter);
        assert.equal(wasm.wasm_root_hash_from_bytes(remoteAfter), witness.candidateRoot);
        const stream = await api.queryRootOutcome(NATIVE_ROOT_FIXTURE_VAULT);
        assert.equal(stream.status, "stream"); assert.equal(stream.last_sequence, acceptedBeforeCrash ? 1 : 2);
        const io = f.native.snapshot(); assert.equal(io.total.inFlight, 0);
        return { scenario: witness.boundary, freshProcess: true, files: COUNT,
            recoveredSequence: acceptedBeforeCrash ? 1 : 2,
            cancellationSequence: acceptedBeforeCrash ? null : 1,
            commitSequence: acceptedBeforeCrash ? null : 2,
            sourceReads, uploadedRecords, exactRoot: true, pendingJournal: 0,
            pendingRootIntent: false, ioInFlight: io.total.inFlight };
    } finally {
        await preflightIntents?.closeAndDrain();
        await engine?.stopAndDrain();
        await Promise.all([base?.closeAndDrain(), journal?.closeAndDrain()]);
        api.closeDataLane();
        await writer.closeAndDrain();
        tree?.free(); disposeWorkScheduler();
    }
}

export function recoverObjectsBeforeRoot(options, phase) {
    return recoverRootBoundary(options, phase, "objects-acked-before-root");
}

export function recoverRootAcceptedBeforeLocalReceipt(options, phase) {
    return recoverRootBoundary(options, phase, "root-accepted-before-local-receipt");
}
