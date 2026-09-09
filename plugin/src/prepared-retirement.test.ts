import { strict as assert } from "node:assert";
import { retirablePreparedHints } from "./prepared-retirement";

const candidates = ["ordinary", "renamed", "scan", "scan-retained", "older-ack"]
    .map((path, index) => ({ scopeHash: "a".repeat(64), path, expectedMutationId: index + 1 }));
const journalCuts = new Map([["ordinary", 10], ["renamed", 11], ["older-ack", 12]]);
const settlement = { acknowledged: [{ path: "ordinary", throughId: 10 },
    { path: "renamed", throughId: 11 }, { path: "older-ack", throughId: 11 }, { path: "unrelated", throughId: 99 }],
    retained: [{ path: "renamed" }, { path: "scan-retained" }] };
const result = retirablePreparedHints(candidates, journalCuts, settlement);
assert.deepEqual(result.map(value => value.path).sort(), ["ordinary", "scan"]);
result[0].expectedMutationId = 999;
assert.deepEqual(candidates.map(value => value.expectedMutationId), [1, 2, 3, 4, 5]);
assert.deepEqual(retirablePreparedHints([candidates[1]], journalCuts, { acknowledged: [], retained: [] }), [],
    "empty journal ACK retired an unacknowledged destination preparation");
assert.deepEqual(retirablePreparedHints([candidates[1]], journalCuts,
    { acknowledged: [{ path: "renamed", throughId: 12 }], retained: [] }), [candidates[1]]);
assert.deepEqual(retirablePreparedHints([], journalCuts, settlement), []);
console.log("prepared-retirement.test: covered generations, retained rename peers and detached CAS tokens passed");
