import { strict as assert } from "node:assert";
import {
    OperationCheckpoint,
    OPERATION_CHECKPOINT_PATHS,
} from "./operation-checkpoint";

class MemoryIO {
    files = new Map<string, Uint8Array>();

    async readFile(path: string): Promise<Uint8Array> {
        const value = this.files.get(path);
        if (!value) throw new Error("missing");
        return value.slice();
    }

    async writeFile(path: string, data: Uint8Array): Promise<void> {
        this.files.set(path, data.slice());
    }

    async deleteFile(path: string): Promise<void> {
        this.files.delete(path);
    }
}

async function run(): Promise<void> {
    let clock = 1000;
    const io = new MemoryIO();
    const first = new OperationCheckpoint(io, "1.10.1", () => clock);
    await first.initialize();
    const id = await first.begin("pull", "0/20 files");
    assert.ok(io.files.has(OPERATION_CHECKPOINT_PATHS.active));

    clock += 4000;
    first.progress(id, "10/20 files\n12 MB");
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    const active = JSON.parse(
        new TextDecoder().decode(io.files.get(OPERATION_CHECKPOINT_PATHS.active)),
    );
    assert.equal(active.detail, "10/20 files 12 MB");

    // Simulate a renderer kill: no complete() call, then a fresh instance.
    const restarted = new OperationCheckpoint(io, "1.10.1", () => 9000);
    await restarted.initialize();
    assert.equal(restarted.getLastInterruption()?.phase, "pull");
    assert.ok(io.files.has(OPERATION_CHECKPOINT_PATHS.lastInterruption));
    assert.ok(!io.files.has(OPERATION_CHECKPOINT_PATHS.active));

    const nextId = await restarted.begin("push", "3 paths");
    await restarted.complete(nextId);
    assert.ok(!io.files.has(OPERATION_CHECKPOINT_PATHS.active));

    console.log("operation-checkpoint.test: 6 assertions passed");
}

void run();
