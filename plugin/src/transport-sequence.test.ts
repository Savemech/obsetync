import { strict as assert } from "node:assert";
import { DurableSequenceAllocator } from "./transport-sequence";

class MemoryPersistence {
    value = 0;
    writes: number[] = [];

    readReservedThrough(): number {
        return this.value;
    }

    async persistReservedThrough(value: number): Promise<void> {
        this.value = value;
        this.writes.push(value);
    }
}

async function run(): Promise<void> {
    {
        const state = new MemoryPersistence();
        const allocator = new DurableSequenceAllocator(state);
        const values = await Promise.all(
            Array.from({ length: 5000 }, () => allocator.next()),
        );
        assert.equal(new Set(values).size, 5000);
        assert.equal(Math.min(...values), 1);
        assert.equal(Math.max(...values), 5000);
        assert.deepEqual(state.writes, [4096, 8192]);
    }

    {
        const state = new MemoryPersistence();
        const firstProcess = new DurableSequenceAllocator(state);
        assert.equal(await firstProcess.next(), 1);
        assert.equal(state.value, 4096);

        // A restart skips the unused tail of the durably reserved block.
        const restarted = new DurableSequenceAllocator(state);
        assert.equal(await restarted.next(), 4097);
        assert.equal(state.value, 8192);
    }

    {
        const state = new MemoryPersistence();
        const allocator = new DurableSequenceAllocator(state);
        await allocator.recover(12_288);
        assert.equal(await allocator.next(), 12_289);
        assert.deepEqual(state.writes, [16_384]);
    }

    console.log("transport-sequence.test: 10 assertions passed");
}

void run();
