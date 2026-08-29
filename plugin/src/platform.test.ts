import { exactArrayBuffer } from "./binary";

let assertions = 0;
const check = (condition: unknown, message: string) => {
    assertions++;
    if (!condition) throw new Error(message);
};

const backing = new Uint8Array([9, 1, 2, 3, 8]);
const exact = new Uint8Array(exactArrayBuffer(backing.subarray(1, 4)));
check(exact.length === 3, "binary writer leaked bytes outside a Uint8Array view");
check(exact.join(",") === "1,2,3", "binary writer changed the selected bytes");

console.log(`platform.test: ${assertions} assertions passed`);
