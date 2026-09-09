import { strict as assert } from "node:assert";
import { runtimeForHost } from "./host-runtime";

for (const isMobile of [false, true]) {
    const desktop = { isDesktopApp: true, isMobileApp: false, isMobile };
    const mobile = { isDesktopApp: false, isMobileApp: true, isMobile };
    assert.equal(runtimeForHost(desktop), "desktop", "mobile UI disabled desktop capabilities");
    assert.equal(runtimeForHost(mobile), "mobile", "desktop UI granted mobile host Node capabilities");
}
assert.equal(runtimeForHost({}), "mobile");
assert.equal(runtimeForHost({ isDesktopApp: true, isMobileApp: true }), "mobile");
console.log("host-runtime.test: app host, UI independence and conservative unknown policy passed");
