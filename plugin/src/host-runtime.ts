/** UI layout is not a host capability: Electron may display the mobile UI.
 * Unknown/contradictory hosts receive conservative mobile policy, not Node
 * workers or a desktop whole-file allowance. Actual IO still checks capability. */
export function runtimeForHost(platform: {
    isDesktopApp?: boolean;
    isMobileApp?: boolean;
}): "desktop" | "mobile" {
    return platform.isDesktopApp === true && platform.isMobileApp !== true ? "desktop" : "mobile";
}
