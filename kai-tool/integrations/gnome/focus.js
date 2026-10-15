// Snapshot only IDs and focus. Shared-PID windows stay separate so clients can
// reject ambiguous terminals; a locked desktop never acknowledges a completion.
export function windowSnapshot(windows, focused, locked) {
    return {id: 0, nodes: windows.map(window => ({
        id: window.get_stable_sequence(), pid: window.get_pid(),
        focused: !locked && window === focused,
    }))};
}

export async function focusWindow({pid, startTicks, locked, windows, readStat, activateAndWait}) {
    const sameProcess = () => {
        const stat = readStat(pid);
        const fields = stat.slice(stat.lastIndexOf(')') + 1).trim().split(/\s+/);
        return Number(stat.slice(0, stat.indexOf('(')).trim()) === pid &&
            fields[19] === startTicks && !['Z', 'X'].includes(fields[0]);
    };
    try {
        if (locked())
            return 'locked';
        if (!Number.isInteger(pid) || pid <= 0 || !/^\d+$/.test(startTicks) || !sameProcess())
            return 'changed';
        const matches = windows.filter(window => window.get_pid() === pid);
        if (matches.length > 1)
            return 'ambiguous';
        if (!matches.length)
            return 'missing';
        const result = await activateAndWait(matches[0]);
        if (locked())
            return 'locked';
        return result === 'focused' && !sameProcess() ? 'changed' : result;
    } catch {
        return 'changed';
    }
}

// Keep the Shell main loop free while activation moves between workspaces/windows.
// Signal/timer adapters return cleanup functions, making every completion path
// (including extension disable) release the same resources.
export class FocusConfirmation {
    constructor(platform) {
        this._platform = platform;
        this._pending = new Set();
        this._enabled = true;
    }

    confirm(target) {
        if (!this._enabled)
            return Promise.resolve('changed');
        const platform = this._platform;
        return new Promise(resolve => {
            let done = false;
            const cleanup = [];
            const finish = result => {
                if (done)
                    return;
                done = true;
                for (const dispose of cleanup) {
                    try { dispose(); } catch { /* Target may already be destroyed. */ }
                }
                this._pending.delete(finish);
                resolve(result);
            };
            const check = () => {
                try {
                    if (platform.locked())
                        finish('locked');
                    else if (platform.focused() === target)
                        finish('focused');
                } catch {
                    finish('changed');
                }
            };
            this._pending.add(finish);
            try {
                cleanup.push(platform.watchFocus(check));
                cleanup.push(platform.watchClosed(target, () => finish('changed')));
                cleanup.push(platform.watchLock(check));
                cleanup.push(platform.schedule(() => finish('unfocused'), 1000));
                if (platform.locked()) {
                    finish('locked');
                } else {
                    platform.activate(target);
                    check();
                }
            } catch {
                finish('changed');
            }
        });
    }

    disable() {
        this._enabled = false;
        for (const finish of [...this._pending])
            finish('changed');
    }
}
