import assert from 'node:assert/strict';
import test from 'node:test';
import {FocusConfirmation, focusWindow, windowSnapshot} from './focus.js';

test('read-only snapshot keeps ambiguous windows separate and suppresses focus while locked', () => {
    const first = {get_pid: () => 42, get_stable_sequence: () => 1};
    const second = {get_pid: () => 42, get_stable_sequence: () => 2};
    assert.deepEqual(windowSnapshot([first, second], second, false), {id: 0, nodes: [
        {id: 1, pid: 42, focused: false}, {id: 2, pid: 42, focused: true},
    ]});
    assert.equal(windowSnapshot([first], first, true).nodes[0].focused, false);
    assert.deepEqual(windowSnapshot([], null, false), {id: 0, nodes: []});
});

test('activates one exact process and refuses stale, ambiguous, or locked targets', async () => {
    const window = {get_pid: () => 42};
    let active = null;
    const options = {
        pid: 42, startTicks: '123', locked: () => false, windows: [window],
        readStat: () => `42 (foot (test)) S ${Array(18).fill('0').join(' ')} 123`,
        activateAndWait: async value => { active = value; return 'focused'; },
    };
    assert.equal(await focusWindow(options), 'focused');
    assert.equal(active, window);
    for (const [changes, result] of [
        [{startTicks: '124'}, 'changed'],
        [{windows: [window, window]}, 'ambiguous'],
        [{windows: []}, 'missing'],
        [{locked: () => true}, 'locked'],
        [{readStat: () => 'broken'}, 'changed'],
        [{pid: -1}, 'changed'],
    ]) {
        active = null;
        assert.equal(await focusWindow({...options, ...changes}), result);
        assert.equal(active, null);
    }
    assert.equal(await focusWindow({...options, activateAndWait: async () => 'unfocused'}), 'unfocused');
    let stat = options.readStat();
    assert.equal(await focusWindow({...options, readStat: () => stat,
        activateAndWait: async () => { stat = 'broken'; return 'focused'; },
    }), 'changed');
});

function fixture() {
    const target = {};
    const signals = new Map();
    let timeout = null;
    let active = null;
    let locked = false;
    let activations = 0;
    const watch = (name, callback) => {
        signals.set(name, callback);
        return () => signals.delete(name);
    };
    const platform = {
        activate: () => { activations++; },
        focused: () => active,
        locked: () => locked,
        watchFocus: callback => watch('focus', callback),
        watchClosed: (window, callback) => {
            assert.equal(window, target);
            return watch('closed', callback);
        },
        watchLock: callback => watch('lock', callback),
        schedule: (callback, milliseconds) => {
            assert.equal(milliseconds, 1000);
            timeout = callback;
            return () => { timeout = null; };
        },
    };
    return {
        target, platform,
        focus: value => { active = value; signals.get('focus')?.(); },
        close: () => signals.get('closed')?.(),
        lock: () => { locked = true; signals.get('lock')?.(); },
        timeout: () => timeout?.(),
        assertClean: () => {
            assert.equal(signals.size, 0);
            assert.equal(timeout, null);
        },
        activations: () => activations,
    };
}

test('waits for delayed focus and ignores another window gaining focus', async () => {
    const f = fixture();
    const confirmation = new FocusConfirmation(f.platform);
    let settled = false;
    const result = confirmation.confirm(f.target).then(value => { settled = true; return value; });
    await Promise.resolve();
    assert.equal(settled, false);
    f.focus({});
    await Promise.resolve();
    assert.equal(settled, false);
    f.focus(f.target);
    assert.equal(await result, 'focused');
    assert.equal(f.activations(), 1);
    f.assertClean();
});

test('accepts immediate focus and cleans up listeners and the timeout', async () => {
    const f = fixture();
    f.platform.activate = window => f.focus(window);
    assert.equal(await new FocusConfirmation(f.platform).confirm(f.target), 'focused');
    f.assertClean();
});

for (const [event, expected] of [['timeout', 'unfocused'], ['close', 'changed'], ['lock', 'locked']]) {
    test(`${event} settles a pending activation and releases its resources`, async () => {
        const f = fixture();
        const result = new FocusConfirmation(f.platform).confirm(f.target);
        f[event]();
        assert.equal(await result, expected);
        f.assertClean();
        f.focus(f.target); // A late notification cannot alter the completed result.
        assert.equal(await result, expected);
    });
}

test('disabling settles pending requests and prevents future activation', async () => {
    const f = fixture();
    const confirmation = new FocusConfirmation(f.platform);
    const result = confirmation.confirm(f.target);
    confirmation.disable();
    assert.equal(await result, 'changed');
    f.assertClean();
    assert.equal(await confirmation.confirm(f.target), 'changed');
    assert.equal(f.activations(), 1);
});

test('partial setup and activation failures clean up previously installed resources', async () => {
    for (const step of ['watchClosed', 'activate']) {
        const f = fixture();
        f.platform[step] = () => { throw new Error('Window disappeared'); };
        assert.equal(await new FocusConfirmation(f.platform).confirm(f.target), 'changed');
        f.assertClean();
    }
});
