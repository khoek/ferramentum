import Gio from 'gi://Gio';
import GLib from 'gi://GLib';
import {FocusConfirmation, focusWindow, windowSnapshot} from './focus.js';

const xml = `<node><interface name="org.gnome.Shell.Extensions.KaiWindowFocus"><method name="Focus"><arg type="u" direction="in"/><arg type="s" direction="in"/><arg type="s" direction="out"/></method><method name="GetWindows"><arg type="s" direction="out"/></method></interface></node>`;
const loop = GLib.MainLoop.new(null, false);
const target = {get_pid: () => 42, get_stable_sequence: () => 1};
const signals = new Map();
let active = null;
let timers = 0;
let failure = null;
const schedule = (callback, milliseconds) => {
    timers++;
    let id = GLib.timeout_add(GLib.PRIORITY_DEFAULT, milliseconds, () => {
        id = 0;
        timers--;
        callback();
        return GLib.SOURCE_REMOVE;
    });
    return () => {
        if (id) {
            GLib.Source.remove(id);
            timers--;
        }
        id = 0;
    };
};
const watch = (name, callback) => {
    signals.set(name, callback);
    return () => signals.delete(name);
};
const confirmation = new FocusConfirmation({
    activate: window => schedule(() => {
        active = window;
        signals.get('focus')?.();
    }, 100),
    focused: () => active,
    locked: () => false,
    watchFocus: callback => watch('focus', callback),
    watchClosed: (_window, callback) => watch('closed', callback),
    watchLock: callback => watch('lock', callback),
    schedule,
});
const service = Gio.DBusExportedObject.wrapJSObject(xml, {
    GetWindows: () => JSON.stringify(windowSnapshot([target], active, false)),
    FocusAsync: ([pid, startTicks], invocation) => {
        focusWindow({
            pid, startTicks, locked: () => false, windows: [target],
            readStat: () => `42 (foot) S ${Array(18).fill('0').join(' ')} 123`,
            activateAndWait: window => confirmation.confirm(window),
        }).then(result => invocation.return_value(new GLib.Variant('(s)', [result])));
    },
});
const connection = Gio.DBus.session;
const path = '/org/gnome/Shell/Extensions/KaiWindowFocus';
service.export(connection, path);
const start = GLib.get_monotonic_time();
const watchdog = GLib.timeout_add(GLib.PRIORITY_DEFAULT, 3000, () => {
    failure = new Error('D-Bus reply timed out');
    loop.quit();
    return GLib.SOURCE_REMOVE;
});
connection.call(connection.get_unique_name(), path,
    'org.gnome.Shell.Extensions.KaiWindowFocus', 'Focus',
    new GLib.Variant('(us)', [42, '123']), new GLib.VariantType('(s)'),
    Gio.DBusCallFlags.NONE, 2000, null, (source, reply) => {
        try {
            const [result] = source.call_finish(reply).deepUnpack();
            if (result !== 'focused' || GLib.get_monotonic_time() - start < 100000)
                throw new Error(`Unexpected asynchronous reply: ${result}`);
            if (signals.size || timers)
                throw new Error('Confirmation leaked signals or timers');
            connection.call(connection.get_unique_name(), path,
                'org.gnome.Shell.Extensions.KaiWindowFocus', 'GetWindows',
                null, new GLib.VariantType('(s)'), Gio.DBusCallFlags.NONE, 2000, null,
                (source, reply) => {
                    try {
                        const [json] = source.call_finish(reply).deepUnpack();
                        const window = JSON.parse(json).nodes[0];
                        if (window.id !== 1 || window.pid !== 42 || !window.focused)
                            throw new Error(`Unexpected window snapshot: ${json}`);
                        print('GJS asynchronous focus and read-only window snapshot passed; cleanup passed');
                    } catch (error) { failure = error; }
                    GLib.Source.remove(watchdog);
                    loop.quit();
                });
            return;
        } catch (error) {
            failure = error;
        }
        GLib.Source.remove(watchdog);
        loop.quit();
    });
loop.run();
confirmation.disable();
service.unexport();
if (failure)
    throw failure;
