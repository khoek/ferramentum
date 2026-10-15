// Beta companion backend. No Shell.Eval or arbitrary command execution.
import Gio from 'gi://Gio';
import GLib from 'gi://GLib';
import {Extension} from 'resource:///org/gnome/shell/extensions/extension.js';
import * as Main from 'resource:///org/gnome/shell/ui/main.js';
import {FocusConfirmation, focusWindow, windowSnapshot} from './focus.js';

const interfaceXml = `<node>
  <interface name="org.gnome.Shell.Extensions.KaiWindowFocus">
    <method name="GetWindows">
      <arg name="snapshot" type="s" direction="out"/>
    </method>
    <method name="Focus">
      <arg name="pid" type="u" direction="in"/>
      <arg name="startTicks" type="s" direction="in"/>
      <arg name="result" type="s" direction="out"/>
    </method>
  </interface>
</node>`;

export default class KaiWindowFocus extends Extension {
    enable() {
        const watch = (object, signal, callback) => {
            const id = object.connect(signal, callback);
            return () => object.disconnect(id);
        };
        const confirmation = new FocusConfirmation({
            activate: window => Main.activateWindow(window),
            focused: () => global.display.focus_window,
            locked: () => Main.sessionMode.isLocked,
            watchFocus: callback => watch(global.display, 'notify::focus-window', callback),
            watchClosed: (window, callback) => watch(window, 'unmanaged', callback),
            watchLock: callback => watch(Main.sessionMode, 'updated', callback),
            schedule: (callback, milliseconds) => {
                let id = GLib.timeout_add(GLib.PRIORITY_DEFAULT, milliseconds, () => {
                    id = 0;
                    callback();
                    return GLib.SOURCE_REMOVE;
                });
                return () => {
                    if (id)
                        GLib.Source.remove(id);
                    id = 0;
                };
            },
        });
        this._confirmation = confirmation;
        this._service = Gio.DBusExportedObject.wrapJSObject(interfaceXml, {
            GetWindows: () => JSON.stringify(windowSnapshot(
                global.get_window_actors().map(actor => actor.meta_window),
                global.display.focus_window, Main.sessionMode.isLocked)),
            FocusAsync: ([pid, startTicks], invocation) => {
                const reply = result => invocation.return_value(new GLib.Variant('(s)', [result]));
                focusWindow({
                    pid,
                    startTicks,
                    locked: () => Main.sessionMode.isLocked,
                    windows: global.get_window_actors().map(actor => actor.meta_window),
                    readStat: target => {
                        const [ok, data] = GLib.file_get_contents(`/proc/${target}/stat`);
                        return ok ? new TextDecoder().decode(data) : '';
                    },
                    activateAndWait: window => confirmation.confirm(window),
                }).then(reply, () => reply('changed'));
            },
        });
        this._service.export(Gio.DBus.session, '/org/gnome/Shell/Extensions/KaiWindowFocus');
    }

    disable() {
        this._confirmation?.disable();
        this._confirmation = null;
        this._service?.unexport();
        this._service = null;
    }
}
