# Kai Window Focus

Internal status: **beta**. This companion extension supplies GNOME Wayland window activation
for `kai status --watch`. It exports narrow `Focus` and read-only `GetWindows` D-Bus methods;
it does not enable Shell.Eval. `GetWindows` returns numeric window IDs, process IDs, and focus
flags so the dashboard can clear unread completions when users switch through the desktop.
It excludes titles and contents and reports no focused window while the desktop is locked.
Update an older installation to enable automatic unread clearing through this backend.
The code targets GNOME Shell 45–51's ES module API. Native GNOME testing is still required;
the activation logic has fixture tests. Activation waits asynchronously for the requested window
to receive focus, for up to one second. Closing the window, locking the session, or disabling the
extension ends pending confirmation and removes its signal handlers and timer.

From this directory, package and install it for your user:

```sh
gnome-extensions pack --force --extra-source=focus.js --out-dir=/tmp .
gnome-extensions install --force /tmp/kai-window-focus@ferramentum.shell-extension.zip
```

Log out and back in on Wayland, then enable it:

```sh
gnome-extensions enable kai-window-focus@ferramentum
```

The dashboard also needs `gdbus` (usually provided by GLib). The extension refuses ambiguous
shared terminal processes and stale process IDs, and does not activate windows while locked.
Use a separate Foot, Alacritty, xterm, st, or urxvt process per window. GNOME Terminal, Console,
and other terminals with internal tabs need a further terminal integration; this extension
alone cannot identify their tabs. GNOME on X11 can use the `wmctrl`/`xprop` backend instead.

Run fixture tests with `node --test focus.test.js`. To check asynchronous GJS/D-Bus
replies and listener/timer cleanup on a private bus, install GJS and D-Bus and run
`GIO_USE_VFS=local dbus-run-session -- gjs -m dbus-smoke.js`. This smoke test uses a simulated window
and does not require GNOME Shell or an installed extension; native desktop testing
remains separate.

Remove the installed extension with
`gnome-extensions uninstall kai-window-focus@ferramentum`.
