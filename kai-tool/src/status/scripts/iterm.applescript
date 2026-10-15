on run argv
    set targetTTY to item 1 of argv
    if application "iTerm2" is not running then return "missing"
    tell application "iTerm2"
        set foundCount to 0
        repeat with w in windows
            repeat with t in tabs of w
                repeat with s in sessions of t
                    if tty of s is targetTTY then
                        set foundCount to foundCount + 1
                        set targetWindow to w
                        set targetTab to t
                        set targetSession to s
                    end if
                end repeat
            end repeat
        end repeat
        if foundCount is 0 then return "missing"
        if foundCount is not 1 then return "ambiguous"
        tell targetWindow to select
        tell targetTab to select
        tell targetSession to select
        set miniaturized of targetWindow to false
        activate
        if tty of current session of current window is targetTTY then return "focused"
    end tell
    return "unfocused"
end run
