on run argv
    set targetTTY to item 1 of argv
    if application "Terminal" is not running then return "missing"
    tell application "Terminal"
        set foundCount to 0
        repeat with w in windows
            repeat with t in tabs of w
                if tty of t is targetTTY then
                    set foundCount to foundCount + 1
                    set targetWindow to w
                    set targetTab to t
                end if
            end repeat
        end repeat
        if foundCount is 0 then return "missing"
        if foundCount is not 1 then return "ambiguous"
        set selected tab of targetWindow to targetTab
        set miniaturized of targetWindow to false
        set index of targetWindow to 1
        activate
        if tty of selected tab of front window is targetTTY then return "focused"
    end tell
    return "unfocused"
end run
