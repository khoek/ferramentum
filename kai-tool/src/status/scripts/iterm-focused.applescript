-- Read only; never launch, activate, or change the selected pane.
if application "iTerm2" is not running then return ""
tell application "iTerm2"
    if frontmost and (count of windows) > 0 then
        if not miniaturized of current window then return tty of current session of current window
    end if
end tell
return ""
