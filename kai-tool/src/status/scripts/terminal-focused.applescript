-- Read only; never launch, activate, or change the selected tab.
if application "Terminal" is not running then return ""
tell application "Terminal"
    if frontmost and (count of windows) > 0 then
        if not miniaturized of front window then return tty of selected tab of front window
    end if
end tell
return ""
