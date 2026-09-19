' Launch a console payload with no visible window.
' awrise-generated 0.2.0 - rewritten on every install; do not edit.
' wscript.exe is GUI-subsystem, so Task Scheduler allocates NO console for
' it; WScript.Shell.Run(cmd, 0, True) starts the payload hidden and WAITS,
' so the payload's exit code still reaches the scheduler.
Set sh = CreateObject("WScript.Shell")
args = ""
For i = 0 To WScript.Arguments.Count - 1
  args = args & """" & WScript.Arguments(i) & """ "
Next
WScript.Quit sh.Run(args, 0, True)
