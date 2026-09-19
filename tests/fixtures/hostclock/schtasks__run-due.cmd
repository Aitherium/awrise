@echo off
REM awrise-generated 0.2.0 - written by `awrise install --schtasks`.
REM Do not edit: the next install rewrites it from the wheel.
setlocal
set "AWRISE_HOME=C:\aw"
if not exist "C:\aw\logs" mkdir "C:\aw\logs"
C:\Python\python.exe -m awrise run-due --quiet --invoker schtasks >>"C:\aw\logs\run-due.log" 2>&1
REM The redirection comes FIRST on purpose. cmd.exe reads a digit
REM immediately left of `>>` as a FILE HANDLE, so the natural form
REM `echo ... exit=%ERRORLEVEL%>>"log"` logs `exit=` with the code
REM swallowed -- and for codes 0 and 2 redirects the line to stdin or
REM stderr, where the hidden shim loses it entirely. This line is the
REM only host-side evidence left when the ledger is the thing that
REM failed, so it is written where a handle cannot be read out of it.
>>"C:\aw\logs\run-due.log" echo %DATE% %TIME% awrise run-due exit=%ERRORLEVEL%
REM A failing JOB is not a failing TASK: the wake ledger carries the
REM verdict and `awrise install --check` reads it, so this wrapper
REM always reports success and the task's last result stays green.
exit /b 0
