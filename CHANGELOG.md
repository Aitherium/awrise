# Changelog

All notable changes to `awrise` are recorded here.

## [0.4.3] - 2026-11-07

### Fixed

- **awrise**: on Linux a zombie (exited, not yet reaped) counts as dead, so an overdue detached child that was SIGKILLed no longer keeps its job's lock Held.
- **awrise**: `__version__` and the package version are one number again.

