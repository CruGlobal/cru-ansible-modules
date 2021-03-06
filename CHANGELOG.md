# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v1.3.0] - 2021-04-16
### Updated dbfacts
- Gather database facts needed to setup data guard

## [v1.2.4] - 2020-09-23
### Changed
- Fixed indentation problem causing `orafacts.py` to not list all databases in `database_details`

## [v1.2.3] - 2020-07-26
### Updated dbfacts
- added local host debug.log ( ~/.debug.log ) for debugging dbfacts.py module
- added more debugging to orafacts.py module. Also fixed issue where orafacts
  was not handling si on new server that only had ASM running and no other dbs.

## [v1.2.2] - 2020-07-22
### Updated dbfacts
- Fix for dbfacts connecting to a single instance database running on RAC host

## [v1.2.1] - 2020-07-06
- Fixed issue where orafacts.py module was incorrectly reporting nodes for RAC

- Also fixed issue of orafacts.py module was not handling MGMTDB correctly.

## [v1.2.0] - 2020-05-19
- Removed resize functionality from redologs module and moved it to utils GUI

- Added functionality in orfacts module to determine ORACLE_HOME using running
  processes if a database is running with no /etc/oratab entry.

## [v1.1.0] - 2020-04-22
### Added
- New module `splitout`
### Updated
- Use Python 3 for local action modules
- Various fixes

## [v1.0.0] - 2020-02-07

### Added
- This CHANGELOG file
- Basic CONTRIBUTING template
- Removed redo resize functionality from redologs.py module since this is
  more dynamic than Ansible. The functionality was moved to utils GUI app
  which resides in the DBA Google drive.

[Unreleased]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.4...HEAD

[v1.2.4]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.3...v1.2.4
[v1.2.3]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.2...v1.2.3
[v1.2.2]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.1...v1.2.2
[v1.2.1]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.0...v1.2.1
[v1.2.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.1.0...v1.2.0
[v1.1.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.0.0...v1.1.0
[v1.0.0]: https://github.com/CruGlobal/cru-ansible-modules/releases/tag/v1.0.0
