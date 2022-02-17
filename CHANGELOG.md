# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v2.1.0] - 2022-02-17
### Module fixes associated cru-ansible-oracle [v4.1.0] - 2022-02-17
- sysdbafacts - added more debugging. Added current controlfiles to output for reference by new module rmxctl.
- mkalias - more documentation in the header. More debugging.
- rmxctl - New module that removes unused obsolete controlfiles that can accumulate in ASM during refresh and cloning.       

## [v2.0.2] - 2022-01-03
### Module fix associated with v3.3.2
- Fix for dbfacts needed since adding dr environment to handle dr domain

## [v2.0.1] - 2021-12-28
### Module fixes associated with v2.0.0 and the massive merge of cru-ansible-oracle
- mkalias fixes to work on disaster recovery environment
- linkmapper fixes
- removed all references to os.system("/usr/bin/scl enable python27 bash") to correct way: #!/usr/bin/env python3

## [v2.0.0] - 2021-11-15
### Created new modules escpass.py, linkmapper.py, xtractitem.py
- escpass.py looks for special characters that need escaped to be used by oracle in a password.
- linkmapper.py takes a python list of filters and database link mappings and remaps current db links and synonyms to reflect mapping changes.
- xtracitem.py extracts a specified item from a string based on user defined criteria.
- OSB and Data Guard support

## [v1.4.0] - 2021-07-22
### Updated splitout module
- Created a module to run commands against a database and resturn ansible usable results if requested.

## [v1.3.5] - 2021-07-12
### Updated splitout module
- Fixed issue that caused module to crash when passed empty string.

## [v1.3.4] - 2021-07-09
### Updated dbfacts module
- Added extra oracle_version

## [v1.3.3] - 2021-07-09
### Updated srvctl module
- Fixed bug that caused module to crash when debugging directory wasn't present

## [v1.3.2] - 2021-06-18
### Updated dbdbfacts module
- Added data guard parameters to output of dbfacts module to aid in plays working with dg dbs.

## [v1.3.1] - 2021-06-07
### Updated mkalias
- Fixed mkalias module to delete previous alias if dbca missed it.

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

[Unreleased]: https://github.com/CruGlobal/cru-ansible-modules/compare/v2.0.2...HEAD

[v2.0.2]: https://github.com/CruGlobal/cru-ansible-modules/compare/v2.0.1...v2.0.2
[v2.0.1]: https://github.com/CruGlobal/cru-ansible-modules/compare/v2.0.0...v2.0.1
[v2.0.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.4.0...v2.0.0
[v1.4.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.3.5...v1.4.0
[v1.3.5]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.3.4...v1.3.5
[v1.3.4]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.3.3...v1.3.4
[v1.3.3]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.3.2...v1.3.3
[v1.3.2]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.3.1...v1.3.2
[v1.3.1]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.3.0...v1.3.1
[v1.3.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.4...v1.3.0
[v1.2.4]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.3...v1.2.4
[v1.2.3]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.2...v1.2.3
[v1.2.2]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.1...v1.2.2
[v1.2.1]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.2.0...v1.2.1
[v1.2.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.1.0...v1.2.0
[v1.1.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.0.0...v1.1.0
[v1.0.0]: https://github.com/CruGlobal/cru-ansible-modules/releases/tag/v1.0.0
