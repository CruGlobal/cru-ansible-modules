# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
[v1.2.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.1.0...v1.2.0
[v1.1.0]: https://github.com/CruGlobal/cru-ansible-modules/compare/v1.0.0...v1.1.0
[v1.0.0]: https://github.com/CruGlobal/cru-ansible-modules/releases/tag/v1.0.0
