# Contributing to cru-ansible-modules
The format is based on [Keep Contributing](https://github.com/jokeyrhyme/keep-contributing),

This document contains a set of guidelines to help you during the contribution
process.
Please review it in order to ensure a fast and effective response from the
maintainers.

## Table Of Contents

- [Submitting contributions](#submitting-contributions)


## Submitting contributions

We are glad to receive code contributions in the form of patches, improvements
and new features.
Below you will find the process and workflow used to review and merge your
changes.

### Branch

Create a new branch. Use its name to identify the issue your addressing.

```sh
git checkout -b <branch-name>
```

### Code

- **Document your changes**: Add or update the relevant entries for your change
  in the documentation to reflect your work and inform the users about it.
  If you don't write documentation, we have to, and this can hold up acceptance
  of your changes

- **Update the CHANGELOG**
  - Determine next release version based on [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
  - Add release to the changelog and update markdown relative links.
  - For more information see [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)


### Commit

Try to commit as often as you can, keeping your changes logically grouped
within individual commits.
Generally it's easier to review changes that are split across multiple commits.

```sh
git add changed-file-1 changed-file-2
git commit
```

Use git interactive rebase if you need to tidy up your commits.

### Push

When your work is ready and complies with the project conventions,
upload your changes to your fork:

```sh
git push origin <branch-name>
```

### Pull request

Open a new Pull Request from your working branch to `master`.
Write about the changes and add any relevant information to the reviewer.

### Review and approval

Merge Pull Request after approval.

### Create Git Tag

Create tag to match release in the CHANGELOG.

```
git checkout master
git pull
git tag <release>
git push --tags
```
