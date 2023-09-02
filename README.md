# Calc filter plugin for Embulk

A filter plugin for Embulk to calculate with formula.

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: columns to calculate
-   **name**: name of column to output calculation result.
-   **formula**: calculation formula.

## Example

Input data

```
id,account,time,purchase,comment
1,32864,2015-01-27 19:23:49,20150127,embulk
2,14824,2015-01-27 19:01:23,20150127,embulk jruby
3,27559,2015-01-28 02:20:02,20150128,"Embulk ""csv"" parser plugin"
4,11270,2015-01-29 11:54:36,20150129,NULL
```

```yaml
filters:
  - type: calc
    columns:
    - name: id
      formula: "1 + id"
    - name: account
      formula: "0.5 + account + id"
```

The execution result is the following.

```
+---------+----------------+-------------------------+-------------------------+----------------------------+
| id:long | account:double |          time:timestamp |      purchase:timestamp |             comment:string |
+---------+----------------+-------------------------+-------------------------+----------------------------+
|       2 |        32865.5 | 2015-01-27 19:23:49 UTC | 2015-01-27 00:00:00 UTC |                     embulk |
|       3 |        14826.5 | 2015-01-27 19:01:23 UTC | 2015-01-27 00:00:00 UTC |               embulk jruby |
|       4 |        27562.5 | 2015-01-28 02:20:02 UTC | 2015-01-28 00:00:00 UTC | Embulk "csv" parser plugin |
|       5 |        11274.5 | 2015-01-29 11:54:36 UTC | 2015-01-29 00:00:00 UTC |                            |
+---------+----------------+-------------------------+-------------------------+----------------------------+
```

## Supported formula

* Basic Operators
  * Add: +
  * Sub: -
  * Mod: *
  * Div: /
  * Mod: %
  * Power: ^
  * Paren: ()
* Math functions
  * sin/cos/tan

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

For Maintainers
----------------

### Release

Modify `version` in `build.gradle` at a detached commit, and then tag the commit with an annotation.

```
git checkout --detach master

(Edit: Remove "-SNAPSHOT" in "version" in build.gradle.)

git add build.gradle

git commit -m "Release vX.Y.Z"

git tag -a vX.Y.Z

(Edit: Write a tag annotation in the changelog format.)
```

See [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) for the changelog format. We adopt a part of it for Git's tag annotation like below.

```
## [X.Y.Z] - YYYY-MM-DD

### Added
- Added a feature.

### Changed
- Changed something.

### Fixed
- Fixed a bug.
```

Push the annotated tag, then. It triggers a release operation on GitHub Actions after approval.

```
git push -u origin vX.Y.Z
```
