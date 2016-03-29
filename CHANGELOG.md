## v2.1.1 [2016-03-23]

### Release Notes

- Introducing CHANGELOG.md
- [PR #71](https://github.com/zalando/PGObserver/pull/71) [Issue #69](https://github.com/zalando/PGObserver/issues/69) : Fixing a poor design decison from past, where for "performance views" screens "hostname" was used for data fetching instead of "uishortname".
Users of "performance views" need to run a schema migration script (sql/schema_upgrade/02_performance_views_hostname_to_uishortname.sql)
