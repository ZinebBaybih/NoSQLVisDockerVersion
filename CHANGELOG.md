# Changelog

All notable changes to NoSQL Vis are documented in this file.

## [v1.1] - 2026-05-18

### Fixed

- Redis metadata load redesigned: replaced full-keyspace `KEYS *` scan with `DBSIZE` for exact key count and bounded `SCAN` sampling for type and prefix statistics.
- Cassandra total row count changed from automatic `COUNT(*)` on load to on-demand retrieval via the "Calculate Total" button.

### Changed

- MongoDB adapter: native indexed equality predicates when indexed fields are detected; falls back to preview-level filtering otherwise.
- Cassandra adapter: native CQL equality filtering on partition keys and indexed columns; falls back to preview-level filtering otherwise.

## [v1.0] - 2026-04-23

- Initial release.
