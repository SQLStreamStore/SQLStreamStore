# SQL Stream Store ![CI](https://github.com/SQLStreamStore/SQLStreamStore/workflows/CI/badge.svg) [![release](https://img.shields.io/github/release/SQLStreamStore/SQLStreamStore.svg)](https://github.com/SQLStreamStore/SQLStreamStore/releases) [![license](https://img.shields.io/github/license/SQLStreamStore/SQLStreamStore.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/SQLStreamStore/SQLStreamStore.svg) [![docs status](https://img.shields.io/readthedocs/sqlstreamstore.svg?logo=readthedocs&style=popout)](https://sqlstreamstore.readthedocs.io) [<img src="https://img.shields.io/badge/slack-DDD--CQRS--ES%20%23sql--stream--store-yellow.svg?logo=slack">](https://t.co/MRxpx0rLH2)

:warning: These libraries are no longer actively maintained.

A stream store library for .NET that specifically targets SQL based implementations. Primarily used to implement Event Sourced applications.

| Package | Install |
| --- | --- |
| [SqlStreamStore](https://www.nuget.org/packages/SqlStreamStore) (includes in-memory version for behaviour testing) | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg?logo=nuget)](https://www.nuget.org/packages/SqlStreamStore) |
| [MS SQL Server](https://www.nuget.org/packages/SqlStreamStore.MsSql) / Azure SQL Database | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg?logo=nuget)](https://www.nuget.org/packages/SqlStreamStore.MsSql) |
| [PostgreSQL](https://www.nuget.org/packages/SqlStreamStore.Postgres) / AWS Aurora | [![NuGet](https://img.shields.io/nuget/vpre/SqlStreamStore.Postgres.svg?logo=nuget)](https://www.nuget.org/packages/SqlStreamStore.Postgres) |
| [MySQL](https://www.nuget.org/packages/SqlStreamStore.MySql) / AWS Aurora | [![NuGet](https://img.shields.io/nuget/vpre/SqlStreamStore.MySql.svg?logo=nuget)](https://www.nuget.org/packages/SqlStreamStore.MySql) |
| Sqlite | [_up for grabs_](https://github.com/SQLStreamStore/SqlStreamStore/issues/28) |
| HTTP Wrapper API | On CI Feed |
| [Schema Creation Script Tool](https://www.fuget.org/packages/SqlStreamStore.SchemaCreationScriptTool) | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.SchemaCreationScriptTool.svg?logo=nuget)](https://www.nuget.org/packages/SqlStreamStore.SchemaCreationScriptTool) |

CI Packages available [on Feedz](https://f.feedz.io/logicality/streamstore-ci/nuget/index.json).

# Design considerations:

 - Designed to only ever support RDMBS/SQL implementations.
 - Subscriptions are eventually consistent.
 - API is influenced by (but not compatible with) [EventStore](https://eventstore.org/).
 - Async only.
 - JSON only event and metadata payloads (usually just a `string` / `varchar` / etc).
 - No support for `System.Transaction`, enforcing the concept of the stream as the consistency and transaction boundary.

# Building

Building requires Docker. Solution and tests are run on a linux container with .NET Core leveraging SQL Server, Postgres and MySQL as sibling containers.

 - Windows, run `.\build.cmd`
 - Linux, run `./build.sh`

Note: build does not work via WSL.

# Help & Support

Ask questions in the `#sql-stream-store` channel in the [ddd-cqrs-es slack](https://ddd-cqrs-es.slack.com) workspace. ([Join here](https://j.mp/ddd-es-cqrs)).

# Licences

Licenced under [MIT](LICENSE).
