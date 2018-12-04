# SQL Stream Store [![Build Status](https://travis-ci.org/SQLStreamStore/SQLStreamStore.svg?branch=master)](https://travis-ci.org/SQLStreamStore/SQLStreamStore)

A stream store library for .NET that specifically target SQL based implementations. Typically
used in Event Sourced based applications.

| Package | Install |
| --- | --- |
| SqlStreamStore (Memory) | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore) |
| MSSql | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore.MsSql) |
| Postgres | On CI Feed |
| MySql | [_up for grabs_](https://github.com/SQLStreamStore/SqlStreamStore/issues/29) |
| Sqlite | [_up for grabs_](https://github.com/SQLStreamStore/SqlStreamStore/issues/28) |
| HTTP Wrapper API | On CI Feed |

CI Packages available [on MyGet](https://www.myget.org/gallery/sqlstreamstore)

# Design considerations:

 - Designed to only ever support RDMBS\SQL implementations.
 - Subscriptions are eventually consistent.
 - API is influenced by (but not compatible with) [EventStore](https://eventstore.org/).
 - Async only.
 - JSON only event and metadata payloads (usually just a `string` / `varchar` / etc).
 - No support for `System.Transaction` enforcing the concept of the stream as the consistency and transaction boundary.

# Building

Building requires Docker. Solution and tests are run on a linux container with .net core leveraging SQL Server and Postgres as sibling containers.

 - Widows, run `.\build.cmd`
 - Linux, run `./build.sh`

Note: build does not work via WSL.

# Help & Support

Ask questions in the `#sql-stream-store` channel in the [ddd-cqrs-es slack](https://ddd-cqrs-es.slack.com) workspace. ([Join here](https://t.co/MRxpx0rLH2)).

# Licences

Licenced under [MIT](LICENSE).
