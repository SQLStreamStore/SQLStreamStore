# SQL Stream Store

A stream store library for .NET that specifically target SQL based implementations. Typically
used in Event Sourced based applications.

| Package | Install |
| --- | --- |
| SqlStreamStore (Memory) | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore) |
| MSSql | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore.MsSql) |
| Postgres | _under development_ |
| MySql | [_up for grabs_](https://github.com/damianh/SqlStreamStore/issues/29) |
| Sqlite | [_up for grabs_](https://github.com/damianh/SqlStreamStore/issues/28) |
| HTTP Wrapper API | _under development_ |

# Design considerations:

 - Designed to only ever support RDMBS\SQL implementations.
 - Subscriptions are eventually consistent.
 - API is influenced by (but not compatible with) [EventStore](https://geteventstore.com/).
 - Async only.
 - JSON only event and metadata payloads (usually just a `string` / `varchar` / etc).
 - No support for `System.Transaction` enforcing the concept of the stream as the consistency and transaction boundary.

# Using

Coming soon.

# Help & Support

Ask questions in the `#sql-stream-store` channel in the [ddd-cqrs-es slack](https://ddd-cqrs-es.slack.com) workspace. ([Join here](https://ddd-cqrs-es.herokuapp.com/)). 

# Licences

Licenced under [MIT](LICENSE).
