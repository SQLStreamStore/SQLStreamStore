# SQL Stream Store

A stream store library for .NET that specifically target SQL based implementations. Typically
used in Event Sourced based applications.

| Package | |
| --- | --- |
| SqlStreamStore | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore) |
| MSSql | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore.MsSql) |
| Postgres | _under development_ |
| MySql | [_up for grabs_](https://github.com/damianh/SqlStreamStore/issues/29) |
| Sqlite | [_up for grabs_](https://github.com/damianh/SqlStreamStore/issues/28) |
| HTTP Wrapper API | _under development_ |

# Key design considerations:

 - Designed to only support RDMBS\SQL implementations.
 - API and behaviour is influenced by [EventStore](https://geteventstore.com/)
 - Async by default
 - JSON only event and metadata payloads
 - No in-built snapshot support; can be implemented externally (it's just a projection, right?)
 - Leverage RDBMS specific notifications, if available, for low latency catch-up subscriptions
 - No support for `System.Transaction` enforcing the concept of the stream as the consistency and transaction boundary.

