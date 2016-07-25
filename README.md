# SQL Stream Store

A stream store library for .NET

**This project is under heavy development at this time.**

Key design considerations:

 - Designed to only support RDMBS\SQL implementations.
 - API and behaviour is influenced by [EventStore](https://geteventstore.com/)
 - Async by default
 - JSON only event and metadata payloads
 - No in-built snapshot support; can be implemented externally (it's just a projection, right?)
 - Leverage RDBMS specific notifications, if available, for low latency catch-up subscriptions
 - No support for `System.Transaction` enforcing the concept of the stream as the consistency and transaction boundary.

**TODO** See wiki for feature implementation status.

[CI Feed](https://www.myget.org/F/dh/api/v2)
