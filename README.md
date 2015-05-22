# Cedar EventStore

Cedar Event Store is an event sourcing library for .NET

Key design considerations:

 - Designed to primarily support RDMBS\SQL implementations
 - Highly API and behaviorally compatible with [EventStore](https://geteventstore.com/)
 - Async by default
 - PCL for embedded event store scenarios
 - dnxcore50 support will be available as soon as dnxcore50 is rtm
 - JSON only event and metadata payloads
 - No in-built snapshot support; can be implemented externally
 - Levarge RDBMS specific notifications for low latency catch-up subscriptions
 - No support for `System.Transaction` enforcing the stream as the consistency / transaction boundary.

TODO See wiki for feature implementation status.

[CI Feed](https://www.myget.org/F/cedar/api/v2) - note very unstable.
