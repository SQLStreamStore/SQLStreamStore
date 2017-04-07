# SQL Stream Store

A stream store library for .NET that specifically target SQL based implementations. 
Typically used in Event Sourced based applications.

# Design considerations:

 - Designed to only ever support RDMBS\SQL implementations.
 - Subscriptions are eventually consistent.
 - API is influenced by (but not compatible with) [EventStore](https://geteventstore.com/)
 - Async only.
 - JSON only event and metadata payloads (usually just a `string` / `varchar` / etc).
 - No support for `System.Transaction` enforcing the concept of the stream as the consistency and transaction boundary.
