# SQL StreamStore

A stream store library for .NET that specifically targets RDBMS based
implementations. Typically used in event sourced based applications.

| Package | Install |
| --- | --- |
| Core Library (with in-memory store for testing) | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore) |
| MSSql | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore.MsSql) |
| PostgreSQ: | [_under development_](https://github.com/damianh/SqlStreamStore/issues/98) |
| MySql | [_under development_](https://github.com/damianh/SqlStreamStore/issues/29) |
| Sqlite | [_up for grabs_](https://github.com/damianh/SqlStreamStore/issues/28) |
| HTTP API | [_under development_](https://github.com/SQLStreamStore/SQLStreamStore.HAL) |


<!-- TOC depthFrom:2 withLinks:true -->

- [SQL StreamStore](#sql-streamstore)
  - [1. Introduction](#1-introduction)
  - [2. Things you need to know before adopting](#2-things-you-need-to-know-before-adopting)
  - [3. Using](#3-using)
    - [3.1. Appending messages](#31-appending-messages)
    - [3.2.1 Idempotency](#321-idempotency)
    - [3.2.2 Deterministed Message ID Generation](#322-deterministed-message-id-generation)
    - [3.2. Reading messages](#32-reading-messages)
      - [3.2.1. All Stream](#321-all-stream)
      - [3.2.2. Individual Streams](#322-individual-streams)
    - [3.3. Stream Metadata](#33-stream-metadata)
    - [3.4. Subscriptions](#34-subscriptions)
    - [3.5. Deleting messages](#35-deleting-messages)
  - [4. MS SqlServer](#4-ms-sqlserver)
    - [4.1. Setting up](#41-setting-up)
    - [4.1. Schema Notes](#41-schema-notes)
  - [5. Building](#5-building)
  - [6. Acknowledgements](#6-acknowledgements)

<!-- /TOC -->

## 1. Introduction
SQLStreamStore is a .NET library to assist with developing applications that use
event sourcing or wish to use stream based patterns over a relational database
and existing operational infrastructure.

This documentation assumes you already have some knowledge of event sourcing. If
not, there is a good guide on [Event Sourcing
Basics](https://eventstore.org/docs/event-sourcing-basics/index.html) by the
EventStore team.

The reasons for creating this and a comparison with NEventStore and EventStore
can be viewed
[here](https://github.com/SQLStreamStore/SQLStreamStore/issues/108#issuecomment-348154346).

## 2. Things you need to know before adopting

- This is first and foremost a _library_ to help with working with stream based
  concepts implemented on RDMBS with SQL. It has no intention of becoming a full
  blown application/database server.

- While it helps you with working with stream concepts over a relational
  databases, you must still be have mechanical apathy with the underlying
  database such as limits, log growth, performance characteristics, exception
  handling etc.

- SQLStreamStore/ A relational database will never be be as fast as custom
  stream / event based databases (e.g EventStore, Kafka). For DDD applications
  that would otherwise use a traditional RDBMS databases (and ORMs) it should
  perform within expectations.

- Subscriptions (and thus projections) are eventually consistent and always will
  be.

- You must understand your application's characteristics in terms of load, data
  growth, acceptable latency for eventual consistency etc.

- Message metadata payloads are strings only and expected to be JSON format.
  This is for operational reasons to support splunking a database using it's
  standard administration tools. Other serialization formats (or compression)
  are not support (strictly speaking JSON isn't _enforced_ either).

- No support for ambient `System.Transaction` scopes enforcing the concept of
  the stream as the consistency and transactional boundary.

## 3. Using

The following usage applies to the core interface `IStreamStore`. For storage
specific notes please refer to the specific section.

The in-memory store is used primarily to assist with testing. While it's
behavior mimics the relational behavior as reasonably as possible one should
still perform adequate integration testing with a real database.

To create an in-memory store:

    var store = new InMemoryStreamStore();

All store implementations support injecting a delegate for getting the current
UTC date time via the `GetUtcNow` delegate. This allows creating deterministic
datetime stamps on messages and other operations, if desired.

    GetUtcNow getUtcNow = () => new DateTime(2020, 3, 15, 8, 0, 0);
    var store = new InMemoryStreamStore(getUtcNow);

Logging is internally handled with [LibLog](https://github.com/damianh/LibLog).
All store implementations support defining the logger name (aka 'Category') if
you wish to customize it. The default logger name is the type name of the store
implementation.

    var store = new InMemoryStreamStore(loggerName: "StreamStore");

### 3.1. Appending messages

The method to append a message to a stream is:

    Task<AppendResult> AppendToStream(
            StreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken = default);

1. The `streamId` is a value object that wraps a string ensuring non-null and no
   whitespace. StreamIds lengths should not exceed the limits set by underlying
   store.
2. The `expectedVersion` parameter is used for concurrency checking. You can
   supply a specific that you expect the stream to be at and if the stream is at
   a different version, a `WrongExpectedVersionException` is thrown.
   Alternatively you can supply `ExpectedVersion.Any` if you don't care what the
   current stream version is (including if it doesn't yet exist) or
   `ExpectedVersion.NoStream` if you explicitly expect it to not yet exist.
3. The `message` parameter defines the collection of messages that are appended
   in a transaction. If empty or null then the call is effectively a no-op.
4. `AppendResult` return value contains two properties, `CurrentVersion` and
   `CurrentPosition`. This is useful to return to callers if they need to
   subsequently load a projection to help with reading their own writes.

The constructor of `NewStreamMessage` is:

    public NewStreamMessage(
        Guid messageId,
        string type,
        string jsonData,
        string jsonMetadata = null)

1. `messageId` parameter is the unique id of the message being appenended.
   It has an important function with regards to idempotent handling (See
   idempotency section below). MessageIds within stream must be unique.
2. `type` parameter represents the message type. Examples include `car-leased`
   and `customer-registered`. Using a fully qualified CLR type name (e.g.
   `Company.App.Domian.Event.Foo`) is anti-pattern. CLR types are re-named and
   moved so you want to maintain a map of event type -> clr type in your
   application.
3. `jsonData` paramater is string. SQLStreamStore doesn't check
   the structure nor validity of this. It is names json to encourage json only
   usage.
4. `jsonMetadata` paramater is option metadata about the message that is
   typically orthogonal and/or doesn't belong in the main message body. Examples
   of usage include the security context (`sub` / `client_id`) that caused the
   event as well as causation / correlation identifiers.

### 3.2.1 Idempotency

Idempotent appends is when an stream append operation occurs multiple times but
the messages are appended once. This is useful for retry / resume type of
operations. When appending messages, the `MessageId` of `NewStreamMessage`,
coupled with the `expectedVersion`, determines the idempotency policy applied.

 - `ExpectedVersion.Any` if the collection of messages have been previously written

### 3.2.2 Deterministed Message ID Generation



### 3.2. Reading messages

#### 3.2.1. All Stream

#### 3.2.2. Individual Streams

### 3.3. Stream Metadata

### 3.4. Subscriptions

### 3.5. Deleting messages

## 4. MS SqlServer

### 4.1. Setting up

### 4.1. Schema Notes

For operational reasons, you should familiarlize yourself with the underlying
database schema. The schema is implemented as two tables, `Streams` and
`Messages`, and a number of indexes.

The `Streams` table contains the collection of unique streams in the store. As
stream Ids are strings there are a number of optimisations applied:

1. The original stream Id is stored in column `IdOriginal` and limited to 1000
   characters.

2. When a stream is appended for first time is it checked to see if it is
   parsable as a `Guid`. If so, then that is stored in the `Id` column. If not
   then a `Sha1` hash of the Id is generated and used. This helps with stream
   lookups and the Id unique index constraint.

3. An `IdInternal` identity column is used for joins with the messages table.

Please refer to `CreateSchema.sql` for full schema details.

## 5. Building

## 6. Acknowledgements

- [EventStore](https://eventstore.org) from which SQLStreamStore leans heavily
  on a subset of its .NET API.
