A stream store library for .NET that specifically targets RDBMS based
implementations. Typically used in event sourced based applications.

| Package | Install |
| --- | --- |
| Core Library (with in-memory store for testing) | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore) |
| MSSql | [![NuGet](https://img.shields.io/nuget/v/SqlStreamStore.svg)](https://www.nuget.org/packages/SqlStreamStore.MsSql) |
| Postgres | [_up for grabs_](https://github.com/damianh/SqlStreamStore/issues/98) |
| MySql | [_under development_](https://github.com/damianh/SqlStreamStore/issues/29) |
| Sqlite | [_up for grabs_](https://github.com/damianh/SqlStreamStore/issues/28) |
| HTTP API | [_under development_](https://github.com/SQLStreamStore/SQLStreamStore.HAL) |

<!-- TOC -->

- [1. Preface](#1-preface)
- [2. Things you need to know before adopting](#2-things-you-need-to-know-before-adopting)
- [3. Using](#3-using)
    - [3.1. Appending messages](#31-appending-messages)
    - [3.2. Reading messages](#32-reading-messages)
        - [3.2.1. All Stream](#321-all-stream)
        - [3.2.2. Individual Streams](#322-individual-streams)
    - [3.3. Stream Metadata](#33-stream-metadata)
    - [3.4. Subscriptions](#34-subscriptions)
    - [3.5. Deleting messages](#35-deleting-messages)
    - [3.6. Testing with In Memory provider](#36-testing-with-in-memory-provider)
- [4. MS SqlServer Provider](#4-ms-sqlserver-provider)
- [5. MySql Provider](#5-mysql-provider)
- [6. Patterns](#6-patterns)
- [7. Acknowledgements](#7-acknowledgements)

<!-- /TOC -->

# 1. Preface

I built this libray to primarily assist with building event sourced and stream
based applications with collaborative domains using relational databases as the
persistance store. The need to support relational stores is due to deploying
software where the only operational support is with relational databases only.
That is, introducing and deploying alternative specialized storage is not viable
nor accepted. If you are not bound by such constraints, then you may consider
alternatives such as [Event Store](https://geteventstore.com)

The purpose of this library is to provide stream based API over relational based
stores.  The API and behavior is heavily influenced by  but is not designed to be completely
compatible with it. That is, if your needs grow 

# 2. Things you need to know before adopting

 - This is first and foremost a _library_ to help with working with stream based
   concepts supporting RDMBS\SQL implementations. It has no intention of
   becoming a full blown application/databse server.

 - While it helps you with working with stream concepts over a relational
   databases, you must still be have mechanical apathy with the underlying
   database such as limits, log growth, performance characteristics, exception
   handling etc.

 - Subscriptions (and thus projections) are eventually consistent always will
   be.

 - You must understand your application's characteristics in terms of load, data
   growth, acceptable latency for eventual consistency etc.

 - Message metadadata payloads are strings only and expected to be JSON format.
   This is for operational reasons to support splunking a database using it's
   standard administration tools. Other serialization formats (or compression)
   are not support (strictly speaking JSON isn't _enforced_ either).

 - No support for ambient `System.Transaction` scopes enforcing the concept of
   the stream as the consistency and transactional boundary.

# 3. Using

## 3.1. Appending messages

## 3.2. Reading messages

### 3.2.1. All Stream

### 3.2.2. Individual Streams

## 3.3. Stream Metadata

## 3.4. Subscriptions

## 3.5. Deleting messages

## 3.6. Testing with In Memory provider

# 4. MS SqlServer Provider

# 5. MySql Provider

# 6. Patterns

# 7. Acknowledgements



