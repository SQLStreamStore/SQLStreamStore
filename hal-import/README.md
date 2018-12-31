# SQL Stream Store HTTP Server [![Build Status](https://travis-ci.org/SQLStreamStore/SQLStreamStore.HAL.svg?branch=master)](https://travis-ci.org/SQLStreamStore/SQLStreamStore.HAL)

Fully RESTful HTTP Server for [SQL Stream Store](https://github.com/SQLStreamStore/SQLStreamStore). Uses [application/hal+json](https://tools.ietf.org/html/draft-kelly-json-hal-08) for resource navigation, with [JSON Hyper-Schema](https://json-schema.org/latest/json-schema-hypermedia.html) as [embedded resources](https://tools.ietf.org/html/draft-kelly-json-hal-08#section-4.1.2).

The solution includes example of use  LittleHalHost.Example project. The url structure and parameters are described in LittleHalHost.Example/readme.md

# Development

- `./build.sh` or `./build.cmd` to run the complete dockerized build.
- `dotnet run --project build/build.csproj` to build without using docker.
To see a list of build targets, `dotnet run --project build/build.csproj -- --list-targets`.
For general help with the build, `dotnet run --project build/build.csproj -- --help`.
- If you are running test from your IDE, you _must_ build with the `GenerateDocumentation` target at least once, otherwise the tests around documentation will fail.

# Clients

- [Official dotnet client](https://github.com/SQLStreamStore/SQLStreamStore)
- [Javascript ops user interface](https://github.com/SQLStreamStore/sql-stream-store-browser)

# Help & Support

Ask questions in the `#sql-stream-store` channel in the [ddd-cqrs-es slack](https://ddd-cqrs-es.slack.com) workspace. ([Join here](https://ddd-cqrs-es.herokuapp.com/)).

# Licences

Licenced under [MIT](LICENSE).