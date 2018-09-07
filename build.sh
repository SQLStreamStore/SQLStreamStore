#!/usr/bin/env bash

docker run --rm --name sss-build -v /var/run/docker.sock:/var/run/docker.sock -v $(pwd):/repo -w /repo microsoft/dotnet:2.1.401-sdk-alpine dotnet run -p /src/build/build.csproj "$@"