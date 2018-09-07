#!/usr/bin/env bash

docker run --rm --name sss-build -v $(pwd):/repo -v /var/run/docker.sock:/var/run/docker.sock -w /repo --network host microsoft/dotnet:2.1.401-sdk-alpine dotnet run -p build/build.csproj "$@"