#!/usr/bin/env bash

docker build --tag sss-build .
docker run --rm --name sss-build \
 -v /var/run/docker.sock:/var/run/docker.sock \
 -v $PWD/artifacts:/artifacts \
 --network host \
 -e TRAVIS_BUILD_NUMBER=$TRAVIS_BUILD_NUMBER \
 -e MYGET_API_KEY=$MYGET_API_KEY \
 sss-build \
 dotnet run -p /build/build.csproj -- "$@"