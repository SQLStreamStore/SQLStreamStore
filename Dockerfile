FROM microsoft/dotnet:2.1.402-sdk-alpine3.7 AS build
WORKDIR /src

# https://github.com/moby/moby/issues/15858
# Docker will flatten out the file structure on COPY
# We don't want to specify each csproj either - it creates pointless layers and it looks ugly
# https://code-maze.com/aspnetcore-app-dockerfiles/
COPY ./src/*.sln ./
COPY ./src/*/*.csproj ./
RUN for file in $(ls *.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

COPY ./NuGet.Config ./

COPY ./src .

WORKDIR /build

COPY ./build/build.csproj .

COPY ./build .

WORKDIR /
