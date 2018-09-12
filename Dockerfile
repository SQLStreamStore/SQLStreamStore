FROM microsoft/dotnet:2.1.402-sdk-alpine3.7 AS build
WORKDIR /src

COPY ./src/*.sln ./
COPY ./src/*/*.csproj ./
RUN for file in $(ls *.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

COPY ./NuGet.Config ./

RUN dotnet restore

COPY ./src .

WORKDIR /build

COPY ./build/build.csproj .

RUN dotnet restore

COPY ./build .

WORKDIR /
