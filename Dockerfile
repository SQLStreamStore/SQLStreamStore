FROM mcr.microsoft.com/dotnet/sdk:7.0-jammy

RUN apt-get update \
    && apt-get install git \
    && apt-get install \
    ca-certificates \
    curl \
    gnupg \
    lsb-release -y \
    && curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install docker-ce-cli -y \
    && apt-get install zip -y

WORKDIR /repo

# https://github.com/moby/moby/issues/15858
# Docker will flatten out the file structure on COPY
# We don't want to specify each csproj either - it creates pointless layers and it looks ugly
# https://code-maze.com/aspnetcore-app-dockerfiles/
COPY ./*.sln ./

COPY ./build/ ./build/

COPY ./src/*/*.csproj ./src/
RUN for file in $(ls src/*.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

COPY ./tests/*/*.csproj ./tests/
RUN for file in $(ls tests/*.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

RUN dotnet restore

COPY ./assets ./assets/

COPY ./src ./src/

COPY ./tests ./tests/

WORKDIR /repo/build

COPY ./build/build.csproj .

RUN dotnet restore

COPY ./build .

WORKDIR /repo
