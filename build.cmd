docker build --tag sss-build . && docker run --rm --name sss-build -v /var/run/docker.sock:/var/run/docker.sock --network host sss-build dotnet run -p /build/build.csproj -- --parallel %*

