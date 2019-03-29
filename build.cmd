docker build --tag sss-build .
docker run --rm -it --name sss-build ^
 -v /var/run/docker.sock:/var/run/docker.sock ^
 -v %cd%/artifacts:/artifacts ^
 -v %cd%/.git:/.git ^
 --network host ^
 sss-build ^
 dotnet run -p /repo/build/build.csproj -- %*