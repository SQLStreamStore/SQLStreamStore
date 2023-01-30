# Generates the HAL Json schema mark down documents that are included in SqlStreamStore.HAL as embedded resources.
# The logic is in the build script. Requires node and yarn to be installed.

pushd
cd ../../
dotnet run --project build/build.csproj -- build-hal-docs
popd