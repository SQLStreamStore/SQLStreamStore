using System;
using System.IO;
using System.Linq;
using SimpleExec;
using static Bullseye.Targets;
using static SimpleExec.Command;

namespace build
{
    class Program
    {
        private const string ArtifactsDir = "artifacts";
        private const string BuildHalDocs = "build-hal-docs";
        private const string Build = "build";
        private const string Test = "test";
        private const string Pack = "pack";
        private const string Publish = "publish";
        private static bool s_oneOrMoreTestsFailed;

        private static void Main(string[] args)
        {
            Target(BuildHalDocs, () =>
            {
                Run("yarn", workingDirectory: "./tools/hal-docs");

                var srcDirectory = new DirectoryInfo("./src");

                var schemaDirectories = srcDirectory.GetFiles("*.schema.json", SearchOption.AllDirectories)
                    .Select(schemaFile => schemaFile.DirectoryName)
                    .Distinct()
                    .Select(schemaDirectory => schemaDirectory.Replace(Path.DirectorySeparatorChar, '/'));

                foreach (var schemaDirectory in schemaDirectories)
                {
                    Run("node",
                    $"node_modules/@adobe/jsonschema2md/cli.js -n --input {schemaDirectory} --out {schemaDirectory} --schema-out=-",
                    "tools/hal-docs");
                }
            });

            Target(Build, () => Run("dotnet", "build --configuration=Release"));

            Target(
                Test,
                DependsOn(Build),
                ForEach(
                    "SqlStreamStore.Tests",
                    "SqlStreamStore.MsSql.Tests",
                    "SqlStreamStore.MsSql.V3.Tests",
                    "SqlStreamStore.MySql.Tests",
                    "SqlStreamStore.Postgres.Tests",
                    "SqlStreamStore.HAL.Tests",
                    "SqlStreamStore.Http.Tests"), 
                project =>
                {
                    try
                    {
                        Run("dotnet",
                            $"test tests/{project}/{project}.csproj --configuration=Release --no-build --no-restore --verbosity=normal");
                    }
                    catch (NonZeroExitCodeException)
                    {
                        s_oneOrMoreTestsFailed = true;
                    }
                });

            Target(
                Pack,
                DependsOn(Build),
                ForEach(
                    "SqlStreamStore",
                    "SqlStreamStore.MsSql",
                    "SqlStreamStore.MySql",
                    "SqlStreamStore.Postgres",
                    "SqlStreamStore.HAL",
                    "SqlStreamStore.Http"),
                project => Run("dotnet", $"pack src/{project}/{project}.csproj -c Release -o ../../../{ArtifactsDir} --no-build"));

            Target(Publish, DependsOn(Pack), () =>
            {
                var packagesToPush = Directory.GetFiles($"../{ArtifactsDir}", "*.nupkg", SearchOption.TopDirectoryOnly);
                Console.WriteLine($"Found packages to publish: {string.Join("; ", packagesToPush)}");

                var apiKey = Environment.GetEnvironmentVariable("FEEDZ_SSS_API_KEY");

                if (string.IsNullOrWhiteSpace(apiKey))
                {
                    Console.WriteLine("Feedz API key not available. Packages will not be pushed.");
                    return;
                }

                foreach (var packageToPush in packagesToPush)
                {
                    Run("dotnet", $"nuget push {packageToPush} -s https://f.feedz.io/streamstore/ci/nuget/index.json -k {apiKey} --skip-duplicate", noEcho: true);
                }
            });

            Target("default",
                DependsOn(Test, Publish),
                () =>
                {
                    if (s_oneOrMoreTestsFailed)
                    {
                        throw new Exception("One or more tests failed.");
                    }
                });

            RunTargetsAndExit(args);
        }
    }
}
