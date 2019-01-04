using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using static Bullseye.Targets;
using static SimpleExec.Command;

namespace build
{
    class Program
    {
        private const string ArtifactsDir = "artifacts";
        private const string BuildHalDocs = "build-hal-docs";
        private const string Build = "build";
        private const string RunTests = "run-tests";
        private const string Pack = "pack";
        private const string Publish = "publish";

        private static void Main(string[] args)
        {
            Target(BuildHalDocs, () =>
            {
                RunCmd("yarn", workingDirectory: "./tools/hal-docs");

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

            Target(Build, () => Run("dotnet", "build SqlStreamStore.sln -c Release"));

            Target(
                RunTests,
                DependsOn(Build),
                project => Run("dotnet", $"test -c Release -r ../../{ArtifactsDir} -l trx;LogFileName={project}.xml --verbosity=normal"));

            Target(
                Pack,
                DependsOn(Build),
                ForEach(
                    "SqlStreamStore",
                    "SqlStreamStore.MsSql",
                    "SqlStreamStore.Postgres",
                    "SqlStreamStore.HAL",
                    "SqlStreamStore.Http"),
                project => Run("dotnet", $"pack src/{project}/{project}.csproj -c Release -o ../../{ArtifactsDir} --no-build"));

            Target(Publish, DependsOn(Pack), () =>
            {
                var packagesToPush = Directory.GetFiles(ArtifactsDir, "*.nupkg", SearchOption.TopDirectoryOnly);
                Console.WriteLine($"Found packages to publish: {string.Join("; ", packagesToPush)}");

                var apiKey = Environment.GetEnvironmentVariable("MYGET_API_KEY");

                if (string.IsNullOrWhiteSpace(apiKey))
                {
                    Console.WriteLine("MyGet API key not available. Packages will not be pushed.");
                    return;
                }

                foreach (var packageToPush in packagesToPush)
                {
                    Run("dotnet", $"nuget push {packageToPush} -s https://www.myget.org/F/sqlstreamstore/api/v3/index.json -k {apiKey}", noEcho: true);
                }
            });

            Target("default", DependsOn(RunTests, Publish));

            RunTargetsAndExit(args);
        }

        private static void RunCmd(string name, string args = null, string workingDirectory = null, bool noEcho = false)
        {
            try
            {
                Run(name, args, workingDirectory, noEcho);
            }
            catch (Win32Exception ex) when (ex.NativeErrorCode == 2) // The system cannot find the file specified.
            {
                var cmdArgs = $"/C {name}";
                if (args != null)
                {
                    cmdArgs += " " + args;
                }
                Run("cmd", cmdArgs, workingDirectory, noEcho);
            }
        }
    }
}
