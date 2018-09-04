﻿using System;
using System.IO;
using static Bullseye.Targets;
using static SimpleExec.Command;

namespace build
{
    class Program
    {
        private const string ArtifactsDir = "artifacts";
        private const string Clean = "clean";
        private const string Build = "build";
        private const string RunTests = "run-tests";
        private const string Pack = "pack";
        private const string Publish = "publish";

        static void Main(string[] args)
        {
            var travisBuildNumber = Environment.GetEnvironmentVariable("TRAVIS_BUILD_NUMBER");
            var buildNumber = travisBuildNumber ?? "0";
            var versionSuffix = "build" + buildNumber.PadLeft(5, '0');
            var apiKey = Environment.GetEnvironmentVariable("MYGET_API_KEY");

            var packages = new[]
            {
                "SqlStreamStore",
                "SqlStreamStore.MsSql",
                "SqlStreamStore.Postgres",
                "SqlStreamStore.Http",
            };

            Target(Clean, () =>
            {
                if (Directory.Exists(ArtifactsDir))
                {
                    Directory.Delete(ArtifactsDir, true);
                }
            });

            Target(Build, () => Run("dotnet", "build ./src/SqlStreamStore.sln -c Release"));

            Target(
                RunTests,
                DependsOn(Build),
                ForEach(
                    "SqlStreamStore.Tests",
                    "SqlStreamStore.MsSql.Tests",
                    "SqlStreamStore.MsSql.V3.Tests",
                    "SqlStreamStore.Postgres.Tests",
                    "SqlStreamStore.Http.Tests"),
                project => Run("dotnet", $"test src/{project}/{project}.csproj -c Release -r ../../{ArtifactsDir} --no-build -l trx;LogFileName={project}.xml"));

            Target(
                Pack,
                DependsOn(Build),
                ForEach(packages),
                project => Run("dotnet", $"pack src/{project}/{project}.csproj -c Release -o ../../{ArtifactsDir} --no-build --version-suffix {versionSuffix}"));

            Target(Publish, DependsOn(Pack), ForEach(packages), file =>
            {
                if (string.IsNullOrWhiteSpace(apiKey))
                {
                    Console.WriteLine("MyGet API key not available. Package will not be pushed.");
                }
                else
                {
                    Run("dotnet", $"nuget push {file} -s https://www.myget.org/F/sqlstreamstore/api/v3/index.json -k {apiKey}");
                }
            });

            Target("default", DependsOn(Clean, RunTests, Publish));

            RunTargets(args);
        }
    }
}
