#tool "nuget:?package=xunit.runner.console&version=2.1.0"
#tool "nuget:?package=ILRepack&Version=2.0.12"

#addin "Cake.FileHelpers"

var target          = Argument("target", "Default");
var configuration   = Argument("configuration", "Release");
var artifactsDir    = Directory("./artifacts");
var solution        = "./src/SqlStreamStore.sln";
var buildNumber     = string.IsNullOrWhiteSpace(EnvironmentVariable("BUILD_NUMBER")) ? "0" : EnvironmentVariable("BUILD_NUMBER");

var projectSqlStreamStore = "./src/SqlStreamStore/SqlStreamStore.csproj";
var projectSqlStreamStoreTests = "./src/SqlStreamStore.Tests/SqlStreamStore.Tests.csproj";
var projectSqlStreamStoreMsSql = "./src/SqlStreamStore.MsSql/SqlStreamStore.MsSql.csproj";
var projectSqlStreamStoreMsSqlTests = "./src/SqlStreamStore.MsSql.Tests/SqlStreamStore.MsSql.Tests.csproj";

Task("Clean")
    .Does(() =>
{
    CleanDirectory(artifactsDir);
});

Task("RestorePackages")
    .IsDependentOn("Clean")
    .Does(() =>
{
	DotNetCoreRestore(solution);
	NuGetRestore(solution);
});

Task("Build")
    .IsDependentOn("RestorePackages")
    .Does(() =>
{
    if (Context.Environment.Platform.IsUnix())
    {
        Warning("Full solution building is not possible on Unix based systems. Building individual projects instead.");

        var settingsStandard = new DotNetCoreBuildSettings
        {
            Configuration = configuration,
            Framework = "netstandard1.3"
        };

        DotNetCoreBuild(projectSqlStreamStore, settingsStandard);
        DotNetCoreBuild(projectSqlStreamStoreMsSql, settingsStandard);

        var settingsApp = new DotNetCoreBuildSettings
        {
            Configuration = configuration,
            Framework = "netcoreapp1.0"
        };

        DotNetCoreBuild(projectSqlStreamStoreTests, settingsApp);
        DotNetCoreBuild(projectSqlStreamStoreMsSqlTests, settingsApp);
    }
    else
    {
        var settings = new DotNetCoreBuildSettings
        {
            Configuration = configuration
        };

        DotNetCoreBuild(solution, settings);
    }
});

Task("RunTests")
    .IsDependentOn("Build")
    .Does(() =>
{
    if (Context.Environment.Platform.IsUnix())
    {
        var testProjects = new string[] { "SqlStreamStore.Tests", "SqlStreamStore.MsSql.Tests" };

        foreach(var testProject in testProjects)
        {
            var projectDir = "./src/"+ testProject + "/";
            var settings = new ProcessSettings
            {
                Arguments = "xunit -framework netcoreapp1.0",
                WorkingDirectory = projectDir
            };
            StartProcess("dotnet", settings);
        }

        //Warning("Skipping the SqlStreamStore.MsSql test suite. Running it is not possible on Unix based systems.");
    }
    else
    {
        var testProjects = new string[] { "SqlStreamStore.Tests", "SqlStreamStore.MsSql.Tests" };

        foreach(var testProject in testProjects)
        {
            var projectDir = "./src/"+ testProject + "/";
            var settings = new ProcessSettings
            {
                Arguments = "xunit",
                WorkingDirectory = projectDir
            };
            StartProcess("dotnet", settings);
        }
    }    
});

Task("NuGetPack")
    .IsDependentOn("Build")
    .Does(() =>
{
    if (Context.Environment.Platform.IsUnix())
    {
        Warning("Packaging is not possible on Unix based systems given the lack of support for .NET 4.6.1.");
    }
    else
    {
        var versionSuffix = "build" + buildNumber.ToString().PadLeft(5, '0');

        var dotNetCorePackSettings   = new DotNetCorePackSettings
        {
            ArgumentCustomization = args => args.Append("/p:Version=1.0.0-" + versionSuffix),
            OutputDirectory = artifactsDir,
            NoBuild = true,
            Configuration = configuration,
            VersionSuffix = versionSuffix
        };
        
        DotNetCorePack("./src/SqlStreamStore", dotNetCorePackSettings);
        DotNetCorePack("./src/SqlStreamStore.MsSql", dotNetCorePackSettings);
    }
});

Task("Default")
    .IsDependentOn("RunTests")
    .IsDependentOn("NuGetPack");

RunTarget(target);