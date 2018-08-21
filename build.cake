#addin "nuget:?package=Cake.FileHelpers&version=3.0.0"

var target          = Argument("target", "Default");
var configuration   = Argument("configuration", "Release");
var artifactsDir    = Directory("./artifacts");
var sourceDir       = Directory("./src");
var solution        = "./src/SqlStreamStore.sln";
var buildNumber     = string.IsNullOrWhiteSpace(EnvironmentVariable("BUILD_NUMBER")) ? "0" : EnvironmentVariable("BUILD_NUMBER");

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
});

Task("Compile")
    .IsDependentOn("RestorePackages")
    .Does(() =>
{
    var settings = new DotNetCoreBuildSettings
    {
        Configuration = configuration
    };

    DotNetCoreBuild(solution, settings);
});

Task("RunTests")
    .IsDependentOn("Compile")
    .Does(() =>
{
    var projects = new string[] 
    {
        "SqlStreamStore.Tests",
        "SqlStreamStore.MsSql.Tests",
        "SqlStreamStore.MsSql.V3.Tests",
        "SqlStreamStore.Postgres.Tests",
    };

    Parallel.ForEach(projects, project => 
    {
        var projectDir = $"./src/{project}/";
        var projectFile = $"{project}.csproj";
        var settings = new DotNetCoreTestSettings
        {
            Configuration = configuration,
            WorkingDirectory = projectDir,
            NoBuild = true,
            NoRestore = true,
            ResultsDirectory = artifactsDir,
            Logger = $"trx;LogFileName={project}.xml"
        };
        DotNetCoreTest(projectFile, settings);
    });
});

Task("DotNetPack")
    .IsDependentOn("Compile")
    .Does(() =>
{
    var versionSuffix = "build" + buildNumber.ToString().PadLeft(5, '0');
    var projects = new string[]
    {
        "SqlStreamStore",
        "SqlStreamStore.MsSql",
        "SqlStreamStore.Postgres"
    };
    var settings = new DotNetCorePackSettings
    {
        OutputDirectory = artifactsDir,
        NoBuild = true,
        Configuration = configuration,
        VersionSuffix = versionSuffix,
    };
    foreach(var project in projects)
    {
        DotNetCorePack($"./src/{project}", settings);
    }
});

Task("PublishPackage")
    .IsDependentOn("DotNetPack")
    .Does(() => 
{
});

Task("Default")
    .IsDependentOn("RunTests")
    .IsDependentOn("DotNetPack");

RunTarget(target);