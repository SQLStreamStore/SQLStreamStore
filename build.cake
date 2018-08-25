#addin "nuget:?package=Cake.FileHelpers&version=3.0.0"

var target          = Argument("target", "Default");
var configuration   = Argument("configuration", "Release");
var artifactsDir    = Directory("./artifacts");
var sourceDir       = Directory("./src");
var solution        = "./src/SqlStreamStore.sln";
var buildNumber     = string.IsNullOrWhiteSpace(EnvironmentVariable("TRAVIS_BUILD_NUMBER")) ? "0" : EnvironmentVariable("TRAVIS_BUILD_NUMBER");

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
    var projects = new [] 
    {
        "SqlStreamStore.Tests",
        "SqlStreamStore.MsSql.Tests",
        "SqlStreamStore.MsSql.V3.Tests",
        "SqlStreamStore.Postgres.Tests",
        "SqlStreamStore.Http.Tests"
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
        "SqlStreamStore.Postgres",
        "SqlStreamStore.Http"
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

Task("PublishPackages")
    .IsDependentOn("DotNetPack")
    .Does(() => 
{
    var apiKey = EnvironmentVariable("MYGET_API_KEY");
    if(string.IsNullOrWhiteSpace(apiKey))
    {
        Information("MyGet API key not available. Packages will not be pushed.");
        return;
    }
    var settings = new DotNetCoreNuGetPushSettings
    {
        ApiKey = EnvironmentVariable("MYGET_API_KEY"),
        Source = "https://www.myget.org/F/sqlstreamstore/api/v3/index.json"
    };
    var files = GetFiles("artifacts/*.nupkg");
    foreach(var file in files)
    {
        Information(file);
        DotNetCoreNuGetPush(file.FullPath, settings);
    }
});

Task("Default")
    .IsDependentOn("RunTests")
    .IsDependentOn("PublishPackages");

RunTarget(target);
