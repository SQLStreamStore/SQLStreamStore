#addin "Cake.FileHelpers"
#addin "Cake.Docker"

var target          = Argument("target", "Default");
var configuration   = Argument("configuration", "Release");
var artifactsDir    = Directory("./artifacts");
var sourceDir       = Directory("./src");
var solution        = "./src/SqlStreamStore.sln";
var buildNumber     = string.IsNullOrWhiteSpace(EnvironmentVariable("BUILD_NUMBER")) ? "0" : EnvironmentVariable("BUILD_NUMBER");

var imageName = "microsoft/mssql-server-linux";

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
});

Task("Build")
    .IsDependentOn("RestorePackages")
    .Does(() =>
{
    var settings = new DotNetCoreBuildSettings
    {
        Configuration = configuration
    };

    if (Context.Environment.Platform.IsUnix())
    {
        Warning("Selectively building projects on Unix systems.");

        DotNetCoreBuild(projectSqlStreamStore, settings);
        DotNetCoreBuild(projectSqlStreamStoreMsSql, settings);
        DotNetCoreBuild(projectSqlStreamStoreTests, settings);
        DotNetCoreBuild(projectSqlStreamStoreMsSqlTests, settings);
    }
    else
    {
        DotNetCoreBuild(solution, settings);
    }
});

Task("RunTests")
    .IsDependentOn("Build")
    .Does(() =>
{
    var containerName = "sss-mssql-server";
    if (Context.Environment.Platform.IsUnix())
    {
        Information("Starting the Microsoft SQL Server Docker Instance.");
        DockerComposeUp(new DockerComposeUpSettings{
            DetachedMode = true
        });
        Information("Waiting for the Microsoft SQL Server Docker Instance and SQL to become ready.");
        System.Threading.Thread.Sleep(2000);
    }

    var testProjects = new string[] { "SqlStreamStore.Tests", "SqlStreamStore.MsSql.Tests" };

    var processes = testProjects.Select(TestAssembly).ToArray();

    foreach (var process in processes) {
        using (process) {
            process.WaitForExit();
        }
    }

    if (Context.Environment.Platform.IsUnix())
    {
        Information("Stopping the SQL Server Docker Instance.");
        DockerComposeStop();
    }
});

Task("DotNetPack")
    .IsDependentOn("Build")
    .Does(() =>
{
    var versionSuffix = "build" + buildNumber.ToString().PadLeft(5, '0');

    var dotNetCorePackSettings   = new DotNetCorePackSettings
    {
        OutputDirectory = artifactsDir,
		NoBuild = true,
		Configuration = configuration,
        VersionSuffix = versionSuffix
    };
    
	DotNetCorePack("./src/SqlStreamStore", dotNetCorePackSettings);
	DotNetCorePack("./src/SqlStreamStore.MsSql", dotNetCorePackSettings);
});

Task("Default")
    .IsDependentOn("RunTests")
    .IsDependentOn("DotNetPack");

RunTarget(target);

IProcess TestAssembly(string name)
    => StartAndReturnProcess(
        "dotnet",
        new ProcessSettings {
            Arguments = $"xunit -quiet -parallel all -configuration {configuration} -nobuild",
            WorkingDirectory = sourceDir + Directory(name)
        });
