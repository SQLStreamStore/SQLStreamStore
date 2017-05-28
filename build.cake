#tool "nuget:?package=xunit.runner.console&version=2.1.0"
#tool "nuget:?package=ILRepack&Version=2.0.12"

#addin "Cake.FileHelpers"

var target          = Argument("target", "Default");
var configuration   = Argument("configuration", "Release");
var artifactsDir    = Directory("./artifacts");
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
	NuGetRestore(solution);
});

Task("Build")
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
    .IsDependentOn("Build")
    .Does(() =>
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
});

Task("NuGetPack")
    .IsDependentOn("Build")
    .Does(() =>
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
});

Task("Default")
    .IsDependentOn("RunTests")
    .IsDependentOn("NuGetPack");

RunTarget(target);