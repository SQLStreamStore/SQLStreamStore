#tool "nuget:?package=xunit.runner.console&version=2.1.0"
#tool "nuget:?package=ILRepack&Version=2.0.12"

#addin "Cake.FileHelpers"
#addin "Cake.Json"

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

Task("UpdateAssemblyInfoVersion")
    .Does(() =>
{
    var version = FileReadText("version.txt");
    ReplaceTextInFiles("src/SharedAssemblyInfo.cs", "1.0.0.0", version);

	var projects = GetFiles("./**/project.json");
	foreach(var project in projects)
	{
		var json = ParseJsonFromFile(project);
		json["version"] = version + "-*";
		SerializeJsonToFile(project, json);
	}
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

	var testSettings = new DotNetCoreTestSettings
	{
		NoBuild = true,
		Configuration = configuration
	};

    var testProjects = new string[] { "SqlStreamStore.Tests", "SqlStreamStore.MsSql.Tests" };

    foreach(var testProject in testProjects)
	{
        var projectDir = "./src/"+ testProject + "/" + testProject + ".csproj";
		DotNetCoreTest(projectDir, testSettings);
    }
});

Task("Merge")
    .IsDependentOn("Build")
    .Does(() =>
{
    Action<string[], string> merge = (sourceAssemblies, primaryAssemblyName) => {
        var assemblyPaths = sourceAssemblies
            .Select(assembly => GetFiles("./src/" + primaryAssemblyName + "/bin/" + configuration + "/net46/" + assembly + ".dll").Single())
            .ToArray();

        var outputAssembly = artifactsDir.Path + "/" + primaryAssemblyName + ".dll";
        var primaryAssembly = "./src/" + primaryAssemblyName + "/bin/" + configuration + "/net46/" + primaryAssemblyName + ".dll";

        ILRepack(outputAssembly, primaryAssembly, assemblyPaths, new ILRepackSettings 
        { 
            Internalize = true,
            Parallel = true
        });
    };

    var assemblies = new [] { "Ensure.That", "Nito.AsyncEx", "Nito.AsyncEx.Concurrent", "Nito.AsyncEx.Enlightenment" };
    merge(assemblies, "SqlStreamStore");

    assemblies = new [] { "Ensure.That" };
    merge(assemblies, "SqlStreamStore.MsSql");
});

Task("NuGetPack")
    .IsDependentOn("Merge")
    .Does(() =>
{
    var version = FileReadText("version.txt");
	var build = "build" + buildNumber.ToString().PadLeft(5, '0');
    Information(version + build);

    var dotNetCorePackSettings   = new DotNetCorePackSettings
	{
		VersionSuffix = build,
        OutputDirectory = artifactsDir,
		NoBuild = true,
		Configuration = configuration
    };

    
	DotNetCorePack("./src/SqlStreamStore", dotNetCorePackSettings);
	DotNetCorePack("./src/SqlStreamStore.MsSql", dotNetCorePackSettings);
});

Task("Default")
    .IsDependentOn("UpdateAssemblyInfoVersion")
    .IsDependentOn("RunTests")
    .IsDependentOn("NuGetPack");

RunTarget(target);