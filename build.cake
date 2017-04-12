#tool "nuget:?package=xunit.runner.console&version=2.1.0"
#tool "nuget:?package=ILRepack&Version=2.0.12"

#addin "Cake.FileHelpers"

var target          = Argument("target", "Default");
var configuration   = Argument("configuration", "Release");
var artifactsDir    = Directory("./artifacts");
var solution        = "./src/SqlStreamStore.sln";
var buildNumber     = string.IsNullOrWhiteSpace(EnvironmentVariable("BUILD_NUMBER")) ? "0" : EnvironmentVariable("BUILD_NUMBER");
var version         = FileReadText("version.txt");

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
		ArgumentCustomization = args => args.Append("/p:Version=" + version + ";FileVersion=" + version),
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

/*
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
*/

Task("NuGetPack")
    .IsDependentOn("Build")
    .Does(() =>
{
	var packageVersion = version + "-build" + buildNumber.ToString().PadLeft(5, '0');
    Information(packageVersion);

    var dotNetCorePackSettings   = new DotNetCorePackSettings
	{
		ArgumentCustomization = args => args.Append("/p:Version=" + packageVersion),
        OutputDirectory = artifactsDir,
		NoBuild = true,
		Configuration = configuration
    };

    
	DotNetCorePack("./src/SqlStreamStore", dotNetCorePackSettings);
	DotNetCorePack("./src/SqlStreamStore.MsSql", dotNetCorePackSettings);
});

Task("Default")
    .IsDependentOn("RunTests")
    .IsDependentOn("NuGetPack");

RunTarget(target);