#tool "nuget:?package=xunit.runner.console&version=2.1.0"
#tool "nuget:?package=ILRepack&Version=2.0.12"

#addin "Cake.FileHelpers"
#addin "Cake.Json"

var target          = Argument("target", "Merge");
var configuration   = Argument("configuration", "Release");
var artifactsDir    = Directory("./artifacts");
var solution        = "./SqlStreamStore.sln";
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
	DotNetCoreRestore("./");
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

	var projects = GetFiles("./**/project.json");
	foreach(var project in projects)
	{
		Information(project.GetDirectory().FullPath);
		if(project.GetDirectory().FullPath == @"C:/Development/SqlStreamStore/src/Example")
			continue;
		DotNetCoreBuild(project.GetDirectory().FullPath, settings);
	}
});

Task("RunTests")
    .IsDependentOn("Build")
    .Does(() =>
{
    //var testProjects = new string[] { "SqlStreamStore.Tests", "SqlStreamStore.MsSql.Tests" };

	var testSettings = new DotNetCoreTestSettings
	{
		NoBuild = true
	};

	var testProjects = new string[] { "SqlStreamStore.Tests" };
    foreach(var testProject in testProjects)
	{
        var projectDir = "./test/"+ testProject;
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

    //assemblies = new [] { "Ensure.That" };
    //merge(assemblies, "SqlStreamStore.MsSql");
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
		Configuration = "Release"
    };

    
	DotNetCorePack("./src/SqlStreamStore", dotNetCorePackSettings);
    //NuGetPack("./src/SqlStreamStore.MsSql/SqlStreamStore.MsSql.nuspec", nuGetPackSettings);
});

Task("Default")
    .IsDependentOn("UpdateAssemblyInfoVersion")
    .IsDependentOn("RunTests")
    .IsDependentOn("NuGetPack");

RunTarget(target);