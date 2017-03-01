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

Task("Restore-NuGet-Packages")
    .IsDependentOn("Clean")
    .Does(() =>
{
    NuGetRestore(solution);
});

Task("Update-Version")
    .Does(() =>
{
    var version = FileReadText("version.txt");
    ReplaceTextInFiles("src/SharedAssemblyInfo.cs", "1.0.0.0", version);
});

Task("Build")
    .IsDependentOn("Restore-NuGet-Packages")
    .Does(() =>
{
    MSBuild(solution, settings => settings.SetConfiguration(configuration));
});

Task("Run-Unit-Tests")
    .IsDependentOn("Build")
    .Does(() =>
{
    var testProjects = new string[] { "SqlStreamStore", "SqlStreamStore.MsSql" };
    foreach(var testProject in testProjects){
        var dll = "./src/"+ testProject +".Tests/bin/" + configuration + "/" + testProject + ".Tests.dll";
        XUnit(
            dll,
            new XUnitSettings 
            { 
                ToolPath = "./tools/xunit.runner.console/tools/xunit.console.exe"
            });
    }
});

Task("Merge")
    .IsDependentOn("Build")
    .Does(() =>
{
    Action<string[], string> merge = (sourceAssemblies, primaryAssemblyName) => {
        var assemblyPaths = sourceAssemblies
            .Select(assembly => GetFiles("./src/" + primaryAssemblyName + "/bin/" + configuration + "/" + assembly + ".dll").Single())
            .ToArray();

        var outputAssembly = artifactsDir.Path + "/" + primaryAssemblyName + ".dll";
        var primaryAssembly = "./src/" + primaryAssemblyName + "/bin/" + configuration + "/" + primaryAssemblyName + ".dll";

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

Task("Nuget-Pack")
    .IsDependentOn("Merge")
    .Does(() =>
{
    var version = FileReadText("version.txt");
    var packageVersion = version + "-build" + buildNumber.ToString().PadLeft(5, '0');
    Information(packageVersion);

    var nuGetPackSettings   = new NuGetPackSettings {
        Version = packageVersion,
        OutputDirectory = artifactsDir
    };

    NuGetPack("./src/SqlStreamStore/SqlStreamStore.nuspec", nuGetPackSettings);
    NuGetPack("./src/SqlStreamStore.MsSql/SqlStreamStore.MsSql.nuspec", nuGetPackSettings);
});

Task("Default")
    .IsDependentOn("Update-Version")
    .IsDependentOn("Run-Unit-Tests")
    .IsDependentOn("Nuget-Pack");

RunTarget(target);