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
    const int TenMinutes = 10 * 60 * 60 * 1000;
    Parallel.ForEach(Projects, project => 
    {
        using (var process = TestAssembly($"{project}.Tests")) 
        {
            process.WaitForExit(TenMinutes);

            Console.Out.WriteLine(string.Join(Environment.NewLine, process.GetStandardOutput()));
            Console.Error.WriteLine(string.Join(Environment.NewLine, process.GetStandardError()));
        }
    });
});

Task("DotNetPack")
    .IsDependentOn("Build")
    .Does(() =>
{
    var versionSuffix = "build" + buildNumber.ToString().PadLeft(5, '0');

    var dotNetCorePackSettings = new DotNetCorePackSettings
    {
        OutputDirectory = artifactsDir,
        NoBuild = true,
        Configuration = configuration,
        VersionSuffix = versionSuffix
    };

    Parallel.ForEach(Projects, project => DotNetCorePack($"./src/{project}", dotNetCorePackSettings));
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
            WorkingDirectory = sourceDir + Directory(name),
            RedirectStandardError = true,
            RedirectStandardOutput = true
        });

string XUnitArguments(string name) {
    var args = $"xunit -parallel all -configuration {configuration} -nobuild";
    if (BuildSystem.IsRunningOnTeamCity) {
        args += $" -xml {artifactsDir + File($"{name}.xml")}";
    }
    return args;
}

string[] Projects => new[] {"SqlStreamStore", "SqlStreamStore.MsSql", "SqlStreamStore.Postgres"};
