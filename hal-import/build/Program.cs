using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using static Bullseye.Targets;
using static SimpleExec.Command;

static class Program
{
    private const string ArtifactsDir = "artifacts";
    private const string PublishDir = "publish";

    private static readonly string MYGET_API_KEY = Environment.GetEnvironmentVariable(nameof(MYGET_API_KEY));

    public static void Main(string[] args)
    {
        const string clean = nameof(Clean);
        const string init = nameof(Init);
        const string generateDocumentation = nameof(GenerateDocumentation);
        const string build = nameof(Build);
        const string runTests = nameof(RunTests);
        const string pack = nameof(Pack);
        const string publish = nameof(Publish);
        const string push = nameof(Push);

        var srcDirectory = new DirectoryInfo("./src");

        Target(
            clean,
            Clean);

        Target(
            init,
            Init);

        Target(
            generateDocumentation,
            DependsOn(init),
            ForEach(SchemaDirectories(srcDirectory)),
            GenerateDocumentation);

        Target(
            build,
            DependsOn(generateDocumentation),
            Build);

        Target(
            runTests,
            DependsOn(build),
            RunTests);

        Target(
            publish,
            DependsOn(build),
            Publish);

        Target(
            pack,
            DependsOn(publish),
            Pack);

        Target(
            push,
            DependsOn(pack),
            Push);

        Target("default", DependsOn(clean, runTests, push));

        RunTargetsAndExit(args.Concat(new[] {"--parallel"}));
    }

    private static readonly Action Init = () => Yarn("./docs");

    private static readonly Action Clean = () =>
    {
        if (Directory.Exists(ArtifactsDir))
        {
            Directory.Delete(ArtifactsDir, true);
        }

        if (Directory.Exists(PublishDir))
        {
            Directory.Delete(PublishDir, true);
        }
    };

    private static readonly Func<string, Task> GenerateDocumentation = schemaDirectory =>
        RunAsync(
            "node",
            $"node_modules/@adobe/jsonschema2md/cli.js -n --input {schemaDirectory} --out {schemaDirectory} --schema-out=-",
            "docs");

    private static readonly Action Build = () => Run(
        "dotnet",
        "build src/SqlStreamStore.HAL.sln --configuration Release");

    private static readonly Action RunTests = () => Run(
        "dotnet",
        $"test src/SqlStreamStore.HAL.Tests --configuration Release --results-directory ../../{ArtifactsDir} --verbosity normal --no-build -l trx;LogFileName=SqlStreamStore.HAL.Tests.xml");

    private static readonly Action Publish = () => Run(
        "dotnet",
        $"publish --configuration=Release --output=../../{PublishDir} --runtime=alpine.3.7-x64 /p:ShowLinkerSizeComparison=true src/SqlStreamStore.HAL.ApplicationServer");

    private static readonly Action Pack = () => Run(
        "dotnet",
        $"pack src/SqlStreamStore.HAL --configuration Release --output ../../{ArtifactsDir} --no-build");

    private static readonly Action Push = () =>
    {
        var packagesToPush = Directory.GetFiles(ArtifactsDir, "*.nupkg", SearchOption.TopDirectoryOnly);
        Console.WriteLine($"Found packages to publish: {string.Join("; ", packagesToPush)}");

        if (string.IsNullOrWhiteSpace(MYGET_API_KEY))
        {
            Console.WriteLine("MyGet API key not available. Packages will not be pushed.");
            return;
        }

        foreach (var packageToPush in packagesToPush)
        {
            Run(
                "dotnet",
                $"nuget push {packageToPush} -s https://www.myget.org/F/sqlstreamstore/api/v3/index.json -k {MYGET_API_KEY}");
        }
    };

    private static void Yarn(string workingDirectory, string args = default)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            Run("cmd", "/c yarn", workingDirectory);
        else
            Run("yarn", args, workingDirectory);
    }

    private static string[] SchemaDirectories(DirectoryInfo srcDirectory)
        => srcDirectory.GetFiles("*.schema.json", SearchOption.AllDirectories)
            .Select(schemaFile => schemaFile.DirectoryName)
            .Distinct()
            .Select(schemaDirectory => schemaDirectory.Replace(Path.DirectorySeparatorChar, '/'))
            .ToArray();
}