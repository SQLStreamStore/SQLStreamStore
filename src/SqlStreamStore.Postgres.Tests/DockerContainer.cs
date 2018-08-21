namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Docker.DotNet;
    using Docker.DotNet.Models;
    using SqlStreamStore.Infrastructure;

    internal class DockerContainer
    {
        private const string UnixPipe = "unix:///var/run/docker.sock";
        private const string WindowsPipe = "npipe://./pipe/docker_engine";

        private static readonly Uri s_DockerUri = new Uri(Environment.OSVersion.IsWindows() ? WindowsPipe : UnixPipe);

        private static readonly DockerClientConfiguration s_dockerClientConfiguration =
            new DockerClientConfiguration(s_DockerUri);

        private readonly int[] _ports;
        private readonly string _image;
        private readonly string _tag;
        private readonly Func<CancellationToken, Task<bool>> _healthCheck;
        private readonly IDockerClient _dockerClient;

        private string ImageWithTag => $"{_image}:{_tag}";

        public string ContainerName { get; set; } = Guid.NewGuid().ToString("n");
        public string[] Env { get; set; } = Array.Empty<string>();

        public DockerContainer(
            string image,
            string tag,
            Func<CancellationToken, Task<bool>> healthCheck,
            params int[] ports)
        {
            _dockerClient = s_dockerClientConfiguration.CreateClient();
            _ports = ports;
            _image = image;
            _tag = tag;
            _healthCheck = healthCheck;
        }

        public async Task TryStart(CancellationToken cancellationToken = default)
        {
            var images = await _dockerClient.Images.ListImagesAsync(new ImagesListParameters
                {
                    MatchName = ImageWithTag
                },
                cancellationToken).NotOnCapturedContext();

            if(images.Count == 0)
            {
                // No image found. Pulling latest ..
                await _dockerClient.Images.CreateImageAsync(new ImagesCreateParameters
                    {
                        FromImage = _image,
                        Tag = _tag
                    },
                    null,
                    IgnoreProgress.Forever,
                    cancellationToken).NotOnCapturedContext();
            }

            var containerId = await FindContainer(cancellationToken).NotOnCapturedContext()
                              ?? await CreateContainer(cancellationToken).NotOnCapturedContext();

            await StartContainer(containerId, cancellationToken);
        }

        private async Task<string> FindContainer(CancellationToken cancellationToken)
        {
            var containers = await _dockerClient.Containers.ListContainersAsync(new ContainersListParameters
                {
                    All = true,
                    Filters = new Dictionary<string, IDictionary<string, bool>>
                    {
                        ["name"] = new Dictionary<string, bool>
                        {
                            [ContainerName] = true
                        }
                    }
                },
                cancellationToken).NotOnCapturedContext();

            return containers.Select(x => x.ID).FirstOrDefault();
        }

        private async Task<string> CreateContainer(CancellationToken cancellationToken)
        {
            var createContainerParameters = new CreateContainerParameters
            {
                Image = ImageWithTag,
                Name = ContainerName,
                Tty = true,
                Env = Env,
                HostConfig = new HostConfig
                {
                    PortBindings = _ports.ToDictionary(
                        port => $"{port}/tcp",
                        port => (IList<PortBinding>) new List<PortBinding>
                        {
                            new PortBinding
                            {
                                HostPort = port.ToString()
                            }
                        })
                }
            };

            var container = await _dockerClient.Containers.CreateContainerAsync(
                createContainerParameters,
                cancellationToken).NotOnCapturedContext();

            return container.ID;
        }

        private async Task StartContainer(string containerId, CancellationToken cancellationToken)
        {
            // Starting the container ...
            var started = await _dockerClient.Containers.StartContainerAsync(
                containerId,
                new ContainerStartParameters(),
                cancellationToken).NotOnCapturedContext();

            if(started)
            {
                while(!await _healthCheck(cancellationToken).NotOnCapturedContext())
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken).NotOnCapturedContext();
                }
            }
        }

        private class IgnoreProgress : IProgress<JSONMessage>
        {
            public static readonly IProgress<JSONMessage> Forever = new IgnoreProgress();

            public void Report(JSONMessage value)
            { }
        }
    }
}