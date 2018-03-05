namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Docker.DotNet;
    using Docker.DotNet.Models;

    internal class DockerContainer
    {
        private const string UnixPipe = "unix:///var/run/docker.sock";
        private const string WindowsPipe = "npipe://./pipe/docker_engine";

        private static readonly Uri s_DockerUri = new Uri(Environment.OSVersion.IsWindows() ? WindowsPipe : UnixPipe);

        private readonly int[] _ports;
        private readonly string _image;
        private readonly string _tag;

        public string ContainerName { get; set; }
        public string[] Env { get; set; }

        public DockerContainer(
            string image,
            string tag,
            params int[] ports)
        {
            _ports = ports;
            _image = image;
            _tag = tag;
        }

        public async Task TryStart()
        {
            var config = new DockerClientConfiguration(s_DockerUri);

            var client = config.CreateClient();

            var images = await client.Images.ListImagesAsync(new ImagesListParameters
            {
                MatchName = _image
            });

            if(images.Count == 0)
            {
                // No image found. Pulling latest ..
                await client.Images.CreateImageAsync(new ImagesCreateParameters
                    {
                        FromImage = _image,
                        Tag = _tag
                    },
                    null,
                    IgnoreProgress.Forever);
            }

            var containers = await client.Containers.ListContainersAsync(new ContainersListParameters {All = true});

            if(containers.Any(container => container.Image == _image && container.Names.Any(name => name == $"/{ContainerName}")))
            {
                return;
            }

            await CreateContainer(client);
        }

        private async Task CreateContainer(IDockerClient client)
        {
            try
            {
                var container = await client.Containers.CreateContainerAsync(
                    new CreateContainerParameters
                    {
                        Image = _image,
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
                    });

                // Starting the container ...
                var started = await client.Containers.StartContainerAsync(ContainerName, new ContainerStartParameters());

                if(started)
                {
                    for(;;)
                    {
                        var result = await client.Containers.ListContainersAsync(new ContainersListParameters
                        {
                            All = true,
                            Filters = new Dictionary<string, IDictionary<string, bool>>
                            {
                                ["id"] = new Dictionary<string, bool>
                                {
                                    [container.ID] = true
                                },
                                ["health"] = new Dictionary<string, bool>
                                {
                                    ["healthy"] = true
                                }
                            }
                        });
                        if(result.Any())
                        {
                            return;
                        }
                    }
                }
            }
            catch(DockerApiException ex)
                when(ex.StatusCode == HttpStatusCode.BadRequest)
            {
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