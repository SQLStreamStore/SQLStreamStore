namespace SqlStreamStore.HAL.Tests
{
    using System;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.DependencyInjection;

    internal class TestStartup : IStartup
    {
        private readonly IStreamStore _streamStore;

        public TestStartup(IStreamStore streamStore)
        {
            _streamStore = streamStore;
        }

        public IServiceProvider ConfigureServices(IServiceCollection services) 
            => services
                .BuildServiceProvider();

        public void Configure(IApplicationBuilder app) => app.UseSqlStreamStoreHal(_streamStore);
    }
}