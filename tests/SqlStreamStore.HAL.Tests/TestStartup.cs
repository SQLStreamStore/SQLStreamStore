namespace SqlStreamStore.HAL.Tests
{
    using System;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.DependencyInjection;
    using Xunit.Abstractions;

    internal class TestStartup : IStartup
    {
        private readonly IStreamStore _streamStore;
        private readonly ITestOutputHelper _output;

        public TestStartup(IStreamStore streamStore, ITestOutputHelper output)
        {
            _streamStore = streamStore;
            _output = output;
        }

        public IServiceProvider ConfigureServices(IServiceCollection services) 
            => services
                .AddSqlStreamStoreHal()
                .BuildServiceProvider();

        public void Configure(IApplicationBuilder app) => app
            .Use(async (context, next) =>
            {
                try
                {
                    await next();
                }
                catch(Exception ex)
                {
                    _output.WriteLine(ex.ToString());
                    throw;
                }
            })
            .UseSqlStreamStoreHal(_streamStore);
    }
}