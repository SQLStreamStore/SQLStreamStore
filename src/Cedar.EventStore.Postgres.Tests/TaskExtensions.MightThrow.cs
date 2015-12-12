namespace Cedar.EventStore.Postgres.Tests
{
    using FluentAssertions;
    using System;
    using System.Threading.Tasks;

    internal static class TaskExtensions
    {
        internal static async Task MightThrow<T>(this Task task, string message)
        {
            try
            {
                await task;
            }
            catch (Exception ex)
            {
                ex.Should().BeOfType<T>();
                ex.Message.Should().Be(message);

                return;
            }

            //it didn't throw an exception, that's ok too
        }
    }
}
