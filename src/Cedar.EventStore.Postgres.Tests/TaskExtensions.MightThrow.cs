namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;

    internal static partial class TaskExtensions
    {
        internal static async Task MightThrow<T>(this Task task, string message)
        {
            try
            {
                await task;
            }
            catch (Exception ex)
            {
                ex.ShouldBeOfType<T>();
                ex.Message.ShouldBe(message);

                return;
            }

            //it didn't throw an exception, that's ok too
        }
    }
}
