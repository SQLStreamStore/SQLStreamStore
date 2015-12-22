namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;

    internal static partial class TaskExtensions
    {
        internal static async Task ShouldThrow<T>(this Task task, string message)
        {
            try
            {
                await task;
            }
            catch(Exception ex)
            {
                ex.ShouldBeOfType<T>();
                ex.Message.ShouldBe(message);

                return;
            }
            throw new Exception("Exception not thrown");
        }

        internal static async Task ShouldNotThrow(this Task task)
        {
            try
            {
                await task;
            }
            catch (Exception ex)
            {
                throw new Exception("Exception throw but was not expected", ex);
            }
        }
    }
}