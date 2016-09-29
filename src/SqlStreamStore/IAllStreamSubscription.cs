namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;

    public interface IAllStreamSubscription : IDisposable
    {
        string Name { get; }

        long? LastPosition { get; }

        Task Started { get; }

        int PageSize { get; set; }
    }
}