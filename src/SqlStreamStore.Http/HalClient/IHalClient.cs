namespace SqlStreamStore.HalClient
{
    using System.Collections.Generic;
    using SqlStreamStore.HalClient.Models;

    /// <summary>
    /// A lightweight fluent .NET client for navigating and consuming HAL APIs containing the current state.
    /// </summary>
    internal interface IHalClient : IHalClientBase
    {
        /// <summary>
        /// The most recently navigated resource.
        /// </summary>
        /// 
        IEnumerable<IResource> Current { get; }
    }
}