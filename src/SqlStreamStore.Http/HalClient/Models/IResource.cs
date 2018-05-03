namespace SqlStreamStore.HalClient.Models
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represents a generic HAL resource.
    /// </summary>
    internal interface IResource : IDictionary<string, object>, INode
    {
        /// <summary>
        /// The list of link relations.
        /// </summary>
        IList<ILink> Links { get; }

        /// <summary>
        /// The list of embedded resources.
        /// </summary>
        IList<IResource> Embedded { get; }
        
        Uri BaseAddress { get; }

        IResource WithBaseAddress(Uri baseAddress);
    }

    /// <summary>
    /// Represents a type-specific HAL resource.
    /// </summary>
    /// <typeparam name="T">The type of the resource.</typeparam>
    internal interface IResource<out T> : IResource
        where T : class, new()
    {
        /// <summary>
        /// The content of the resource.
        /// </summary>
        T Data { get; }
    }
}