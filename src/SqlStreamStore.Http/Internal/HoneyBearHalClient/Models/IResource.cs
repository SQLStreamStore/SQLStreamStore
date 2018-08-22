namespace SqlStreamStore.Internal.HoneyBearHalClient.Models
{
    using System;
    using System.Collections.Generic;

    internal interface IResource : INode, IEnumerable<KeyValuePair<string, object>>
    {
        IList<ILink> Links { get; }

        IList<IResource> Embedded { get; }

        Uri BaseAddress { get; }

        IResource WithBaseAddress(Uri baseAddress);
    }
}