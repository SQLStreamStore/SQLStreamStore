namespace SqlStreamStore.Internal.HoneyBearHalClient.Models
{
    using System;

    internal sealed class FailedToResolveRelationship : Exception
    {
        public FailedToResolveRelationship(string relationship)
            : base($"Failed to resolve relationship:{relationship}")
        {

        }
    }
}