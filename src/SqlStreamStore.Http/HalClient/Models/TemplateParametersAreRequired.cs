namespace SqlStreamStore.HalClient.Models
{
    using System;

    internal sealed class TemplateParametersAreRequired : Exception
    {
        public TemplateParametersAreRequired(ILink link)
            : base($"Template parameters are required for link={link}.")
        {

        }
    }
}