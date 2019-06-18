namespace SqlStreamStore.V1
{
    using Shouldly;

    internal static class LinkAssertionExtensions
    {
        public static void ShouldLink(this Resource resource, string rel, string href, string title = null)
            => resource.Links[rel]
                .ShouldHaveSingleItem()
                .ShouldBe(new Link
                {
                    Href = href,
                    Rel = rel,
                    Title = title
                });

        public static void ShouldLink(this Resource resource, Links links)
        {
            var halLinks = links.ToHalLinks();

            foreach(var link in halLinks)
            {
                resource.Links[link.Rel].ShouldContain(new Link
                {
                    Rel = link.Rel,
                    Href = link.Href,
                    Title = link.Title
                });
            }
        }
    }
}