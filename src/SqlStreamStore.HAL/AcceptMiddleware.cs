namespace SqlStreamStore.HAL
{
    using System.Linq;
    using Halcyon.HAL;
    using Microsoft.AspNetCore.Builder;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class AcceptMiddleware
    {
        public static IApplicationBuilder UseAccept(this IApplicationBuilder builder, params string[] acceptable)
            => builder.Use(Accept(acceptable));

        private static MidFunc Accept(params string[] acceptable) => (context, next) =>
        {
            var acceptHeaders = context.Request.GetAcceptHeaders();

            return acceptHeaders.Any(
                acceptHeader => acceptHeader == Constants.MediaTypes.Any || acceptable.Contains(acceptHeader))
                ? next()
                : context.WriteResponse(new HalJsonResponse(new HALResponse(new
                    {
                        type = "Not Acceptable",
                        title = "Not Acceptable",
                        detail = $"The target resource only understands {string.Join(", ", acceptable)}."
                    }),
                    406));
        };
    }
}