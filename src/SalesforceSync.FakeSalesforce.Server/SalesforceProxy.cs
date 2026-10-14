using Microsoft.AspNetCore.Http.Extensions;

namespace SalesforceSync.FakeSalesforce.Server;

/// <summary>Translates an incoming HTTP request into a call on <see cref="FakeSalesforceHandler"/> and copies the response back.</summary>
public static class SalesforceProxy
{
    private static readonly HashSet<string> ForwardedHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "If-Modified-Since", "If-Unmodified-Since", "If-Match", "If-None-Match", "Authorization", "User-Agent"
    };

    public static async Task HandleAsync(HttpContext ctx)
    {
        var invoker = ctx.RequestServices.GetRequiredService<HttpMessageInvoker>();
        if (ctx.Request.Path.Value?.EndsWith("/composite/graph", StringComparison.OrdinalIgnoreCase) == true)
            ctx.RequestServices.GetRequiredService<ClientLog>().RecordGraphRequest(ctx.Request.Headers.UserAgent);

        using var request = new HttpRequestMessage(new HttpMethod(ctx.Request.Method), ctx.Request.GetEncodedUrl());
        foreach (var (name, values) in ctx.Request.Headers.Where(h => ForwardedHeaders.Contains(h.Key)))
            request.Headers.TryAddWithoutValidation(name, values.ToArray());

        // Request decompression (UseRequestDecompression) drops Content-Length, so go by method
        if (!HttpMethods.IsGet(ctx.Request.Method) && !HttpMethods.IsHead(ctx.Request.Method) && !HttpMethods.IsDelete(ctx.Request.Method))
        {
            request.Content = new StreamContent(ctx.Request.Body);
            if (ctx.Request.ContentType is { } contentType)
                request.Content.Headers.TryAddWithoutValidation("Content-Type", contentType);
        }

        HttpResponseMessage response;
        try
        {
            response = await invoker.SendAsync(request, ctx.RequestAborted);
        }
        catch (FakeResponseLostException)
        {
            // The org applied the request; the client sees its connection drop without an answer
            ctx.Abort();
            return;
        }
        using var _ = response;

        ctx.Response.StatusCode = (int)response.StatusCode;
        foreach (var (name, values) in response.Headers.Concat(response.Content.Headers))
            ctx.Response.Headers[name] = values.ToArray();
        ctx.Response.Headers.Remove("Transfer-Encoding");
        ctx.Response.Headers.Remove("Content-Length"); // the response may be compressed on the way out

        await response.Content.CopyToAsync(ctx.Response.Body, ctx.RequestAborted);
    }
}
