using Microsoft.AspNetCore.Http.Extensions;

namespace SalesforceSync.FakeSalesforce.Server;

/// <summary>Translates an incoming HTTP request into a call on <see cref="FakeSalesforceHandler"/> and copies the response back.</summary>
public static class SalesforceProxy
{
    public static async Task HandleAsync(HttpContext ctx)
    {
        var invoker = ctx.RequestServices.GetRequiredService<HttpMessageInvoker>();
        if (ctx.Request.Path.Value?.EndsWith("/composite/graph", StringComparison.OrdinalIgnoreCase) == true)
            ctx.RequestServices.GetRequiredService<ClientLog>().RecordGraphRequest(ctx.Request.Headers.UserAgent);

        using var request = new HttpRequestMessage(new HttpMethod(ctx.Request.Method), ctx.Request.GetEncodedUrl());
        if (ctx.Request.ContentLength > 0 || ctx.Request.Headers.TransferEncoding.Count > 0)
        {
            request.Content = new StreamContent(ctx.Request.Body);
            if (ctx.Request.ContentType is { } contentType)
                request.Content.Headers.TryAddWithoutValidation("Content-Type", contentType);
        }

        using var response = await invoker.SendAsync(request, ctx.RequestAborted);

        ctx.Response.StatusCode = (int)response.StatusCode;
        foreach (var (name, values) in response.Headers.Concat(response.Content.Headers))
            ctx.Response.Headers[name] = values.ToArray();
        ctx.Response.Headers.Remove("Transfer-Encoding");

        await response.Content.CopyToAsync(ctx.Response.Body, ctx.RequestAborted);
    }
}
