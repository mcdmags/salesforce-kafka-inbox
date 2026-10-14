using System.IO.Compression;
using Microsoft.Extensions.Options;

namespace SalesforceSync.Salesforce;

/// <summary>
/// Gzips request bodies of at least <see cref="SalesforceOptions.CompressRequestsLargerThan"/> bytes
/// (Salesforce REST accepts Content-Encoding: gzip). Composite payloads are repetitive JSON and shrink several-fold.
/// Responses are decompressed by the primary handler (AutomaticDecompression).
/// </summary>
public sealed class GzipRequestHandler(IOptions<SalesforceOptions> options) : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        var threshold = options.Value.CompressRequestsLargerThan;
        if (threshold > 0 && request.Content is { } content && content.Headers.ContentEncoding.Count == 0)
        {
            var bytes = await content.ReadAsByteArrayAsync(ct);
            if (bytes.Length >= threshold)
            {
                using var buffer = new MemoryStream();
                await using (var gzip = new GZipStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
                {
                    await gzip.WriteAsync(bytes, ct);
                }

                var compressed = new ByteArrayContent(buffer.ToArray());
                foreach (var header in content.Headers.Where(h => h.Key != "Content-Length"))
                {
                    compressed.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }

                compressed.Headers.ContentEncoding.Add("gzip");
                request.Content = compressed;
            }
            else
            {
                // Already buffered; resend the same bytes
                var plain = new ByteArrayContent(bytes);
                foreach (var header in content.Headers.Where(h => h.Key != "Content-Length"))
                {
                    plain.Headers.TryAddWithoutValidation(header.Key, header.Value);
                }

                request.Content = plain;
            }
            content.Dispose();
        }
        return await base.SendAsync(request, ct);
    }
}