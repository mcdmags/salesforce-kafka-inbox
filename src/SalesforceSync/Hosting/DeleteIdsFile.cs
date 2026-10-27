using System.Text.Json;
using SalesforceSync.Infrastructure.Salesforce;

namespace SalesforceSync.Hosting;

/// <summary>
/// Ids to hard-delete, as JSON: the object, the query that listed them and each record's Id with its external id (to
/// tell which is which). <see cref="ListDeleteIdsCommand"/> writes one per object; remove the records to keep and pass
/// the file to <see cref="BulkDeleteCommand"/>.
/// </summary>
public sealed record DeleteIdsFile(string Object, string Query, List<DeleteIdsFile.Row> Records)
{
    public sealed record Row(string Id, string? ExternalId);

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    /// <exception cref="JsonException">Not such a file.</exception>
    public static DeleteIdsFile Read(string path)
    {
        using var stream = File.OpenRead(path);
        var file = JsonSerializer.Deserialize<DeleteIdsFile>(stream, Json);
        return file is { Object.Length: > 0, Records: not null }
            ? file
            : throw new JsonException($"{path} has no \"object\" or no \"records\"");
    }

    public void Write(string path)
    {
        using var stream = File.Create(path);
        JsonSerializer.Serialize(stream, this, Json);
    }
}

/// <summary>What the Salesforce commands share: the worker's Salesforce client, and Ctrl+C as a cancellation.</summary>
internal static class OrgCommand
{
    /// <summary>A host with the worker's Salesforce settings and client (appsettings, user secrets, <c>Salesforce__…</c>).</summary>
    public static IHost Host()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.AddSalesforceOrgClient();
        return builder.Build();
    }

    public static ISalesforceRestClient Client(this IHost host)
    {
        return host.Services.GetRequiredService<ISalesforceRestClient>();
    }

    /// <summary>Cancelled by Ctrl+C, which then doesn't kill the process.</summary>
    public static CancellationTokenSource CancelKey()
    {
        var cancel = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cancel.Cancel();
        };
        return cancel;
    }
}
