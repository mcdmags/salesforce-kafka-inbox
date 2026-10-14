namespace SalesforceSync.Salesforce;

/// <summary>An error Salesforce reported for one record (or for a whole subrequest), or one found before sending.</summary>
/// <param name="Fields">The fields Salesforce blames, when it says.</param>
public sealed record SalesforceError(string? ReferenceId, string Code, string? Message, IReadOnlyList<string>? Fields = null)
{
    public override string ToString()
    {
        return ReferenceId is null ? $"{Code} {Message}" : $"{ReferenceId}: {Code} {Message}";
    }
}