namespace SalesforceSync.Salesforce;

/// <summary>A value Salesforce would reject; the code is Salesforce's own error code for it.</summary>
public readonly record struct FieldError(string Field, string Code, string Message)
{
    public override string ToString() => $"{Field}: {Code} {Message}";
}
