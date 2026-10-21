namespace SalesforceSync.LoadGenerator;

public sealed class LoadGeneratorOptions
{
    public const string Section = "LoadGenerator";

    /// <summary>The one topic: account aggregates, the Account with its relationships and contacts.</summary>
    public string Topic { get; set; } = "account-changes";

    /// <summary>
    /// A real event of yours, the base of every published aggregate (<see cref="AggregateTemplate"/>). Empty:
    /// templates/account-event.json, which the build copies next to the program. A missing file, or one that doesn't fit
    /// the sync's event classes, stops startup with the problems listed.
    /// </summary>
    public string? Template { get; set; }

    public string TemplatePath => Template is { Length: > 0 } path ? path : AggregateTemplate.DefaultPath;

    /// <summary>Chance that an account event also touches one of its relationships (Account_Relationship__c) to another account.</summary>
    public double RelationshipProbability { get; set; } = 0.15;

    /// <summary>Chance that an account event also touches one of its contacts (up to 3 per account).</summary>
    public double ContactProbability { get; set; } = 0.15;

    /// <summary>Chance that touching a relationship or contact the account has removes it (a delete), rather than changing it.</summary>
    public double ChildRemovalProbability { get; set; } = 0.2;

    /// <summary>Distinct accounts. Fewer accounts = more updates per account = more ordering pressure.</summary>
    public int Accounts { get; set; } = 500;

    public int EventsPerSecond { get; set; } = 200;

    /// <summary>Stop after this many events (0 = run until stopped).</summary>
    public long TotalEvents { get; set; } = 20_000;

    /// <summary>Chance that an event is a burst of 2-5 rapid updates to the same account.</summary>
    public double BurstProbability { get; set; } = 0.1;
}