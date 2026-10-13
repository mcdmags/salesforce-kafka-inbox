namespace SalesforceSync.LoadGenerator;

public sealed class LoadGeneratorOptions
{
    public const string Section = "LoadGenerator";

    public string Topic { get; set; } = "account-changes";

    /// <summary>Account-to-account relationship events (Account_Relationship__c).</summary>
    public string RelationshipTopic { get; set; } = "account-relationship-changes";

    /// <summary>Chance that an account event is followed by a relationship event between it and another account.</summary>
    public double RelationshipProbability { get; set; } = 0.15;

    /// <summary>Distinct accounts. Fewer accounts = more updates per account = more ordering pressure.</summary>
    public int Accounts { get; set; } = 500;

    public int EventsPerSecond { get; set; } = 200;

    /// <summary>Stop after this many events (0 = run until stopped).</summary>
    public long TotalEvents { get; set; } = 20_000;

    /// <summary>Chance that an event is a burst of 2-5 rapid updates to the same account.</summary>
    public double BurstProbability { get; set; } = 0.1;
}
