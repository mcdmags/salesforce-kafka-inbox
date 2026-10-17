using System.Text.RegularExpressions;

namespace SalesforceSync.Infrastructure.RateLimiting;

public sealed record LimitCounter(long Used, long Max)
{
    public double Ratio => Max <= 0 ? 0 : (double)Used / Max;
    public override string ToString()
    {
        return $"{Used}/{Max}";
    }
}

/// <summary>
/// Parsed <c>Sforce-Limit-Info</c> response header, e.g.
/// <c>api-usage=10018/100000; api-bursts=1/750; per-app-api-usage=17/250(appName=my-app)</c>.
/// The REST API Developer Guide (Limit Info Header) defines only api-usage: the org's calls used and its limit per
/// 24 hours. per-app-api-usage is the connected app's quota, documented in earlier versions as a pilot and sent only
/// when the app has one. api-bursts appears in the guide's example but is not defined, so it is recorded for
/// sf_limit_status and never throttled on. Unknown entries are ignored, so new Salesforce counters don't break parsing.
/// </summary>
public sealed partial record SalesforceLimitInfo(
    LimitCounter? ApiUsage,
    LimitCounter? ApiBursts,
    LimitCounter? PerAppUsage,
    string? AppName)
{
    [GeneratedRegex(@"(?<name>[A-Za-z][\w-]*)\s*=\s*(?<used>\d+)\s*/\s*(?<max>\d+)(?:\s*\(\s*appName\s*=\s*(?<app>[^)]*)\))?")]
    private static partial Regex Entry();

    /// <summary>The tighter of the org-wide and per-connected-app daily allocations.</summary>
    public double DailyRatio => Math.Max(ApiUsage?.Ratio ?? 0, PerAppUsage?.Ratio ?? 0);

    public static SalesforceLimitInfo? Parse(string? header)
    {
        if (string.IsNullOrWhiteSpace(header))
        {
            return null;
        }

        LimitCounter? usage = null, bursts = null, perApp = null;
        string? app = null;

        foreach (Match m in Entry().Matches(header))
        {
            var counter = new LimitCounter(long.Parse(m.Groups["used"].Value), long.Parse(m.Groups["max"].Value));
            switch (m.Groups["name"].Value.ToLowerInvariant())
            {
                case "api-usage": usage = counter; break;
                case "api-bursts": bursts = counter; break;
                case "per-app-api-usage":
                    perApp = counter;
                    app = m.Groups["app"].Success ? m.Groups["app"].Value.Trim() : null;
                    break;
            }
        }

        return usage is null && bursts is null && perApp is null
            ? null
            : new SalesforceLimitInfo(usage, bursts, perApp, app);
    }
}