namespace SalesforceSync.Tests;

/// <summary>
/// <c>[AutoData]</c> for payload and sObject classes. AutoFixture 4 doesn't know <see cref="DateOnly"/> (it would call
/// the constructor with three arbitrary numbers), so dates are taken from its date/times.
/// </summary>
public sealed class TypedAutoDataAttribute() : AutoDataAttribute(() =>
{
    var fixture = new Fixture();
    fixture.Customize<DateOnly>(c => c.FromFactory((DateTime d) => DateOnly.FromDateTime(d)));
    return fixture;
});