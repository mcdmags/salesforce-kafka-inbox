namespace SalesforceSync.IntegrationTests;

[Collection(AppHostCollection.Name)]
public sealed class SchemaTests(AppHostFixture app)
{
    [Fact]
    public async Task Schema_can_be_reapplied_to_an_existing_database()
    {
        // The worker already applied it at startup; a restart against a persisted volume applies it again.
        var sql = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "schema.sql"));

        await app.ExecuteSqlAsync(sql);
    }
}
