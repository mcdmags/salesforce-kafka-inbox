using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SalesforceSync.Data.Migrations;

/// <inheritdoc />
public partial class DropApiUsageAndBudget : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // The view reads daily_budget: dropped and recreated without it (see InitialSchema)
        migrationBuilder.Sql("drop view sf_limit_status;");

        migrationBuilder.DropTable(
            name: "sf_api_usage");

        migrationBuilder.DropColumn(
            name: "daily_budget",
            table: "sf_rate_limit");

        migrationBuilder.Sql(LimitStatusView(withBudget: false));
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql("drop view sf_limit_status;");

        migrationBuilder.AddColumn<long>(
            name: "daily_budget",
            table: "sf_rate_limit",
            type: "bigint",
            nullable: true);

        migrationBuilder.CreateTable(
            name: "sf_api_usage",
            columns: table => new
            {
                bucket = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                calls = table.Column<long>(type: "bigint", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("sf_api_usage_pkey", x => x.bucket);
            });

        migrationBuilder.Sql(LimitStatusView(withBudget: true));
    }

    private static string LimitStatusView(bool withBudget)
    {
        return $"""
            create view sf_limit_status as
            select api_used, api_max, round(100.0 * api_used / nullif(api_max, 0), 1)     as api_pct,
                   burst_used, burst_max, round(100.0 * burst_used / nullif(burst_max, 0), 1) as burst_pct,
                   app_name, app_used, app_max, round(100.0 * app_used / nullif(app_max, 0), 1) as app_pct,
                   refill_per_sec, base_refill_per_sec, paused_until, usage_updated_at{(withBudget ? ",\n       daily_budget" : "")}
            from sf_rate_limit
            where name = 'salesforce';
            """;
    }
}
