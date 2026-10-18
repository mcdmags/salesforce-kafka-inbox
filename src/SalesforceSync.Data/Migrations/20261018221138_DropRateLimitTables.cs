using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SalesforceSync.Data.Migrations;

/// <inheritdoc />
public partial class DropRateLimitTables : Migration
{
    /// <inheritdoc />
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // The rate limiter lives in each process now: Salesforce has no per-second limit and reports the daily one in
        // every response, and the pods' workers are kept under the concurrent limit by configuration
        migrationBuilder.Sql("drop view sf_limit_status;");

        migrationBuilder.DropTable(
            name: "sf_rate_limit");
    }

    /// <inheritdoc />
    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "sf_rate_limit",
            columns: table => new
            {
                name = table.Column<string>(type: "text", nullable: false),
                api_max = table.Column<long>(type: "bigint", nullable: true),
                api_used = table.Column<long>(type: "bigint", nullable: true),
                app_max = table.Column<long>(type: "bigint", nullable: true),
                app_name = table.Column<string>(type: "text", nullable: true),
                app_used = table.Column<long>(type: "bigint", nullable: true),
                base_refill_per_sec = table.Column<double>(type: "double precision", nullable: false),
                burst_max = table.Column<long>(type: "bigint", nullable: true),
                burst_used = table.Column<long>(type: "bigint", nullable: true),
                capacity = table.Column<double>(type: "double precision", nullable: false),
                paused_until = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                refill_per_sec = table.Column<double>(type: "double precision", nullable: false),
                slots = table.Column<string>(type: "jsonb", nullable: false, defaultValue: "[]"),
                tokens = table.Column<double>(type: "double precision", nullable: false),
                updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "clock_timestamp()"),
                usage_updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("sf_rate_limit_pkey", x => x.name);
            });

        migrationBuilder.InsertData(
            table: "sf_rate_limit",
            columns: ["name", "base_refill_per_sec", "capacity", "refill_per_sec", "tokens"],
            values: ["salesforce", 2.0, 5.0, 2.0, 5.0]);

        migrationBuilder.Sql("""
            create view sf_limit_status as
            select api_used, api_max, round(100.0 * api_used / nullif(api_max, 0), 1)     as api_pct,
                   burst_used, burst_max, round(100.0 * burst_used / nullif(burst_max, 0), 1) as burst_pct,
                   app_name, app_used, app_max, round(100.0 * app_used / nullif(app_max, 0), 1) as app_pct,
                   refill_per_sec, base_refill_per_sec, paused_until, usage_updated_at
            from sf_rate_limit
            where name = 'salesforce';
            """);
    }
}
