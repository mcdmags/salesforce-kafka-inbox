using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SalesforceSync.Data.Migrations
{
    /// <inheritdoc />
    public partial class SlotsOnRateLimitRow : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "slots",
                table: "sf_rate_limit",
                type: "jsonb",
                nullable: false,
                defaultValue: "[]");

        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "slots",
                table: "sf_rate_limit");
        }
    }
}
