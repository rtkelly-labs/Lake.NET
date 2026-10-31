using Iceberg.Net.TestSupport;

namespace Iceberg.Net.DuckLake.Tests;

/// <summary>The spec's logical conflicts ("Conflict Resolution"), one case per rule, decided on changes_made alone.</summary>
public sealed class ConflictRuleTests
{
    private static IReadOnlyList<DuckLakeChange> Theirs(string changes) => DuckLakeChange.Parse(changes);

    private static ChangeSet Ours(Action<ChangeSet> build)
    {
        var set = new ChangeSet();
        build(set);
        return set;
    }

    [Theory]
    [Spec("ducklake: Schemas")]
    [InlineData("created_schema:\"s\"", true)]   // both create a schema with the same name
    [InlineData("created_schema:\"other\"", false)]
    public void Both_creating_a_schema(string theirs, bool conflict) =>
        Assert.Equal(conflict, Ours(o => o.CreatedSchemas.Add("s")).ConflictWith(9, Theirs(theirs)) is not null);

    [Theory]
    [Spec("ducklake: Schemas")]
    [InlineData("dropped_schema:4", true)]                      // both drop the same schema
    [InlineData("created_table:\"s\".\"t\"", true)]              // they created an entry in the schema we drop
    [InlineData("created_view:\"s\".\"v\"", true)]
    [InlineData("created_table:\"other\".\"t\"", false)]
    public void Dropping_a_schema(string theirs, bool conflict) =>
        Assert.Equal(conflict, Ours(o => o.DroppedSchemas[4] = "s").ConflictWith(9, Theirs(theirs)) is not null);

    [Theory]
    [Spec("ducklake: Tables / Views")]
    [InlineData("created_table:\"s\".\"t\"", true)]   // same name in the same schema
    [InlineData("created_view:\"s\".\"t\"", true)]
    [InlineData("created_table:\"s2\".\"t\"", false)]
    [InlineData("dropped_schema:4", true)]            // our schema was dropped
    [InlineData("dropped_schema:5", false)]
    public void Creating_a_table(string theirs, bool conflict) =>
        Assert.Equal(conflict, Ours(o => o.CreatedTables.Add(("s", 4, "t"))).ConflictWith(9, Theirs(theirs)) is not null);

    [Theory]
    [Spec("ducklake: Tables / Views")]
    [InlineData("dropped_table:7", true)]  // both drop the same table
    [InlineData("dropped_table:8", false)]
    [InlineData("altered_table:7", false)] // the spec lists alter-after-drop, not drop-after-alter
    public void Dropping_a_table(string theirs, bool conflict) =>
        Assert.Equal(conflict, Ours(o => o.DroppedTables.Add(7)).ConflictWith(9, Theirs(theirs)) is not null);

    [Theory]
    [Spec("ducklake: Tables / Views")]
    [InlineData("dropped_table:7", true)]
    [InlineData("altered_table:7", true)]
    [InlineData("inserted_into_table:7", false)]
    public void Altering_a_table(string theirs, bool conflict) =>
        Assert.Equal(conflict, Ours(o => o.AlteredTables.Add(7)).ConflictWith(9, Theirs(theirs)) is not null);

    [Theory]
    [Spec("ducklake: Data")]
    [InlineData("dropped_table:7", true)]
    [InlineData("altered_table:7", true)]
    [InlineData("deleted_from_table:7", true)]
    [InlineData("inlined_delete:7", true)]
    [InlineData("inserted_into_table:7", false)] // concurrent inserts retry without rewriting files
    [InlineData("inlined_insert:7", false)]
    [InlineData("compacted_table:7", false)]
    [InlineData("deleted_from_table:8", false)]
    public void Inserting(string theirs, bool conflict) =>
        Assert.Equal(conflict, Ours(o => o.InsertedTables.Add(7)).ConflictWith(9, Theirs(theirs)) is not null);

    [Theory]
    [Spec("ducklake: Data")]
    [Spec("ducklake: Compaction")]
    [InlineData("dropped_table:7", true)]
    [InlineData("altered_table:7", true)]
    [InlineData("inserted_into_table:7", true)]
    [InlineData("inlined_insert:7", true)]
    [InlineData("compacted_table:7", true)]
    [InlineData("inline_flush:7", true)]
    // Delete vs delete is decided per data file against the catalog (DeleteConflictTests), not here.
    [InlineData("deleted_from_table:7", false)]
    [InlineData("inserted_into_table:8", false)]
    public void Deleting(string theirs, bool conflict) =>
        Assert.Equal(conflict, Ours(o => o.DeletedTables.Add(7)).ConflictWith(9, Theirs(theirs)) is not null);

    [Theory]
    [Spec("ducklake: Schemas")]
    [Spec("ducklake: Tables / Views")]
    [InlineData("created_schema:\"sales\".\"europe\"", true)]                 // 1.1 nested schema paths
    [InlineData("created_schema:\"europe\"", false)]
    [InlineData("created_table:\"sales\".\"europe\".\"orders\"", true)]
    [InlineData("created_table:\"sales\".\"orders\"", false)]
    public void Nested_schema_paths_are_compared_as_paths(string theirs, bool conflict) =>
        Assert.Equal(conflict, Ours(o =>
        {
            o.CreatedSchemas.Add("sales.europe");
            o.CreatedTables.Add(("sales.europe", null, "orders"));
        }).ConflictWith(9, Theirs(theirs)) is not null);

    [Fact]
    [Spec("ducklake: Conflict Resolution")]
    public void Unknown_change_kinds_conflict_only_on_tables_we_touch()
    {
        var ours = Ours(o => o.InsertedTables.Add(7));
        Assert.NotNull(ours.ConflictWith(9, Theirs("rewrote_everything:7")));
        Assert.Null(ours.ConflictWith(9, Theirs("rewrote_everything:8")));
        Assert.Null(ours.ConflictWith(9, Theirs("created_macro:\"m\"")));
    }

    [Fact]
    [Spec("ducklake: ducklake_snapshot_changes")]
    public void Our_changes_are_written_in_the_spec_format()
    {
        var ours = Ours(o =>
        {
            o.CreatedSchemas.Add("my \"s\"");
            o.CreatedTables.Add(("s", null, "t"));
            o.InsertedTables.Add(3);
            o.DeletedTables.Add(4);
            o.AlteredTables.Add(5);
            o.DroppedTables.Add(6);
            o.DroppedSchemas[1] = "old";
        });
        Assert.Equal(
            "created_schema:\"my \"\"s\"\"\",created_table:\"s\".\"t\",inserted_into_table:3,deleted_from_table:4,altered_table:5,dropped_table:6,dropped_schema:1",
            string.Join(",", ours.Entries()));
    }
}
