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
    // Delete vs delete is decided per data file against the catalog (CatalogTests.Deletes_conflict_only_on_the_same_data_file), not here.
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

    // ---- the full table: every kind of change of ours against every kind of theirs -----------------------------------
    // Ours: schema "s" (ID 4), table "t" (ID 7), view "v" (ID 9), macro "m" (ID 12). Theirs: each kind, on our object and
    // on another one. A pair not listed under an "ours" row must not conflict. Per-data-file decisions (delete vs delete,
    // compaction vs delete or compaction) are made against the catalog at commit, not here, so those pairs are absent.

    private static readonly string[] AllTheirs =
    [
        "created_schema:\"s\"", "created_schema:\"other\"", "dropped_schema:4", "dropped_schema:5",
        "created_table:\"s\".\"t\"", "created_table:\"s\".\"x\"", "created_table:\"s2\".\"t\"", "created_view:\"s\".\"t\"", "created_view:\"s\".\"x\"",
        "dropped_table:7", "dropped_table:8", "altered_table:7", "altered_table:8",
        "inserted_into_table:7", "inserted_into_table:8", "deleted_from_table:7", "deleted_from_table:8",
        "inlined_insert:7", "inlined_insert:8", "inlined_delete:7", "inlined_delete:8",
        "compacted_table:7", "compacted_table:8", "inline_flush:7", "inline_flush:8", "merge_adjacent:7", "rewrite_delete:7",
        "dropped_view:9", "dropped_view:10", "altered_view:9", "altered_view:10",
        "created_scalar_macro:\"s\".\"m\"", "created_table_macro:\"s\".\"m\"", "created_scalar_macro:\"s\".\"n\"",
        "dropped_scalar_macro:12", "dropped_table_macro:12", "dropped_scalar_macro:13",
        "rewrote_everything:7", "rewrote_everything:8",
    ];

    /// <summary>Each kind of change of ours, and the changes of theirs it conflicts with (spec "Logical Conflicts").</summary>
    private static readonly Dictionary<string, (Action<ChangeSet> Build, string[] Conflicts)> Table = new()
    {
        ["create schema"] = (o => o.CreatedSchemas.Add("s"), ["created_schema:\"s\""]),
        // "drop a schema in which another transaction created an entry": tables, views and macros.
        ["drop schema"] = (o => o.DroppedSchemas[4] = "s", ["dropped_schema:4", "created_table:\"s\".\"t\"", "created_table:\"s\".\"x\"", "created_view:\"s\".\"t\"",
            "created_view:\"s\".\"x\"", "created_scalar_macro:\"s\".\"m\"", "created_table_macro:\"s\".\"m\"", "created_scalar_macro:\"s\".\"n\""]),
        ["create table"] = (o => o.CreatedTables.Add(("s", 4, "t")), ["created_table:\"s\".\"t\"", "created_view:\"s\".\"t\"", "dropped_schema:4"]),
        ["drop table"] = (o => o.DroppedTables.Add(7), ["dropped_table:7", "rewrote_everything:7"]),
        ["alter table"] = (o => o.AlteredTables.Add(7), ["dropped_table:7", "altered_table:7", "rewrote_everything:7"]),
        // "rename a table that was dropped, altered or renamed"; the new name also clashes like a create.
        ["rename table"] = (o => o.RenamedTables.Add((7, "s", "x")), ["dropped_table:7", "altered_table:7", "created_table:\"s\".\"x\"", "created_view:\"s\".\"x\"",
            "rewrote_everything:7"]),
        ["insert"] = (o => o.InsertedTables.Add(7), ["dropped_table:7", "altered_table:7", "deleted_from_table:7", "inlined_delete:7", "rewrote_everything:7"]),
        ["inlined insert"] = (o => o.InlinedInsertTables.Add(7), ["dropped_table:7", "altered_table:7", "deleted_from_table:7", "inlined_delete:7", "rewrote_everything:7"]),
        ["delete"] = (o => o.DeletedTables.Add(7), ["dropped_table:7", "altered_table:7", "inserted_into_table:7", "inlined_insert:7", "compacted_table:7",
            "inline_flush:7", "rewrote_everything:7"]),
        // Inlined deletions of file rows are checked per data file against the catalog, compactions included.
        ["inlined delete"] = (o => o.InlinedDeleteTables.Add(7), ["dropped_table:7", "altered_table:7", "inserted_into_table:7", "inlined_insert:7", "inline_flush:7",
            "rewrote_everything:7"]),
        // "compact a table that was dropped"; deletes and compactions of the same files are per data file.
        ["merge adjacent files"] = (o => o.MergedTables.Add(7), ["dropped_table:7", "rewrote_everything:7"]),
        ["rewrite data files"] = (o => o.RewrittenTables.Add(7), ["dropped_table:7", "rewrote_everything:7"]),
        // "Flushing inlined data conflicts with dropping the table, with another flush of the same table and with
        // concurrent inlined deletions" (the spec allows, but does not require, a flush to pass a later inlined deletion).
        ["flush inlined data"] = (o => o.FlushedTables.Add(7), ["dropped_table:7", "inline_flush:7", "inlined_delete:7", "rewrote_everything:7"]),
        ["create view"] = (o => o.CreatedViews.Add(("s", 4, "t")), ["created_table:\"s\".\"t\"", "created_view:\"s\".\"t\"", "dropped_schema:4"]),
        ["alter view"] = (o => o.AlteredViews.Add(9), ["dropped_view:9", "altered_view:9"]),
        ["drop view"] = (o => o.DroppedViews.Add(9), ["dropped_view:9", "altered_view:9"]),
        ["rename view"] = (o => o.RenamedViews.Add((9, "s", "x")), ["dropped_view:9", "altered_view:9", "created_table:\"s\".\"x\"", "created_view:\"s\".\"x\""]),
        ["create macro"] = (o => o.CreatedMacros.Add(("s", 4, "m", false)), ["created_scalar_macro:\"s\".\"m\"", "created_table_macro:\"s\".\"m\"", "dropped_schema:4"]),
        ["drop macro"] = (o => o.DroppedMacros.Add((12, false)), ["dropped_scalar_macro:12", "dropped_table_macro:12"]),
    };

    public static TheoryData<string, string> Pairs()
    {
        var data = new TheoryData<string, string>();
        foreach (var ours in Table.Keys)
            foreach (var theirs in AllTheirs)
                data.Add(ours, theirs);
        return data;
    }

    [Theory]
    [MemberData(nameof(Pairs))]
    [Spec("ducklake: Logical Conflicts")]
    public void Every_change_of_ours_against_every_change_of_theirs(string ours, string theirs)
    {
        var (build, conflicts) = Table[ours];
        Assert.Equal(conflicts.Contains(theirs), Ours(build).ConflictWith(9, Theirs(theirs)) is not null);
    }

    [Fact]
    public void The_table_names_only_changes_that_exist() =>
        Assert.All(Table.Values.SelectMany(v => v.Conflicts), c => Assert.Contains(c, AllTheirs));

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
