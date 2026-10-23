using Iceberg.Net.Json;
using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;
using R = Iceberg.Net.Metadata.UpdateRequirement;
using U = Iceberg.Net.Metadata.MetadataUpdate;

namespace Iceberg.Net.Spec.Tests;

/// <summary>The metadata state machine: updates, requirements, sequence numbers, logs and row lineage.</summary>
public class StateManagementTests
{
    private static readonly Schema Schema = new(NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "name", PrimitiveType.String));
    private long _now = 1_700_000_000_000;
    private long Clock() => _now += 1000;

    private TableMetadata NewTable(int formatVersion = 2) =>
        TableMetadata.NewTable(Schema, null, null, "s3://b/t", formatVersion: formatVersion, clock: Clock) with { MetadataFileLocation = "s3://b/t/metadata/00000-a.metadata.json" };

    private Snapshot Snap(long id, long? parent, long seq, long? firstRowId = null, long? addedRows = null) => new()
    {
        SnapshotId = id,
        ParentSnapshotId = parent,
        SequenceNumber = seq,
        TimestampMs = Clock(),
        ManifestList = $"s3://b/t/metadata/snap-{id}.avro",
        Summary = new Dictionary<string, string> { ["operation"] = "append" },
        FirstRowId = firstRowId,
        AddedRows = addedRows,
    };

    private TableMetadata Commit(TableMetadata m, string next, params MetadataUpdate[] updates) =>
        TableMetadataBuilder.From(m, Clock).ApplyAll(updates).Build(next);

    [Spec("Sequence Numbers")]
    [Spec("Table Metadata Fields")]
    [Fact]
    public void Appending_snapshots_advances_sequence_number_current_snapshot_and_logs()
    {
        var m0 = NewTable();
        var m1 = Commit(m0, "s3://b/t/metadata/00001-b.metadata.json",
            new U.AddSnapshot(Snap(1, null, m0.NextSequenceNumber)), new U.SetSnapshotRef("main", SnapshotRef.Branch(1)));
        Assert.Equal(1, m1.LastSequenceNumber);
        Assert.Equal(1, m1.CurrentSnapshotId);
        Assert.Single(m1.SnapshotLog);
        Assert.Equal("s3://b/t/metadata/00000-a.metadata.json", m1.MetadataLog.Single().MetadataFile);
        Assert.Equal(m0.LastUpdatedMs, m1.MetadataLog.Single().TimestampMs);

        var m2 = Commit(m1, "s3://b/t/metadata/00002-c.metadata.json",
            new U.AddSnapshot(Snap(2, 1, m1.NextSequenceNumber)), new U.SetSnapshotRef("main", SnapshotRef.Branch(2)));
        Assert.Equal(2, m2.LastSequenceNumber);
        Assert.Equal(2, m2.MetadataLog.Count);
        Assert.True(m2.LastUpdatedMs > m1.LastUpdatedMs);
    }

    [Spec("Sequence Numbers")]
    [Fact]
    public void Stale_sequence_number_is_rejected()
    {
        var m1 = Commit(NewTable(), "s3://x/1.metadata.json", new U.AddSnapshot(Snap(1, null, 1)), new U.SetSnapshotRef("main", SnapshotRef.Branch(1)));
        Assert.Throws<ArgumentException>(() => Commit(m1, "s3://x/2.metadata.json", new U.AddSnapshot(Snap(2, 1, 1))));
    }

    [Spec("Table Metadata Fields")]
    [Fact]
    public void Metadata_log_is_trimmed_to_previous_versions_max()
    {
        var m = Commit(NewTable(), "s3://b/t/metadata/00001.metadata.json", new U.SetProperties(new Dictionary<string, string> { [TableMetadataBuilder.MetadataPreviousVersionsMax] = "2" }));
        for (var i = 2; i < 6; i++)
            m = Commit(m, $"s3://b/t/metadata/{i:D5}.metadata.json", new U.SetProperties(new Dictionary<string, string> { ["k"] = i.ToString() }));
        Assert.Equal(["s3://b/t/metadata/00003.metadata.json", "s3://b/t/metadata/00004.metadata.json"], m.MetadataLog.Select(e => e.MetadataFile));
    }

    [Spec("Snapshot References")]
    [Fact]
    public void Branches_and_tags_are_independent_of_main()
    {
        var m = Commit(NewTable(), "s3://x/1.metadata.json",
            new U.AddSnapshot(Snap(1, null, 1)), new U.SetSnapshotRef("main", SnapshotRef.Branch(1)),
            new U.AddSnapshot(Snap(2, 1, 2)), new U.SetSnapshotRef("audit", SnapshotRef.Branch(2)),
            new U.SetSnapshotRef("v1.0", SnapshotRef.Tag(1)));
        Assert.Equal(1, m.CurrentSnapshotId);
        Assert.Equal(2, m.SnapshotForRef("audit")!.SnapshotId);
        Assert.Single(m.SnapshotLog); // only main changes are logged

        var removed = Commit(m, "s3://x/2.metadata.json", new U.RemoveSnapshotRef("audit"), new U.RemoveSnapshots([2]));
        Assert.Null(removed.SnapshotById(2));
        Assert.False(removed.Refs.ContainsKey("audit"));
        Assert.Equal(1, removed.CurrentSnapshotId);
    }

    [Spec("Snapshot Retention Policy")]
    [Fact]
    public void Removing_snapshots_drops_refs_and_truncates_snapshot_log()
    {
        var m = Commit(NewTable(), "s3://x/1.metadata.json",
            new U.AddSnapshot(Snap(1, null, 1)), new U.SetSnapshotRef("main", SnapshotRef.Branch(1)));
        m = Commit(m, "s3://x/2.metadata.json", new U.AddSnapshot(Snap(2, 1, 2)), new U.SetSnapshotRef("main", SnapshotRef.Branch(2)));
        m = Commit(m, "s3://x/3.metadata.json", new U.AddSnapshot(Snap(3, 2, 3)), new U.SetSnapshotRef("main", SnapshotRef.Branch(3)));
        var expired = Commit(m, "s3://x/4.metadata.json", new U.RemoveSnapshots([1, 2]));
        Assert.Equal([3L], expired.Snapshots.Select(s => s.SnapshotId));
        Assert.Equal([3L], expired.SnapshotLog.Select(s => s.SnapshotId));
    }

    [Spec("Schema Evolution")]
    [Fact]
    public void Adding_an_equivalent_schema_reuses_it()
    {
        var m0 = NewTable();
        var evolved = new Schema(0, [.. Schema.Columns, NestedField.Optional(3, "email", PrimitiveType.String)]);
        var m1 = Commit(m0, "s3://x/1.metadata.json", new U.AddSchema(evolved), new U.SetCurrentSchema(U.LastAdded));
        Assert.Equal(1, m1.CurrentSchemaId);
        Assert.Equal(3, m1.LastColumnId);

        // Re-adding the original schema switches back without creating schema 2.
        var b = TableMetadataBuilder.From(m1, Clock).ApplyAll([new U.AddSchema(m0.CurrentSchema), new U.SetCurrentSchema(U.LastAdded)]);
        var m2 = b.Build("s3://x/2.metadata.json");
        Assert.Equal(0, m2.CurrentSchemaId);
        Assert.Equal(2, m2.Schemas.Count);
        Assert.DoesNotContain(b.Changes, c => c is U.AddSchema);
    }

    [Spec("Partition Evolution")]
    [Fact]
    public void Partition_spec_evolution_assigns_new_spec_id()
    {
        var m0 = NewTable();
        var spec = PartitionSpec.BuilderFor(m0.CurrentSchema).Bucket("id", 16).Build();
        var m1 = Commit(m0, "s3://x/1.metadata.json", new U.AddPartitionSpec(spec), new U.SetDefaultPartitionSpec(U.LastAdded));
        Assert.Equal(1, m1.DefaultSpecId);
        Assert.Equal(1000, m1.LastPartitionId);
        Assert.Equal(2, m1.PartitionSpecs.Count);
    }

    [Spec("Format Versioning")]
    [Spec("Appendix E: Format version changes")]
    [Fact]
    public void Format_version_can_upgrade_not_downgrade()
    {
        var m0 = NewTable(2);
        var m1 = Commit(m0, "s3://x/1.metadata.json", new U.UpgradeFormatVersion(3));
        Assert.Equal(3, m1.FormatVersion);
        Assert.Equal(0, m1.NextRowId);
        Assert.Throws<ArgumentException>(() => Commit(m1, "s3://x/2.metadata.json", new U.UpgradeFormatVersion(2)));
    }

    [Spec("Snapshot Row IDs")]
    [Spec("First Row ID Assignment")]
    [Fact]
    public void V3_row_lineage_advances_next_row_id()
    {
        var m0 = NewTable(3);
        var m1 = Commit(m0, "s3://x/1.metadata.json", new U.AddSnapshot(Snap(1, null, 1, firstRowId: 0, addedRows: 100)), new U.SetSnapshotRef("main", SnapshotRef.Branch(1)));
        Assert.Equal(100, m1.NextRowId);
        // A snapshot whose first-row-id is behind next-row-id would reuse row IDs.
        Assert.Throws<ArgumentException>(() => Commit(m1, "s3://x/2.metadata.json", new U.AddSnapshot(Snap(2, 1, 2, firstRowId: 50, addedRows: 10))));
        Assert.Throws<ArgumentException>(() => Commit(m1, "s3://x/2.metadata.json", new U.AddSnapshot(Snap(2, 1, 2))));
    }

    [Spec("Optimistic Concurrency")]
    [Spec("rest: updateTable")]
    [Fact]
    public void Requirements_detect_concurrent_changes()
    {
        var m = Commit(NewTable(), "s3://x/1.metadata.json", new U.AddSnapshot(Snap(1, null, 1)), new U.SetSnapshotRef("main", SnapshotRef.Branch(1)));
        new R.AssertRefSnapshotId("main", 1).Validate(m);
        new R.AssertRefSnapshotId("branch-x", null).Validate(m);
        new R.AssertTableUuid(m.TableUuid).Validate(m);
        Assert.Throws<CommitFailedException>(() => new R.AssertRefSnapshotId("main", null).Validate(m));
        Assert.Throws<CommitFailedException>(() => new R.AssertTableUuid(Guid.NewGuid()).Validate(m));
        Assert.Throws<CommitFailedException>(() => new R.AssertCreate().Validate(m));
        Assert.Throws<CommitFailedException>(() => new R.AssertCurrentSchemaId(5).Validate(m));
        Assert.Throws<CommitFailedException>(() => new R.AssertLastAssignedFieldId(1).Validate(m));
        new R.AssertCreate().Validate(null);
    }

    [Spec("rest: createTable")]
    [Fact]
    public void Create_records_every_initial_setting_as_changes()
    {
        var b = TableMetadataBuilder.ForCreate(NewTable(), Clock);
        Assert.Equal(["assign-uuid", "upgrade-format-version", "add-schema", "set-current-schema", "add-spec", "set-default-spec", "add-sort-order", "set-default-sort-order", "set-location"],
            b.Changes.Select(c => c.Action));
    }

    [Spec("rest: updateTable")]
    [Fact]
    public void Updates_and_requirements_round_trip_as_rest_json()
    {
        MetadataUpdate[] updates =
        [
            new U.AssignUuid(Guid.NewGuid()),
            new U.UpgradeFormatVersion(3),
            new U.AddSchema(Schema),
            new U.SetCurrentSchema(-1),
            new U.AddPartitionSpec(PartitionSpec.BuilderFor(Schema).Bucket("id", 4).Build()),
            new U.SetDefaultPartitionSpec(-1),
            new U.AddSortOrder(SortOrder.Unsorted),
            new U.SetDefaultSortOrder(0),
            new U.AddSnapshot(Snap(9, null, 1)),
            new U.SetSnapshotRef("main", new SnapshotRef(9, SnapshotRefType.Branch, 3, 1000, null)),
            new U.RemoveSnapshotRef("old"),
            new U.RemoveSnapshots([1, 2]),
            new U.SetLocation("s3://new"),
            new U.SetProperties(new Dictionary<string, string> { ["a"] = "b" }),
            new U.RemoveProperties(["c"]),
            new U.RemoveStatistics(9),
            new U.RemovePartitionSpecs([1]),
            new U.RemoveSchemas([2]),
            new U.RemoveEncryptionKey("k"),
        ];
        foreach (var u in updates)
        {
            var json = UpdateJson.ToJson(u).ToJsonString();
            var back = UpdateJson.UpdateFromJson(System.Text.Json.Nodes.JsonNode.Parse(json)!);
            Assert.Equal(u.Action, back.Action);
            Assert.Equal(json, UpdateJson.ToJson(back).ToJsonString());
        }

        UpdateRequirement[] reqs =
        [
            new R.AssertCreate(), new R.AssertTableUuid(Guid.NewGuid()), new R.AssertRefSnapshotId("main", null), new R.AssertRefSnapshotId("main", 5),
            new R.AssertLastAssignedFieldId(3), new R.AssertCurrentSchemaId(1), new R.AssertLastAssignedPartitionId(1000),
            new R.AssertDefaultSpecId(0), new R.AssertDefaultSortOrderId(0),
        ];
        foreach (var r in reqs)
        {
            var json = UpdateJson.ToJson(r).ToJsonString();
            Assert.Equal(r, UpdateJson.RequirementFromJson(System.Text.Json.Nodes.JsonNode.Parse(json)!));
        }
        // A "must not exist" ref assertion serialises snapshot-id as an explicit null.
        Assert.Contains("\"snapshot-id\":null", UpdateJson.ToJson(new R.AssertRefSnapshotId("main", null)).ToJsonString());
    }
}
