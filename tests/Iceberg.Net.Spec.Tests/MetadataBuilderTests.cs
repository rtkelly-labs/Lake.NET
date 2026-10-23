using Iceberg.Net.Metadata;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Transforms;
using Iceberg.Net.Types;

namespace Iceberg.Net.Spec.Tests;

/// <summary>
/// The metadata state machine update by update (written from the survivors of the stryker-metadata mutation run):
/// requirements, format upgrades, "last added" resolution, refs, statistics, removals, snapshot validation and the
/// metadata log.
/// </summary>
[Spec("Table Metadata")]
[Spec("rest: commitTable")]
public sealed class MetadataBuilderTests
{
    private static readonly Schema Schema = new(0, [NestedField.Require(1, "id", PrimitiveType.Long), NestedField.Optional(2, "region", PrimitiveType.String)]);
    private long _now = 1_000;
    private long Clock() => _now;

    private TableMetadata Table(int formatVersion = 2) =>
        TableMetadata.NewTable(Schema, PartitionSpec.BuilderFor(Schema).Identity("region").Build(), null, "s3://b/t", new Dictionary<string, string> { ["a"] = "1" }, formatVersion, Clock);

    private TableMetadataBuilder B(TableMetadata m) => TableMetadataBuilder.From(m, Clock);

    private static Snapshot Snap(long id, long seq, long? parent = null, long ts = 500, long? firstRowId = null) =>
        new() { SnapshotId = id, ParentSnapshotId = parent, SequenceNumber = seq, TimestampMs = ts, ManifestList = $"s3://b/t/metadata/snap-{id}.avro", FirstRowId = firstRowId, AddedRows = firstRowId is null ? null : 10 };

    [Fact]
    public void Each_requirement_fails_when_its_value_moved()
    {
        var m = Table();
        var withSpec = B(m).Apply(new MetadataUpdate.AddPartitionSpec(new PartitionSpecUpdate(m).AddField("id", Transform.Bucket(4)).Apply()))
            .Apply(new MetadataUpdate.SetDefaultPartitionSpec(MetadataUpdate.LastAdded))
            .Apply(new MetadataUpdate.AddSortOrder(SortOrder.BuilderFor(m.CurrentSchema).Asc("id").Build()))
            .Apply(new MetadataUpdate.SetDefaultSortOrder(MetadataUpdate.LastAdded)).Build();
        UpdateRequirement[] stale =
        [
            new UpdateRequirement.AssertLastAssignedPartitionId(m.LastPartitionId),
            new UpdateRequirement.AssertDefaultSpecId(m.DefaultSpecId),
            new UpdateRequirement.AssertDefaultSortOrderId(m.DefaultSortOrderId),
        ];
        foreach (var r in stale)
        {
            r.Validate(m); // holds on the original
            var e = Assert.Throws<CommitFailedException>(() => r.Validate(withSpec));
            Assert.Contains("Requirement failed", e.Message);
            Assert.Throws<CommitFailedException>(() => r.Validate(null)); // no table
        }
        Assert.Throws<CommitFailedException>(() => new UpdateRequirement.AssertCreate().Validate(m));
        Assert.Throws<CommitFailedException>(() => new UpdateRequirement.AssertTableUuid(Guid.NewGuid()).Validate(m));
        new UpdateRequirement.AssertTableUuid(m.TableUuid).Validate(m);
    }

    [Fact]
    public void Ref_requirements_distinguish_absent_and_moved_refs()
    {
        var m = B(Table()).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 1)))
            .Apply(new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, new SnapshotRef(10, SnapshotRefType.Branch))).Build();
        new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, 10).Validate(m);
        new UpdateRequirement.AssertRefSnapshotId("dev", null).Validate(m);
        var moved = Assert.Throws<CommitFailedException>(() => new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, 9).Validate(m));
        Assert.Contains("10", moved.Message);
        var absent = Assert.Throws<CommitFailedException>(() => new UpdateRequirement.AssertRefSnapshotId("dev", 10).Validate(m));
        Assert.Contains("none", absent.Message);
        Assert.Throws<CommitFailedException>(() => new UpdateRequirement.AssertRefSnapshotId(SnapshotRef.Main, null).Validate(m));
    }

    [Fact]
    public void Format_upgrades_go_up_only_and_start_row_ids_at_v3()
    {
        var v2 = Table(2);
        Assert.Throws<ArgumentException>(() => B(v2).Apply(new MetadataUpdate.UpgradeFormatVersion(TableMetadata.SupportedFormatVersion + 1)));
        Assert.Throws<ArgumentException>(() => B(v2).Apply(new MetadataUpdate.UpgradeFormatVersion(1)));
        var same = B(v2).Apply(new MetadataUpdate.UpgradeFormatVersion(2));
        Assert.Empty(same.Changes);
        var v3 = B(v2).Apply(new MetadataUpdate.UpgradeFormatVersion(3)).Build();
        Assert.Equal(3, v3.FormatVersion);
        Assert.Equal(TableMetadata.InitialRowId, v3.NextRowId);
        Assert.Null(B(Table(1)).Apply(new MetadataUpdate.UpgradeFormatVersion(2)).Build().NextRowId);
    }

    [Fact]
    public void Last_added_needs_something_added_in_this_change()
    {
        var m = Table();
        Assert.Throws<ArgumentException>(() => B(m).Apply(new MetadataUpdate.SetCurrentSchema(MetadataUpdate.LastAdded)));
        Assert.Throws<ArgumentException>(() => B(m).Apply(new MetadataUpdate.SetDefaultPartitionSpec(MetadataUpdate.LastAdded)));
        Assert.Throws<ArgumentException>(() => B(m).Apply(new MetadataUpdate.SetDefaultSortOrder(MetadataUpdate.LastAdded)));
        Assert.ThrowsAny<Exception>(() => B(m).Apply(new MetadataUpdate.SetDefaultPartitionSpec(42)));
        Assert.Throws<ArgumentException>(() => B(m).Apply(new MetadataUpdate.SetDefaultSortOrder(42)));
        // Explicit ids work, without anything added.
        Assert.Equal(m.DefaultSortOrderId, B(m).Apply(new MetadataUpdate.SetDefaultSortOrder(m.DefaultSortOrderId)).Build().DefaultSortOrderId);
    }

    [Fact]
    public void New_specs_and_sort_orders_get_the_next_ids_and_are_recorded()
    {
        var m = Table();
        var b = B(m)
            .Apply(new MetadataUpdate.AddSortOrder(SortOrder.BuilderFor(m.CurrentSchema).Asc("id").Build()))
            .Apply(new MetadataUpdate.AddSortOrder(SortOrder.BuilderFor(m.CurrentSchema).Desc("id").Build()))
            .Apply(new MetadataUpdate.AddPartitionSpec(PartitionSpec.BuilderFor(m.CurrentSchema).Bucket("id", 4).Build()))
            .Apply(new MetadataUpdate.AddPartitionSpec(PartitionSpec.BuilderFor(m.CurrentSchema).Bucket("id", 8).Build()));
        var built = b.Build();
        Assert.Equal([0, 1, 2], built.SortOrders.Select(o => o.OrderId));
        Assert.Equal([0, 1, 2], built.PartitionSpecs.Select(s => s.SpecId));
        Assert.Equal(2, b.Changes.OfType<MetadataUpdate.AddSortOrder>().Count());
        Assert.Equal(2, b.Changes.OfType<MetadataUpdate.AddPartitionSpec>().Count());
        Assert.Equal(m.DefaultSpecId, built.DefaultSpecId); // adding is not making default
    }

    [Fact]
    public void Removing_a_ref_moves_the_current_snapshot_only_for_main()
    {
        var m = B(Table()).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 1)))
            .Apply(new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, new SnapshotRef(10, SnapshotRefType.Branch)))
            .Apply(new MetadataUpdate.SetSnapshotRef("dev", new SnapshotRef(10, SnapshotRefType.Branch))).Build();
        var noDev = B(m).Apply(new MetadataUpdate.RemoveSnapshotRef("dev")).Build();
        Assert.Equal(10, noDev.CurrentSnapshotId);
        Assert.False(noDev.Refs.ContainsKey("dev"));
        var noMain = B(m).Apply(new MetadataUpdate.RemoveSnapshotRef(SnapshotRef.Main)).Build();
        Assert.Null(noMain.CurrentSnapshotId);
        Assert.True(noMain.Refs.ContainsKey("dev"));
        var unknown = B(m).Apply(new MetadataUpdate.RemoveSnapshotRef("nope"));
        Assert.Empty(unknown.Changes);
    }

    [Fact]
    public void Statistics_are_set_per_snapshot_and_removed()
    {
        var m = B(Table()).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 1))).Apply(new MetadataUpdate.AddSnapshot(Snap(11, 2, 10))).Build();
        StatisticsFile Stats(long id, string path) => new(id, path, 10, 4, []);
        var s = B(m).Apply(new MetadataUpdate.SetStatistics(Stats(10, "a"))).Apply(new MetadataUpdate.SetStatistics(Stats(11, "b")))
            .Apply(new MetadataUpdate.SetStatistics(Stats(10, "c"))).Build();
        Assert.Equal(["b", "c"], s.Statistics.Select(x => x.StatisticsPath).Order());
        Assert.Equal(["c"], B(s).Apply(new MetadataUpdate.RemoveStatistics(11)).Build().Statistics.Select(x => x.StatisticsPath));
        // Expiring a snapshot drops its statistics.
        var expired = B(s).Apply(new MetadataUpdate.RemoveSnapshots([10])).Build();
        Assert.Equal(["b"], expired.Statistics.Select(x => x.StatisticsPath));
    }

    [Fact]
    public void Properties_can_be_set_and_removed()
    {
        var m = Table();
        var b = B(m).Apply(new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["b"] = "2" })).Apply(new MetadataUpdate.RemoveProperties(["a"]));
        var built = b.Build();
        Assert.Equal("2", built.Properties["b"]);
        Assert.False(built.Properties.ContainsKey("a"));
    }

    [Fact]
    public void Specs_schemas_and_keys_are_removed_but_never_the_current_ones()
    {
        var m = B(Table(3)).Apply(new MetadataUpdate.AddPartitionSpec(PartitionSpec.BuilderFor(Schema).Bucket("id", 4).Build()))
            .Apply(new MetadataUpdate.AddSchema(new Schema(0, [.. Schema.Columns, NestedField.Optional(3, "x", PrimitiveType.Int)])))
            .Apply(new MetadataUpdate.AddEncryptionKey(new EncryptedKey("k1", "bWV0YQ==")))
            .Apply(new MetadataUpdate.AddEncryptionKey(new EncryptedKey("k2", "bWV0YQ=="))).Build();
        Assert.Throws<ArgumentException>(() => B(m).Apply(new MetadataUpdate.RemovePartitionSpecs([m.DefaultSpecId])));
        Assert.Throws<ArgumentException>(() => B(m).Apply(new MetadataUpdate.RemoveSchemas([m.CurrentSchemaId])));
        var removed = B(m).Apply(new MetadataUpdate.RemovePartitionSpecs([1])).Apply(new MetadataUpdate.RemoveSchemas([1]))
            .Apply(new MetadataUpdate.RemoveEncryptionKey("k1")).Build();
        Assert.Equal([0], removed.PartitionSpecs.Select(s => s.SpecId));
        Assert.Equal([0], removed.Schemas.Select(s => s.SchemaId));
        Assert.Equal(["k2"], removed.EncryptionKeys.Select(k => k.KeyId));
        var replaced = B(m).Apply(new MetadataUpdate.AddEncryptionKey(new EncryptedKey("k2", "bmV3"))).Build();
        Assert.Equal("bmV3", Assert.Single(replaced.EncryptionKeys, k => k.KeyId == "k2").EncryptedKeyMetadata);
    }

    [Fact]
    public void Schemas_needing_a_newer_format_are_refused()
    {
        var v2 = Table(2);
        var v3Only = new Schema(0, [.. Schema.Columns, NestedField.Optional(3, "ns", PrimitiveType.TimestampNs)]);
        Assert.Throws<ArgumentException>(() => B(v2).Apply(new MetadataUpdate.AddSchema(v3Only)));
    }

    [Fact]
    public void Snapshots_are_validated_on_add()
    {
        var v2 = B(Table(2)).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 5))).Build();
        Assert.Throws<ArgumentException>(() => B(v2).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 6, 10)))); // duplicate id
        Assert.Throws<ArgumentException>(() => B(v2).Apply(new MetadataUpdate.AddSnapshot(Snap(11, 5, 10)))); // sequence number not past last
        B(v2).Apply(new MetadataUpdate.AddSnapshot(Snap(11, 6, 10)));
        // v1 has no sequence numbers to check.
        var v1 = B(Table(1)).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 0))).Build();
        B(v1).Apply(new MetadataUpdate.AddSnapshot(Snap(11, 0, 10)));

        var v3 = Table(3);
        Assert.Throws<ArgumentException>(() => B(v3).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 1))));               // no first-row-id
        var withRows = B(v3).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 1, firstRowId: 0))).Build();
        Assert.Equal(10, withRows.NextRowId);
        Assert.Throws<ArgumentException>(() => B(withRows).Apply(new MetadataUpdate.AddSnapshot(Snap(11, 2, 10, firstRowId: 5)))); // behind next-row-id
    }

    [Fact]
    public void Ref_moves_log_the_snapshot_time_when_added_together_and_the_clock_otherwise()
    {
        var m = Table();
        _now = 9_000;
        var together = B(m).Apply(new MetadataUpdate.AddSnapshot(Snap(10, 1, ts: 700)))
            .Apply(new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, new SnapshotRef(10, SnapshotRefType.Branch))).Build();
        Assert.Equal(700, together.SnapshotLog.Single().TimestampMs);
        Assert.Equal(9_000, together.LastUpdatedMs);

        _now = 12_000;
        var later = B(together).Apply(new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, new SnapshotRef(10, SnapshotRefType.Branch)));
        Assert.Empty(later.Changes); // same ref: nothing to do
        var rollback = B(B(together).Apply(new MetadataUpdate.AddSnapshot(Snap(11, 2, 10, ts: 800)))
                .Apply(new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, new SnapshotRef(11, SnapshotRefType.Branch))).Build())
            .Apply(new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, new SnapshotRef(10, SnapshotRefType.Branch))).Build();
        Assert.Equal(12_000, rollback.SnapshotLog[^1].TimestampMs); // a move to an existing snapshot happens now
        Assert.Throws<ArgumentException>(() => B(m).Apply(new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, new SnapshotRef(99, SnapshotRefType.Branch))));
    }

    [Fact]
    public void Expiring_a_snapshot_truncates_the_snapshot_log_before_it()
    {
        var b = B(Table());
        for (var i = 1; i <= 3; i++)
            b.Apply(new MetadataUpdate.AddSnapshot(Snap(i, i, i == 1 ? null : i - 1, ts: 100 * i)))
             .Apply(new MetadataUpdate.SetSnapshotRef(SnapshotRef.Main, new SnapshotRef(i, SnapshotRefType.Branch)));
        var m = b.Build();
        Assert.Equal([1L, 2L, 3L], m.SnapshotLog.Select(e => e.SnapshotId));
        var expired = B(m).Apply(new MetadataUpdate.RemoveSnapshots([2])).Build();
        Assert.Equal([3L], expired.SnapshotLog.Select(e => e.SnapshotId)); // history before a gap is no longer usable
    }

    [Fact]
    public void The_metadata_log_keeps_the_configured_number_of_previous_files()
    {
        var m = Table() with { MetadataFileLocation = "s3://b/t/metadata/00000.metadata.json" };
        m = B(m).Apply(new MetadataUpdate.SetProperties(new Dictionary<string, string> { [TableMetadataBuilder.MetadataPreviousVersionsMax] = "2" })).Build("s3://b/t/metadata/00001.metadata.json");
        for (var i = 2; i <= 4; i++) m = B(m).Apply(new MetadataUpdate.SetProperties(new Dictionary<string, string> { ["n"] = $"{i}" })).Build($"s3://b/t/metadata/0000{i}.metadata.json");
        Assert.Equal(["s3://b/t/metadata/00002.metadata.json", "s3://b/t/metadata/00003.metadata.json"], m.MetadataLog.Select(e => e.MetadataFile));
        // No change, same file: nothing logged and last-updated stays.
        var unchanged = B(m).Build(m.MetadataFileLocation);
        Assert.Equal(m.MetadataLog, unchanged.MetadataLog);
        Assert.Equal(m.LastUpdatedMs, unchanged.LastUpdatedMs);
    }

    [Fact]
    public void Partition_evolution_assigns_fresh_field_ids_and_reuses_old_ones()
    {
        var m = Table(2); // region (1000)
        var evolved = new PartitionSpecUpdate(m).AddField("id", Transform.Bucket(16)).RemoveField("region").Apply();
        Assert.Equal([(1001, "id_bucket")], evolved.Fields.Select(f => (f.FieldId, f.Name)));
        var (req, ups) = new PartitionSpecUpdate(m).AddField("id", Transform.Bucket(16)).RemoveField("region").ToCommit();
        var m2 = B(m).ApplyAll(ups).Build();
        foreach (var r in req) r.Validate(m);
        Assert.Equal(1001, m2.LastPartitionId);
        Assert.Equal(1, m2.DefaultSpecId);
        // Adding region back reuses its old field ID; a fresh field continues after the highest.
        var back = new PartitionSpecUpdate(m2).AddField("region").AddField("id", Transform.Truncate(10)).Apply();
        Assert.Equal([1001, 1000, 1002], back.Fields.Select(f => f.FieldId));
        Assert.Throws<ArgumentException>(() => new PartitionSpecUpdate(m2).AddField("id", Transform.Bucket(16)));
        Assert.Throws<ArgumentException>(() => new PartitionSpecUpdate(m2).RemoveField("nope"));
        Assert.Equal("bucket_id", new PartitionSpecUpdate(m2).RenameField("id_bucket", "bucket_id").Apply().Fields[0].Name);
    }

    [Fact]
    public void Removing_a_v1_partition_field_voids_it()
    {
        var v1 = Table(1);
        var spec = new PartitionSpecUpdate(v1).RemoveField("region").AddField("id", Transform.Bucket(4)).Apply();
        Assert.Equal([1000, 1001], spec.Fields.Select(f => f.FieldId));
        Assert.IsType<VoidTransform>(spec.Fields[0].Transform);
        B(v1).Apply(new MetadataUpdate.AddPartitionSpec(spec)).Build();
    }
}
