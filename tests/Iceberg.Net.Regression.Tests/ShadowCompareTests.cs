using Iceberg.Net.Catalog;
using Iceberg.Net.Data;
using Iceberg.Net.Types;

namespace Iceberg.Net.Regression.Tests;

/// <summary>
/// The shadow-comparison fingerprint (tools/ShadowCompare, nightly against production in AWS) agrees with PyIceberg's
/// independent computation for every primitive type and delete mechanism PyIceberg reads, and catches real differences.
/// </summary>
[Trait("Category", "Regression")]
[Spec("Scan Planning")]
public sealed class ShadowCompareTests : IDisposable
{
    private readonly string _warehouse = Path.Combine(Path.GetTempPath(), "iceberg-dotnet-shadow", Guid.NewGuid().ToString("N"));
    private readonly InMemoryCatalog _catalog;

    public ShadowCompareTests()
    {
        _catalog = new InMemoryCatalog(_warehouse);
        _catalog.CreateNamespaceAsync(Namespace.Of("db")).GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _catalog.Dispose();
        if (Directory.Exists(_warehouse)) Directory.Delete(_warehouse, recursive: true);
    }

    private static readonly Schema AllTypes = new(0,
    [
        NestedField.Require(1, "id", PrimitiveType.Long),
        NestedField.Optional(2, "flag", PrimitiveType.Boolean),
        NestedField.Optional(3, "i", PrimitiveType.Int),
        NestedField.Optional(4, "f", PrimitiveType.Float),
        NestedField.Optional(5, "d", PrimitiveType.Double),
        NestedField.Optional(6, "small_dec", new DecimalType(9, 2)),
        NestedField.Optional(7, "big_dec", new DecimalType(18, 4)),
        NestedField.Optional(8, "day", PrimitiveType.Date),
        NestedField.Optional(9, "at", PrimitiveType.Time),
        NestedField.Optional(10, "ts", PrimitiveType.Timestamp),
        NestedField.Optional(11, "tstz", PrimitiveType.TimestampTz),
        NestedField.Optional(12, "s", PrimitiveType.String),
        NestedField.Optional(13, "u", PrimitiveType.Uuid),
        NestedField.Optional(14, "b", PrimitiveType.Binary),
        NestedField.Optional(15, "wide_dec", new DecimalType(38, 10)),
        NestedField.Optional(16, "fx", new FixedType(4)),
    ], identifierFieldIds: [1]);

    private static List<object?[]> Rows(int start, int count) => Enumerable.Range(start, count).Select(i => new object?[]
    {
        (long)i,
        i % 3 == 0 ? null : i % 2 == 0,
        i % 5 == 0 ? null : i * 7 - 50,
        i % 4 == 0 ? float.NaN : i % 6 == 0 ? -0.0f : i / 3f,
        i % 5 == 1 ? double.NaN : i % 7 == 0 ? null : Math.PI * i - 10,
        new IcebergDecimal(i * 101 - 500, 2),
        i % 3 == 1 ? null : new IcebergDecimal(i * 1_000_003L, 4),
        19_000 + i,
        (long)i * 3_600_000_000L % 86_400_000_000L,
        1_700_000_000_000_000L + i * 1_000_001L,
        i % 9 == 0 ? null : 1_600_000_000_000_000L - i * 999L,
        i % 8 == 0 ? null : $"row-{i}-ünïcødé-{new string('x', i % 20)}",
        i % 6 == 0 ? null : new Guid($"{i:x8}-0000-4000-8000-{i * 7919:x12}"),
        i % 4 == 1 ? null : Enumerable.Range(0, i % 9).Select(b => (byte)(b * 31 + i)).ToArray(),
        i % 5 == 2 ? null : new IcebergDecimal(System.Numerics.BigInteger.Parse("123456789012345678901234567") * (i % 2 == 0 ? 1 : -1) + i, 10),
        i % 6 == 3 ? null : new byte[] { (byte)i, (byte)(i * 7), 0xff, 0 },
    }).ToList();

    private static async Task<TableFingerprint> PyIceberg(Table t) =>
        TableFingerprint.FromJson((await TestEnvironment.Current.OracleRawAsync("fingerprint", t.MetadataLocation!)));

    [SkippableTheory]
    [InlineData(2, DeleteStrategy.CopyOnWrite)]
    [InlineData(2, DeleteStrategy.PositionDeletes)]
    [InlineData(3, DeleteStrategy.PositionDeletes)]
    public async Task Fingerprints_agree_with_pyiceberg_for_every_type(int formatVersion, DeleteStrategy strategy)
    {
        Require.LocalOracle();
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", $"t_v{formatVersion}_{strategy}"), AllTypes, FormatVersion: formatVersion));
        await TableMutations.InsertAsync(t, Rows(0, 60));
        await TableMutations.InsertAsync(t, Rows(60, 40));
        await TableMutations.DeleteWhereAsync(t, r => (long)r[0]! % 11 == 3, strategy);

        var ours = await TableFingerprint.ComputeAsync(t);
        var theirs = await PyIceberg(t);
        Assert.Equal(100 - Enumerable.Range(0, 100).Count(i => i % 11 == 3), ours.Rows);
        Assert.Empty(ours.DifferencesFrom(theirs));
        Assert.Equal(ours.RowHash, theirs.RowHash);
    }

    [SkippableFact]
    public async Task A_lost_delete_or_changed_value_is_reported()
    {
        Require.LocalOracle();
        var t = await _catalog.CreateTableAsync(new CreateTableRequest(TableIdentifier.Of("db", "diff"), AllTypes, FormatVersion: 2));
        await TableMutations.InsertAsync(t, Rows(0, 30));
        var before = await TableFingerprint.ComputeAsync(t);
        await TableMutations.DeleteByKeysAsync(t, ["id"], [[7L]], DeleteStrategy.PositionDeletes);
        var afterDelete = await PyIceberg(t);
        Assert.Contains(before.DifferencesFrom(afterDelete), d => d.StartsWith("row count", StringComparison.Ordinal));

        await TableMutations.UpsertAsync(t, [Rows(9, 1)[0].Select((v, i) => i == 11 ? (object?)"changed" : v).ToArray()], strategy: DeleteStrategy.CopyOnWrite);
        var ours = await TableFingerprint.ComputeAsync(t);
        var diffs = ours.DifferencesFrom(afterDelete);
        Assert.StartsWith("s: value hash", Assert.Single(diffs));
    }

    /// <summary>Same multiset of column values, different rows: only the row hash can tell.</summary>
    [Fact]
    public void Swapped_values_between_rows_change_only_the_row_hash()
    {
        var a = new TableFingerprint(2, 1, new Dictionary<int, ColumnFingerprint> { [1] = new("x", 2, 0, 5) });
        var b = a with { RowHash = 2 };
        Assert.Equal(["row hash 0000000000000001 vs 0000000000000002 (same column contents, different rows)"], a.DifferencesFrom(b));
    }
}
