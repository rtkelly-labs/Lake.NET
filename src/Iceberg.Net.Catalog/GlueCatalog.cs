using Amazon.Glue;
using Amazon.Glue.Model;
using Iceberg.Net.IO;
using Iceberg.Net.Metadata;
using Iceberg.Net.Types;
using GlueTable = Amazon.Glue.Model.Table;

namespace Iceberg.Net.Catalog;

public sealed record GlueCatalogOptions
{
    /// <summary>Warehouse root used when neither the table nor the database has a location, e.g. <c>s3://bucket/warehouse</c>.</summary>
    public string? Warehouse { get; init; }
    /// <summary><c>glue.id</c>: catalog (account) ID for cross-account access.</summary>
    public string? CatalogId { get; init; }
    /// <summary><c>glue.endpoint</c>: override for emulators (moto) or VPC endpoints.</summary>
    public string? Endpoint { get; init; }
    public string Region { get; init; } = "eu-west-1";
    /// <summary>
    /// <c>glue.skip-archive</c>: Glue keeps a version of the table for every UpdateTable; Iceberg commits
    /// are frequent, so Java defaults to skipping the archive (as do we).
    /// </summary>
    public bool SkipArchive { get; init; } = true;
}

/// <summary>
/// AWS Glue Data Catalog. Namespaces are single-level Glue databases; tables carry
/// <c>table_type=ICEBERG</c> and <c>metadata_location</c> parameters (what Athena reads).
/// The pointer swap uses Glue's <c>VersionId</c> optimistic lock plus a metadata_location check,
/// matching Java's GlueTableOperations.
/// </summary>
public sealed class GlueCatalog : MetastoreCatalog
{
    public const string TableTypeProp = "table_type";
    public const string MetadataLocationProp = "metadata_location";
    public const string PreviousMetadataLocationProp = "previous_metadata_location";

    private readonly IAmazonGlue _glue;
    private readonly GlueCatalogOptions _options;

    public GlueCatalog(IAmazonGlue glue, IFileIO io, GlueCatalogOptions options, string name = "glue") : base(name, io)
    {
        _glue = glue;
        _options = options;
    }

    public static GlueCatalog Create(IFileIO io, GlueCatalogOptions options, string name = "glue")
    {
        var config = new AmazonGlueConfig { RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(options.Region) };
        if (options.Endpoint is not null)
        {
            config.ServiceURL = options.Endpoint;
            config.AuthenticationRegion = options.Region;
        }
        return new GlueCatalog(new AmazonGlueClient(config), io, options, name);
    }

    private static string Db(Namespace ns) => ns.Levels.Count == 1
        ? ns.Levels[0]
        : throw new ArgumentException($"Glue supports single-level namespaces only, got '{ns}'");

    public override async Task CreateNamespaceAsync(Namespace ns, IReadOnlyDictionary<string, string>? properties = null, CancellationToken ct = default)
    {
        var props = properties ?? new Dictionary<string, string>();
        try
        {
            await _glue.CreateDatabaseAsync(new CreateDatabaseRequest
            {
                CatalogId = _options.CatalogId,
                DatabaseInput = new DatabaseInput
                {
                    Name = Db(ns),
                    LocationUri = props.GetValueOrDefault("location"),
                    Description = props.GetValueOrDefault("comment"),
                    Parameters = props.Where(kv => kv.Key is not ("location" or "comment")).ToDictionary(kv => kv.Key, kv => kv.Value),
                },
            }, ct);
        }
        catch (Amazon.Glue.Model.AlreadyExistsException)
        {
            throw new AlreadyExistsException($"Namespace {ns} already exists");
        }
    }

    public override async Task<IReadOnlyList<Namespace>> ListNamespacesAsync(Namespace? parent = null, CancellationToken ct = default)
    {
        if (parent is { Levels.Count: > 0 }) return [];
        var result = new List<Namespace>();
        string? token = null;
        do
        {
            var resp = await _glue.GetDatabasesAsync(new GetDatabasesRequest { CatalogId = _options.CatalogId, NextToken = token }, ct);
            result.AddRange(resp.DatabaseList.Select(d => Namespace.Of(d.Name)));
            token = resp.NextToken;
        } while (token is not null);
        return result;
    }

    public override async Task<bool> NamespaceExistsAsync(Namespace ns, CancellationToken ct = default)
    {
        try
        {
            await _glue.GetDatabaseAsync(new GetDatabaseRequest { CatalogId = _options.CatalogId, Name = Db(ns) }, ct);
            return true;
        }
        catch (EntityNotFoundException)
        {
            return false;
        }
    }

    public override async Task DropNamespaceAsync(Namespace ns, CancellationToken ct = default)
    {
        if ((await ListTablesAsync(ns, ct)).Count > 0) throw new InvalidOperationException($"Namespace {ns} is not empty");
        await _glue.DeleteDatabaseAsync(new DeleteDatabaseRequest { CatalogId = _options.CatalogId, Name = Db(ns) }, ct);
    }

    public override async Task<IReadOnlyList<TableIdentifier>> ListTablesAsync(Namespace ns, CancellationToken ct = default)
    {
        var result = new List<TableIdentifier>();
        string? token = null;
        do
        {
            var resp = await _glue.GetTablesAsync(new GetTablesRequest { CatalogId = _options.CatalogId, DatabaseName = Db(ns), NextToken = token }, ct);
            result.AddRange(resp.TableList
                .Where(t => string.Equals(t.Parameters?.GetValueOrDefault(TableTypeProp), "ICEBERG", StringComparison.OrdinalIgnoreCase))
                .Select(t => new TableIdentifier(ns, t.Name)));
            token = resp.NextToken;
        } while (token is not null);
        return result;
    }

    /// <summary>Glue has no rename: copy the table entry then delete the old one (not atomic, as in Java).</summary>
    public override async Task RenameTableAsync(TableIdentifier from, TableIdentifier to, CancellationToken ct = default)
    {
        var table = await GetGlueTableAsync(from, ct) ?? throw new NoSuchTableException($"Table {from} does not exist");
        await _glue.CreateTableAsync(new Amazon.Glue.Model.CreateTableRequest
        {
            CatalogId = _options.CatalogId,
            DatabaseName = Db(to.Namespace),
            TableInput = new TableInput
            {
                Name = to.Name,
                TableType = table.TableType,
                Parameters = table.Parameters,
                StorageDescriptor = table.StorageDescriptor,
                Description = table.Description,
            },
        }, ct);
        await _glue.DeleteTableAsync(new DeleteTableRequest { CatalogId = _options.CatalogId, DatabaseName = Db(from.Namespace), Name = from.Name }, ct);
    }

    private async Task<GlueTable?> GetGlueTableAsync(TableIdentifier id, CancellationToken ct)
    {
        try
        {
            return (await _glue.GetTableAsync(new GetTableRequest { CatalogId = _options.CatalogId, DatabaseName = Db(id.Namespace), Name = id.Name }, ct)).Table;
        }
        catch (EntityNotFoundException)
        {
            return null;
        }
    }

    protected override async Task<string?> CurrentMetadataLocationAsync(TableIdentifier id, CancellationToken ct)
    {
        var t = await GetGlueTableAsync(id, ct);
        if (t is null) return null;
        if (!string.Equals(t.Parameters?.GetValueOrDefault(TableTypeProp), "ICEBERG", StringComparison.OrdinalIgnoreCase))
            throw new NoSuchTableException($"Glue table {id} exists but is not an Iceberg table");
        return t.Parameters![MetadataLocationProp];
    }

    protected override async Task SwapMetadataLocationAsync(TableIdentifier id, string? expected, string newLocation, TableMetadata metadata, CancellationToken ct)
    {
        var input = new TableInput
        {
            Name = id.Name,
            TableType = "EXTERNAL_TABLE",
            Parameters = new Dictionary<string, string>
            {
                [TableTypeProp] = "ICEBERG",
                [MetadataLocationProp] = newLocation,
            },
            StorageDescriptor = new StorageDescriptor
            {
                Location = metadata.Location,
                // Columns are informational only (Athena reads the Iceberg schema), but the Glue console,
                // Lake Formation column grants and some crawlers rely on them.
                Columns = metadata.CurrentSchema.Columns.Select(c => new Column
                {
                    Name = c.Name,
                    Type = GlueType(c.Type),
                    Comment = c.Doc,
                    Parameters = new Dictionary<string, string>
                    {
                        ["iceberg.field.id"] = c.Id.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        ["iceberg.field.optional"] = c.IsOptional ? "true" : "false",
                        ["iceberg.field.current"] = "true",
                    },
                }).ToList(),
            },
        };

        if (expected is null)
        {
            try
            {
                await _glue.CreateTableAsync(new Amazon.Glue.Model.CreateTableRequest { CatalogId = _options.CatalogId, DatabaseName = Db(id.Namespace), TableInput = input }, ct);
            }
            catch (Amazon.Glue.Model.AlreadyExistsException e)
            {
                throw new CommitFailedException($"Table {id} was created concurrently", e);
            }
            return;
        }

        var current = await GetGlueTableAsync(id, ct) ?? throw new CommitFailedException($"Table {id} was dropped concurrently");
        var actual = current.Parameters?.GetValueOrDefault(MetadataLocationProp);
        if (actual != expected) throw new CommitFailedException($"Table {id} metadata moved: expected {expected}, found {actual}");

        input.Parameters[PreviousMetadataLocationProp] = expected;
        // Preserve non-Iceberg parameters other tools (Athena, Lake Formation) attach to the table.
        foreach (var (k, v) in current.Parameters!)
            input.Parameters.TryAdd(k, v);
        input.Parameters[MetadataLocationProp] = newLocation;

        try
        {
            await _glue.UpdateTableAsync(new UpdateTableRequest
            {
                CatalogId = _options.CatalogId,
                DatabaseName = Db(id.Namespace),
                TableInput = input,
                SkipArchive = _options.SkipArchive,
                VersionId = current.VersionId,
            }, ct);
        }
        catch (ConcurrentModificationException e)
        {
            throw new CommitFailedException($"Glue reported a concurrent update to {id}", e);
        }
    }

    protected override Task RemoveTableEntryAsync(TableIdentifier id, CancellationToken ct) =>
        _glue.DeleteTableAsync(new DeleteTableRequest { CatalogId = _options.CatalogId, DatabaseName = Db(id.Namespace), Name = id.Name }, ct);

    protected override async Task<string> DefaultTableLocationAsync(TableIdentifier id, CancellationToken ct)
    {
        var db = await _glue.GetDatabaseAsync(new GetDatabaseRequest { CatalogId = _options.CatalogId, Name = Db(id.Namespace) }, ct);
        if (!string.IsNullOrEmpty(db.Database.LocationUri)) return $"{db.Database.LocationUri.TrimEnd('/')}/{id.Name}";
        if (_options.Warehouse is null) throw new InvalidOperationException("No table location: set GlueCatalogOptions.Warehouse or a database LocationUri");
        return $"{_options.Warehouse.TrimEnd('/')}/{Db(id.Namespace)}.db/{id.Name}";
    }

    /// <summary>Hive type strings for Glue columns (what Java's IcebergToGlueConverter emits).</summary>
    public static string GlueType(IcebergType t) => t switch
    {
        { TypeId: TypeId.Boolean } => "boolean",
        { TypeId: TypeId.Int } => "int",
        { TypeId: TypeId.Long } => "bigint",
        { TypeId: TypeId.Float } => "float",
        { TypeId: TypeId.Double } => "double",
        { TypeId: TypeId.Date } => "date",
        { TypeId: TypeId.Timestamp or TypeId.TimestampTz or TypeId.TimestampNs or TypeId.TimestampTzNs } => "timestamp",
        { TypeId: TypeId.String or TypeId.Uuid or TypeId.Time } => "string",
        { TypeId: TypeId.Binary or TypeId.Fixed } => "binary",
        DecimalType d => $"decimal({d.Precision},{d.Scale})",
        StructType s => $"struct<{string.Join(",", s.Fields.Select(f => $"{f.Name}:{GlueType(f.Type)}"))}>",
        ListType l => $"array<{GlueType(l.ElementType)}>",
        MapType m => $"map<{GlueType(m.KeyType)},{GlueType(m.ValueType)}>",
        _ => "string",
    };

    public override void Dispose()
    {
        _glue.Dispose();
        base.Dispose();
    }
}
