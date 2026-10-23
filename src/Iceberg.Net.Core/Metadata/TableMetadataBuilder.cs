using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Metadata;

/// <summary>
/// Applies <see cref="MetadataUpdate"/>s to a base <see cref="TableMetadata"/>, enforcing the spec's
/// invariants (monotonic sequence numbers, v3 row-ID assignment, ref/snapshot-log consistency, metadata log).
/// Mirrors Java's <c>TableMetadata.Builder</c> so that a commit produces the same metadata regardless of
/// whether a REST server or this client applies it.
/// </summary>
public sealed class TableMetadataBuilder
{
    public const string MetadataPreviousVersionsMax = "write.metadata.previous-versions-max";
    public const int MetadataPreviousVersionsMaxDefault = 100;

    private readonly TableMetadata? _base;
    private TableMetadata _m;
    private readonly List<MetadataUpdate> _changes = [];
    private int? _lastAddedSchemaId;
    private int? _lastAddedSpecId;
    private int? _lastAddedOrderId;
    private readonly Func<long> _clock;

    private TableMetadataBuilder(TableMetadata? @base, TableMetadata start, Func<long>? clock)
    {
        _base = @base;
        _m = start;
        _clock = clock ?? (() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    /// <summary>Start from existing metadata (a commit against a loaded table).</summary>
    public static TableMetadataBuilder From(TableMetadata current, Func<long>? clock = null) => new(current, current, clock);

    /// <summary>Start from fresh metadata (table creation): every initial setting is recorded as a change, as REST create does.</summary>
    public static TableMetadataBuilder ForCreate(TableMetadata fresh, Func<long>? clock = null)
    {
        var b = new TableMetadataBuilder(null, fresh, clock);
        b._changes.Add(new MetadataUpdate.AssignUuid(fresh.TableUuid));
        b._changes.Add(new MetadataUpdate.UpgradeFormatVersion(fresh.FormatVersion));
        b._changes.Add(new MetadataUpdate.AddSchema(fresh.CurrentSchema));
        b._changes.Add(new MetadataUpdate.SetCurrentSchema(MetadataUpdate.LastAdded));
        b._changes.Add(new MetadataUpdate.AddPartitionSpec(fresh.DefaultSpec));
        b._changes.Add(new MetadataUpdate.SetDefaultPartitionSpec(MetadataUpdate.LastAdded));
        b._changes.Add(new MetadataUpdate.AddSortOrder(fresh.DefaultSortOrder));
        b._changes.Add(new MetadataUpdate.SetDefaultSortOrder(MetadataUpdate.LastAdded));
        b._changes.Add(new MetadataUpdate.SetLocation(fresh.Location));
        if (fresh.Properties.Count > 0) b._changes.Add(new MetadataUpdate.SetProperties(fresh.Properties));
        return b;
    }

    public IReadOnlyList<MetadataUpdate> Changes => _changes;

    public TableMetadataBuilder ApplyAll(IEnumerable<MetadataUpdate> updates)
    {
        foreach (var u in updates) Apply(u);
        return this;
    }

    public TableMetadataBuilder Apply(MetadataUpdate update)
    {
        switch (update)
        {
            case MetadataUpdate.AssignUuid u:
                if (_base is not null && _base.TableUuid != u.Uuid) throw new ArgumentException("Cannot reassign table UUID");
                _m = _m with { TableUuid = u.Uuid };
                break;

            case MetadataUpdate.UpgradeFormatVersion u:
                if (u.FormatVersion > TableMetadata.SupportedFormatVersion) throw new ArgumentException($"Unsupported format version {u.FormatVersion}");
                if (u.FormatVersion < _m.FormatVersion) throw new ArgumentException($"Cannot downgrade v{_m.FormatVersion} table to v{u.FormatVersion}");
                if (u.FormatVersion == _m.FormatVersion && _base is not null) return this;
                _m = _m with
                {
                    FormatVersion = u.FormatVersion,
                    NextRowId = u.FormatVersion >= 3 ? _m.NextRowId ?? TableMetadata.InitialRowId : null,
                };
                break;

            case MetadataUpdate.AddSchema u:
                AddSchema(u.Schema);
                return this; // AddSchema records its own (possibly re-numbered) change

            case MetadataUpdate.SetCurrentSchema u:
            {
                var id = u.SchemaId == MetadataUpdate.LastAdded ? _lastAddedSchemaId ?? throw new ArgumentException("No schema added in this commit") : u.SchemaId;
                _m.SchemaById(id);
                _m = _m with { CurrentSchemaId = id };
                break;
            }

            case MetadataUpdate.AddPartitionSpec u:
                AddSpec(u.Spec);
                return this;

            case MetadataUpdate.SetDefaultPartitionSpec u:
            {
                var id = u.SpecId == MetadataUpdate.LastAdded ? _lastAddedSpecId ?? throw new ArgumentException("No spec added in this commit") : u.SpecId;
                _m.SpecById(id);
                _m = _m with { DefaultSpecId = id };
                break;
            }

            case MetadataUpdate.AddSortOrder u:
            {
                var existing = _m.SortOrders.FirstOrDefault(o => o.SameOrder(u.SortOrder));
                if (existing is not null) { _lastAddedOrderId = existing.OrderId; return this; }
                var id = u.SortOrder.IsUnsorted ? 0 : Math.Max(1, _m.SortOrders.Max(o => o.OrderId) + 1);
                var order = new SortOrder(id, u.SortOrder.Fields);
                _m = _m with { SortOrders = [.. _m.SortOrders, order] };
                _lastAddedOrderId = id;
                _changes.Add(new MetadataUpdate.AddSortOrder(order));
                return this;
            }

            case MetadataUpdate.SetDefaultSortOrder u:
            {
                var id = u.SortOrderId == MetadataUpdate.LastAdded ? _lastAddedOrderId ?? throw new ArgumentException("No sort order added in this commit") : u.SortOrderId;
                if (_m.SortOrders.All(o => o.OrderId != id)) throw new ArgumentException($"Unknown sort order {id}");
                _m = _m with { DefaultSortOrderId = id };
                break;
            }

            case MetadataUpdate.AddSnapshot u:
                AddSnapshot(u.Snapshot);
                break;

            case MetadataUpdate.SetSnapshotRef u:
                SetRef(u.RefName, u.Ref);
                break;

            case MetadataUpdate.RemoveSnapshotRef u:
                if (!_m.Refs.ContainsKey(u.RefName)) return this;
                var refs = Compat.Copy(_m.Refs);
                refs.Remove(u.RefName);
                _m = _m with { Refs = refs, CurrentSnapshotId = u.RefName == SnapshotRef.Main ? null : _m.CurrentSnapshotId };
                break;

            case MetadataUpdate.RemoveSnapshots u:
                RemoveSnapshots(u.SnapshotIds);
                break;

            case MetadataUpdate.SetLocation u:
                _m = _m with { Location = u.Location.TrimEnd('/') };
                break;

            case MetadataUpdate.SetProperties u:
            {
                var props = Compat.Copy(_m.Properties);
                foreach (var (k, v) in u.Updates) props[k] = v;
                _m = _m with { Properties = props };
                break;
            }

            case MetadataUpdate.RemoveProperties u:
            {
                var props = Compat.Copy(_m.Properties);
                foreach (var k in u.Removals) props.Remove(k);
                _m = _m with { Properties = props };
                break;
            }

            case MetadataUpdate.SetStatistics u:
                _m = _m with { Statistics = [.. _m.Statistics.Where(s => s.SnapshotId != u.Statistics.SnapshotId), u.Statistics] };
                break;

            case MetadataUpdate.RemoveStatistics u:
                _m = _m with { Statistics = _m.Statistics.Where(s => s.SnapshotId != u.SnapshotId).ToList() };
                break;

            case MetadataUpdate.RemovePartitionSpecs u:
                if (u.SpecIds.Contains(_m.DefaultSpecId)) throw new ArgumentException("Cannot remove the default partition spec");
                _m = _m with { PartitionSpecs = _m.PartitionSpecs.Where(s => !u.SpecIds.Contains(s.SpecId)).ToList() };
                break;

            case MetadataUpdate.RemoveSchemas u:
                if (u.SchemaIds.Contains(_m.CurrentSchemaId)) throw new ArgumentException("Cannot remove the current schema");
                _m = _m with { Schemas = _m.Schemas.Where(s => !u.SchemaIds.Contains(s.SchemaId)).ToList() };
                break;

            case MetadataUpdate.AddEncryptionKey u:
                _m = _m with { EncryptionKeys = [.. _m.EncryptionKeys.Where(k => k.KeyId != u.Key.KeyId), u.Key] };
                break;

            case MetadataUpdate.RemoveEncryptionKey u:
                _m = _m with { EncryptionKeys = _m.EncryptionKeys.Where(k => k.KeyId != u.KeyId).ToList() };
                break;

            default:
                throw new NotSupportedException($"Unsupported metadata update {update.Action}");
        }

        _changes.Add(update);
        return this;
    }

    private void AddSchema(Schema schema)
    {
        var existing = _m.Schemas.FirstOrDefault(s => s.SameSchema(schema));
        if (existing is not null)
        {
            _lastAddedSchemaId = existing.SchemaId;
            return;
        }
        if (schema.MinFormatVersion > _m.FormatVersion)
            throw new ArgumentException($"Schema requires format version {schema.MinFormatVersion}; table is v{_m.FormatVersion}");
        var id = _m.Schemas.Max(s => s.SchemaId) + 1;
        var added = schema.WithId(id);
        _m = _m with { Schemas = [.. _m.Schemas, added], LastColumnId = Math.Max(_m.LastColumnId, added.HighestFieldId) };
        _lastAddedSchemaId = id;
        _changes.Add(new MetadataUpdate.AddSchema(added));
    }

    private void AddSpec(PartitionSpec spec)
    {
        var existing = _m.PartitionSpecs.FirstOrDefault(s => s.CompatibleWith(spec));
        if (existing is not null)
        {
            _lastAddedSpecId = existing.SpecId;
            return;
        }
        if (_m.FormatVersion == 1 && spec.Fields.Any(f => f.FieldId <= _m.LastPartitionId && _m.PartitionSpecs.SelectMany(s => s.Fields).All(e => e.FieldId != f.FieldId)))
            throw new ArgumentException("v1 partition field IDs must be sequential");
        var id = _m.PartitionSpecs.Max(s => s.SpecId) + 1;
        var added = new PartitionSpec(id, spec.Fields);
        _m = _m with { PartitionSpecs = [.. _m.PartitionSpecs, added], LastPartitionId = Math.Max(_m.LastPartitionId, added.LastAssignedFieldId) };
        _lastAddedSpecId = id;
        _changes.Add(new MetadataUpdate.AddPartitionSpec(added));
    }

    private void AddSnapshot(Snapshot snapshot)
    {
        if (_m.SnapshotById(snapshot.SnapshotId) is not null) throw new ArgumentException($"Snapshot {snapshot.SnapshotId} already exists");
        if (_m.FormatVersion > 1 && snapshot.SequenceNumber <= _m.LastSequenceNumber && snapshot.ParentSnapshotId is not null)
            throw new ArgumentException($"Cannot add snapshot with sequence number {snapshot.SequenceNumber} older than last sequence number {_m.LastSequenceNumber}");

        long? nextRowId = _m.NextRowId;
        if (_m.FormatVersion >= 3)
        {
            if (snapshot.FirstRowId is null) throw new ArgumentException("v3 snapshots must have first-row-id");
            if (snapshot.FirstRowId < _m.NextRowId)
                throw new ArgumentException($"Cannot add snapshot: first-row-id {snapshot.FirstRowId} is behind table next-row-id {_m.NextRowId}");
            nextRowId = snapshot.FirstRowId + (snapshot.AddedRows ?? 0);
        }

        _m = _m with
        {
            Snapshots = [.. _m.Snapshots, snapshot],
            LastSequenceNumber = Math.Max(_m.LastSequenceNumber, snapshot.SequenceNumber),
            LastUpdatedMs = snapshot.TimestampMs,
            NextRowId = nextRowId,
        };
    }

    private void SetRef(string name, SnapshotRef r)
    {
        var snapshot = _m.SnapshotById(r.SnapshotId) ?? throw new ArgumentException($"Cannot set {name} to unknown snapshot {r.SnapshotId}");
        if (_m.Refs.TryGetValue(name, out var existing) && existing == r) return;
        var refs = Compat.Copy(_m.Refs);
        refs[name] = r;
        _m = _m with { Refs = refs };
        if (name == SnapshotRef.Main)
        {
            // Snapshot log uses the snapshot's own timestamp when it was added in this commit, else now.
            var addedHere = _changes.OfType<MetadataUpdate.AddSnapshot>().Any(a => a.Snapshot.SnapshotId == r.SnapshotId);
            var ts = addedHere ? snapshot.TimestampMs : _clock();
            _m = _m with
            {
                CurrentSnapshotId = r.SnapshotId,
                SnapshotLog = [.. _m.SnapshotLog, new SnapshotLogEntry(ts, r.SnapshotId)],
                LastUpdatedMs = Math.Max(_m.LastUpdatedMs, ts),
            };
        }
    }

    private void RemoveSnapshots(IReadOnlyList<long> ids)
    {
        var remove = ids.ToHashSet();
        var refs = _m.Refs.Where(kv => !remove.Contains(kv.Value.SnapshotId)).ToDictionary(kv => kv.Key, kv => kv.Value);
        // Like Java: the snapshot log is only valid after the last entry that points at a removed snapshot.
        var log = new List<SnapshotLogEntry>();
        foreach (var e in _m.SnapshotLog)
        {
            if (remove.Contains(e.SnapshotId)) log.Clear();
            else log.Add(e);
        }
        _m = _m with
        {
            Snapshots = _m.Snapshots.Where(s => !remove.Contains(s.SnapshotId)).ToList(),
            Refs = refs,
            SnapshotLog = log,
            CurrentSnapshotId = refs.TryGetValue(SnapshotRef.Main, out var main) ? main.SnapshotId : null,
            Statistics = _m.Statistics.Where(s => !remove.Contains(s.SnapshotId)).ToList(),
            PartitionStatistics = _m.PartitionStatistics.Where(s => !remove.Contains(s.SnapshotId)).ToList(),
        };
    }

    /// <summary>
    /// Produce the new metadata. When <paramref name="newMetadataLocation"/> differs from the base's
    /// location, the base file is appended to the metadata log (trimmed to
    /// <c>write.metadata.previous-versions-max</c>).
    /// </summary>
    public TableMetadata Build(string? newMetadataLocation = null)
    {
        var result = _m;
        if (_changes.Count > 0) result = result with { LastUpdatedMs = Math.Max(result.LastUpdatedMs, _clock()) };

        if (_base?.MetadataFileLocation is { } prev && prev != newMetadataLocation)
        {
            var max = int.TryParse(result.Property(MetadataPreviousVersionsMax, ""), out var n) ? Math.Max(1, n) : MetadataPreviousVersionsMaxDefault;
            var log = result.MetadataLog.Append(new MetadataLogEntry(_base.LastUpdatedMs, prev)).ToList();
            if (log.Count > max) log = log.Skip(log.Count - max).ToList();
            result = result with { MetadataLog = log };
        }

        Validate(result);
        return result with { MetadataFileLocation = newMetadataLocation };
    }

    private static void Validate(TableMetadata m)
    {
        m.SchemaById(m.CurrentSchemaId);
        m.SpecById(m.DefaultSpecId);
        if (m.SortOrders.All(o => o.OrderId != m.DefaultSortOrderId)) throw new InvalidOperationException("Default sort order missing");
        if (m.CurrentSnapshotId is { } id && m.SnapshotById(id) is null) throw new InvalidOperationException($"Current snapshot {id} missing");
        foreach (var (name, r) in m.Refs)
            if (m.SnapshotById(r.SnapshotId) is null) throw new InvalidOperationException($"Ref {name} points at missing snapshot {r.SnapshotId}");
        var maxField = m.Schemas.Max(s => s.HighestFieldId);
        if (maxField > m.LastColumnId) throw new InvalidOperationException($"last-column-id {m.LastColumnId} is below highest field id {maxField}");
    }
}
