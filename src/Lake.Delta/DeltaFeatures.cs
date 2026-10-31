namespace Lake.Delta;

/// <summary>
/// The reader side of the protocol gate (PROTOCOL.md "Protocol Evolution", "Table Features"): a table is read only when
/// every reader feature it lists is implemented here. Refusals name every missing feature at once.
/// </summary>
public static class DeltaFeatures
{
    /// <summary>The highest legacy reader version understood (3 = the <c>readerFeatures</c> list).</summary>
    public const int MaxReaderVersion = 3;

    /// <summary>Reader features this reader implements.</summary>
    public static readonly IReadOnlySet<string> SupportedReaderFeatures = new HashSet<string>(StringComparer.Ordinal)
    {
        "columnMapping",
        "deletionVectors",
        "timestampNtz",
        "typeWidening",
        "v2Checkpoint",
        // Readers need only acknowledge it (PROTOCOL.md "Reader Requirements for Vacuum Protocol Check").
        "vacuumProtocolCheck",
    };

    /// <summary>
    /// Every reader-writer feature the protocol defines. A writer feature list naming one of them means the table may use
    /// it, so the reader checks those too (delta-spark 3.1 lists <c>columnMapping</c> only in <c>writerFeatures</c> on
    /// reader version 3; see DAT's iceberg_compat_v1 case).
    /// </summary>
    public static readonly IReadOnlySet<string> ReaderWriterFeatures = new HashSet<string>(StringComparer.Ordinal)
    {
        "columnMapping", "deletionVectors", "timestampNtz", "v2Checkpoint", "vacuumProtocolCheck", "typeWidening",
        "variantType", "variantShredding", "catalogManaged",
    };

    /// <summary>The reader features <paramref name="protocol"/> requires (explicit ones and the legacy-version ones).</summary>
    public static IReadOnlySet<string> RequiredReaderFeatures(DeltaProtocol protocol)
    {
        var features = new HashSet<string>(StringComparer.Ordinal);
        if (protocol.MinReaderVersion == 2) features.Add("columnMapping");
        if (protocol.MinReaderVersion >= 3)
        {
            foreach (var f in protocol.ReaderFeatures ?? []) features.Add(f);
            foreach (var f in protocol.WriterFeatures ?? [])
                if (ReaderWriterFeatures.Contains(f)) features.Add(f);
        }
        return features;
    }

    /// <summary>Whether <paramref name="feature"/> is supported (listed) by the table, legacy versions included.</summary>
    public static bool IsSupportedBy(DeltaProtocol protocol, string feature) => RequiredReaderFeatures(protocol).Contains(feature);

    /// <summary>Refuse <paramref name="protocol"/> unless this reader implements all it requires.</summary>
    public static void EnsureReadable(DeltaProtocol protocol)
    {
        if (protocol.MinReaderVersion > MaxReaderVersion)
            throw new DeltaUnsupportedFeatureException(
                $"The table needs reader version {protocol.MinReaderVersion}; Lake.Delta reads up to version {MaxReaderVersion}",
                [$"minReaderVersion={protocol.MinReaderVersion}"]);
        var missing = RequiredReaderFeatures(protocol).Where(f => !SupportedReaderFeatures.Contains(f)).Order(StringComparer.Ordinal).ToList();
        if (missing.Count > 0)
            throw new DeltaUnsupportedFeatureException(
                $"The table uses reader features Lake.Delta does not implement: {string.Join(", ", missing)}", missing);
    }

    /// <summary>
    /// <c>delta.columnMapping.mode</c>, honoured only when the protocol supports column mapping (reader version 2, or the
    /// <c>columnMapping</c> feature), as PROTOCOL.md "Reader Requirements for Column Mapping" says.
    /// </summary>
    public static ColumnMappingMode ColumnMapping(DeltaProtocol protocol, DeltaMetadata metadata)
    {
        if (!IsSupportedBy(protocol, "columnMapping")) return ColumnMappingMode.None;
        return metadata.Configuration.GetValueOrDefault("delta.columnMapping.mode") switch
        {
            null or "none" => ColumnMappingMode.None,
            "name" => ColumnMappingMode.Name,
            "id" => ColumnMappingMode.Id,
            var other => throw new InvalidDataException($"Unknown delta.columnMapping.mode '{other}'"),
        };
    }

    /// <summary>
    /// The type widenings PROTOCOL.md "Type Widening" allows; the reader converts old files' values to the current type.
    /// A <c>delta.typeChanges</c> entry outside this list is refused.
    /// </summary>
    internal static bool IsSupportedWidening(DeltaType from, DeltaType to)
    {
        static int IntRank(DeltaType t) => t == DeltaPrimitiveType.Byte ? 1 : t == DeltaPrimitiveType.Short ? 2 : t == DeltaPrimitiveType.Integer ? 3 : t == DeltaPrimitiveType.Long ? 4 : 0;
        if (from == to) return true;
        if (IntRank(from) > 0 && IntRank(to) > IntRank(from)) return true;
        if (from == DeltaPrimitiveType.Float && to == DeltaPrimitiveType.Double) return true;
        if (IntRank(from) is 1 or 2 or 3 && to == DeltaPrimitiveType.Double) return true;
        if (from == DeltaPrimitiveType.Date && to == DeltaPrimitiveType.TimestampNtz) return true;
        if (to is DeltaDecimalType d)
        {
            if (from is DeltaDecimalType f) return d.Precision - f.Precision >= d.Scale - f.Scale && d.Scale >= f.Scale;
            if (IntRank(from) is 1 or 2 or 3) return d.Precision - d.Scale >= 10;
            if (IntRank(from) == 4) return d.Precision - d.Scale >= 20;
        }
        return false;
    }

    /// <summary>Refuse a schema whose <c>delta.typeChanges</c> record a widening this reader cannot convert.</summary>
    internal static void EnsureTypeChangesSupported(DeltaStructType schema)
    {
        foreach (var (path, field) in DeltaSchema.AllFields(schema))
        {
            if (field.Metadata["delta.typeChanges"] is not System.Text.Json.Nodes.JsonArray changes) continue;
            foreach (var change in changes)
            {
                if (change is not System.Text.Json.Nodes.JsonObject c) throw new InvalidDataException($"{path}: delta.typeChanges entry is not an object");
                var from = ParsePrimitive(Json.ReqStr(c, "fromType", path), path);
                var to = ParsePrimitive(Json.ReqStr(c, "toType", path), path);
                if (!IsSupportedWidening(from, to))
                    throw new DeltaUnsupportedFeatureException($"{path}: type change {from} -> {to} is not a supported widening", ["typeWidening"]);
            }
        }

        static DeltaType ParsePrimitive(string name, string path) => DeltaSchema.ParseType(System.Text.Json.Nodes.JsonValue.Create(name), path);
    }
}
