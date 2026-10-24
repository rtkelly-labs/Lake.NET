# Typed queries and writes (`Iceberg.Net.Linq`)

`Iceberg.Net.Linq` maps C# classes and records to Iceberg rows by field ID. It adds a LINQ provider that turns
a query into one `ScanRequest` (filter, columns, snapshot) and adds bulk writes shaped like EF Core's
(`ExecuteDelete`, `ExecuteUpdate`). It builds on the shared read contract in `Iceberg.Net.Data/Scan.cs` and on
`TableMutations`, and adds nothing to them. The EF Core provider ([efcore.md](efcore.md)) is built on this layer.

```csharp
var snake = new IcebergMapperOptions { NameConverter = IcebergNaming.SnakeCase };
var table = await catalog.CreateTableAsync<Order>(id, snake);   // schema from the model
var q = table.AsQueryable<Order>(new IcebergQueryOptions { Mapping = snake });

var open = await q.Where(o => o.Status == Status.Open && o.Qty > 2)
                  .OrderBy(o => o.Day).Take(20)
                  .Select(o => new { o.Id, o.Customer })
                  .ToListAsync();

q.Where(o => o.Qty > 2).ToScanRequest();   // Filter = qty > 2, Columns = null
q.Where(o => o.Qty > 2).ToQueryString();   // scan, residual, in-memory operators

await table.InsertAsync(newOrders, new ReflectionRowMapper<Order>(snake));
await table.UpsertAsync(changed, key: o => o.Id, mapper: new ReflectionRowMapper<Order>(snake));
await q.Where(o => o.Customer == "bob").ExecuteDeleteAsync();
await q.Where(o => o.Id == 7).ExecuteUpdateAsync(s => s.SetProperty(o => o.Note, "closed").SetProperty(o => o.Qty, o => o.Qty + 1));
await table.MergeAsync(incoming, on: o => o.Id, m => m.WhenMatchedUpdateAll().WhenNotMatchedInsert());
```

## Mapping

`IIcebergRowMapper<T>.Bind(schema, nameMapping)` resolves members to fields once per schema and returns an
`IIcebergRowBinding<T>`. The binding reads rows aligned to any scan schema (matched by field ID) and writes rows aligned
to the bound schema. `ReflectionRowMapper<T>` is the implementation used by default. A compile-time mapper, such as one
generated for `[ParquetColumn(FieldId = n)]` models, implements the same two interfaces. It fills
`IcebergMemberBinding` (member, field, CLR↔physical converters, nested bindings) so query translation works unchanged.

**Members.** Mapped members are public instance properties with a public getter that are not marked `[IcebergIgnore]`.
A get-only property is mapped only when a constructor parameter fills it, so computed properties like
`Label => …` are not columns. Construction uses a public parameterless constructor when every member has a setter.
Otherwise it uses the widest public constructor whose parameters match properties by name (positional records), then
setters or `init` accessors for the rest.

**Binding a member to a field**, in this order:

1. `[IcebergField(id)]` binds by field ID. This is the only binding that survives renames.
   `IcebergMapperOptions.FieldIds` supplies IDs for top-level members configured elsewhere. The EF Core provider uses
   it for `HasIcebergFieldId`.
2. The name, which is `[IcebergField(Name = "...")]` or the property name passed through
   `IcebergMapperOptions.NameConverter` (for example `IcebergNaming.SnakeCase`):
   1. an exact match on a column name;
   2. then an alias in the table's `schema.name-mapping.default`;
   3. then a case-insensitive match, only with `CaseInsensitive = true` and only if the match is unambiguous.
3. If nothing matches, the bind fails, unless `IgnoreUnmatchedMembers` is set.

Nested members bind by the same rules against the fields of their struct.

**Reads.**
- Members are matched by field ID. A member whose field is absent from the scan schema keeps its default: a column
  added later, a column projected away, or an older snapshot.
- A null column value read into a non-nullable value-type member becomes `default`.
- Promoted columns (int→long, float→double) read through the scan schema's type.

**Writes.**
- Every required column must be mapped, unless it has a write default.
- Writes never round silently. A decimal with more scale or precision than the column throws. So do a `DateTime` with
  a time of day written to a `date` column and a non-exact `double` written to a `float` column.
- Sub-microsecond ticks written to a micros timestamp are truncated, as Spark does.

| Member type | Iceberg type (default first) | Physical value |
|---|---|---|
| `bool` | boolean | bool |
| `int`, `short`, `byte`, `sbyte`, `ushort` | int (`int` also: date as raw days, long) | int |
| `long` | long (also time, timestamp[tz][_ns] as raw micros/nanos, int) | long |
| `uint` | long | long |
| `float` / `double` | float / double (`double` also float) | float / double |
| `decimal` | decimal(38, 18); set `[IcebergField(Type = "decimal(p, s)")]` | `IcebergDecimal` at the column's scale |
| `string` | string | string |
| `Guid` | uuid | Guid |
| `byte[]` | binary, fixed[L] | byte[] |
| `DateOnly` | date | int days |
| `DateTime` | timestamp (also timestamptz, date, timestamp[tz]_ns) | long micros/nanos. Reads give `Unspecified` for timestamp and `Utc` for timestamptz; writes treat `Unspecified` as UTC and convert `Local` |
| `DateTimeOffset` | timestamptz (also timestamp, _ns) | long micros (UTC). Reads come back at offset zero |
| `TimeOnly`, `TimeSpan` | time | long micros |
| enum | int/long (the underlying value), or string (the name, with `Type = "string"`) | int/long/string |
| class / record | struct | `object?[]` aligned to the struct's fields |
| `T[]`, `List<T>`, `IList<T>`, `IReadOnlyList<T>`, `IEnumerable<T>`, `HashSet<T>`, … | list | `List<object?>` |
| `Dictionary<K,V>`, `IDictionary<K,V>`, `IReadOnlyDictionary<K,V>` | map | `Dictionary<object, object?>` |

Every member type also maps as `Nullable<T>`.

**Schema from a model.** `IcebergSchema.FromType<T>()` builds the schema for a model:

- Field IDs come from `[IcebergField(id)]`. The other fields get the lowest free ID, siblings before children, which
  is the order catalogs use when they assign IDs on create.
- The type comes from the member type, or from `[IcebergField(Type = ...)]` in the spec's text form.
- Nullability follows the member type: nullable value types and NRT-nullable references are optional. Override it with
  `[IcebergField(Nullability = ...)]`.
- `[IcebergKey]` members become identifier fields, which is the default upsert key.

`catalog.CreateTableAsync<T>(id)` creates the table and keeps the model's field IDs
(`CreateTableRequest.PreserveFieldIds`). Catalogs that build the metadata locally (in-memory, Glue, JDBC) keep them as
given, gaps included. A REST server builds the metadata itself and may renumber, so for any other catalog a model whose
explicit IDs are not in the order a server assigns them is refused before anything is created.

## Query translation

At execution the chain is split into two parts:

- **The scan.** It covers every `Where` on the table's rows that comes before any operator other than
  `OrderBy`/`ThenBy`, since filters commute with sorting. It also covers the snapshot and the columns.
- **LINQ to Objects.** Everything else runs here, over the streamed and mapped rows.

Each top-level `&&` conjunct of those filters is translated on its own:

- **Exact:** the Iceberg filter keeps exactly the rows the C# predicate keeps. The conjunct is not evaluated again.
- **Partial:** a *superset* filter is pushed down, which is a sound pre-filter. The conjunct then runs in .NET as a
  *residual* on the materialized `T`.
- `OR` with one untranslatable side becomes `true` plus a residual. Half of an `OR` is never pushed down.
- Negating an inexact filter gives `true` plus a residual, because the complement of a superset is not a superset.

| C# (member `x.M`, constant or captured `c`) | Iceberg filter | Notes |
|---|---|---|
| `x.M == c`, `!=`, `<`, `<=`, `>`, `>=`, either side | `eq`/`not_eq`/`lt`/… | ordering only for numbers, decimal, dates/times/timestamps and int-stored enums |
| `x.M == null`, `!= null`, `x.M.HasValue` | `is_null` / `not_null` | |
| `x.Flag`, `!x.Flag`, `x.Flag == false` | `flag == true` / `!=` | |
| `c.Contains(x.M)` (array, `List`, `HashSet`, any `IEnumerable`) | `in` (`or is_null` if `c` holds null) | empty → `false` |
| `x.S.StartsWith("p")`, `StartsWith("p", StringComparison.Ordinal)` | `starts_with` | **ordinal**, as Iceberg compares; the one-argument .NET overload is culture-sensitive |
| `string.IsNullOrEmpty(x.S)`, `x.S.Equals(c)` | `is_null or == ''`, `eq` | |
| `x.Struct.Field op c` | `struct.field op c` | dotted names through struct columns |
| `&&`, `\|\|`, `!` | `and`, `or`, exact complement | `!(a < 5)` → `a >= 5 or a is null [or a is nan]` |
| enum stored by name, ordered (`x.Stage > Status.Open`) | residual | names do not sort like values |
| member vs member, method calls (`Length`, `Contains` on strings, `ToUpper`, …), computed members | residual | |
| constants the column cannot hold exactly (`x.IntCol < 2.5`, sub-µs ticks, decimal with more scale, local `DateTime` vs UTC) | residual | literal round-trip check |
| `byte[] ==` | residual | reference equality in C# |
| a column renamed since a time-travel snapshot | pushed down under the snapshot's name | the core binds the filter to the snapshot's schema |
| a column dropped since, or added after, a time-travel snapshot | residual | |

The rules follow C# semantics, not SQL's. A lifted comparison with null is false and its negation is true. A
non-nullable member reads a null column as its default, so `x.Qty == 0` over an optional `qty` becomes
`qty == 0 or qty is null`. The query-matrix tests check every row against LINQ to Objects over the same rows.

**Strict mode.** With `IcebergQueryOptions.Strict` set, a query throws `InvalidOperationException` when any `Where`
would leave a residual, or when a filter comes after paging or projection. This is EF Core's client-evaluation rule.
Sorting, paging and projection still run in memory.

**Columns.** `ScanRequest.Columns` lists only the top-level columns that the residual filters, in-memory filters,
sort keys and `Select` read, while rows are still `T`. A nested member reads its top-level column. `Count`, `LongCount`
and `Any` with no residual read no columns. The full mapped column set is read in two cases:

- a `T` is returned;
- a `T` is passed whole to an operator or method, or an unmapped (computed) member is read.

**Operators.**

- `Where`, `Select`, `Skip` and `Take` stream, both sync and async. `Take`/`First` stop the scan within one row of
  the limit, and `Any` stops at the first row.
- `OrderBy`/`ThenBy(Descending)`, `GroupBy`, aggregates and other operators run in LINQ to Objects. Async execution
  buffers the scanned rows first.
- Without `OrderBy`, rows come back in scan order (file order).
- `Count()`/`LongCount()` with no residual use `TableScan.CountAsync`, which answers from manifest record counts when
  there is no filter and no delete file, and otherwise decodes only the filter columns. `Any()` stops at the first row.
- Async terminals: `ToListAsync`, `ToArrayAsync`, `First[OrDefault]Async`, `Single[OrDefault]Async`, `CountAsync`,
  `LongCountAsync`, `AnyAsync`, `AsAsyncEnumerable`. They live in this package and do not depend on EF Core.
- Sync enumeration blocks on the async scan without capturing the synchronization context.

**Time travel.** `query.AsOf(instant)`, `.AtSnapshot(id)` and `.AtRef("tag-or-branch")` set `ScanRequest.Snapshot`.
They can appear anywhere in the chain. Rows are read with that snapshot's schema.

**Inspection.** `query.ToScanRequest()` returns the scan. `query.ToQueryString()` returns the scan's filter and
columns, the residual and the in-memory operators.

## Typed writes

Each call commits one snapshot through `TableMutations`.

| Call | Becomes |
|---|---|
| `table.InsertAsync(items)` | `InsertAsync(rows)` |
| `table.UpsertAsync(items, key: x => x.Id / x => new { x.A, x.B })` | `UpsertAsync(rows, keyColumns)`; no key → identifier fields |
| `query.ExecuteDeleteAsync()` | exact filter → `DeleteWhereAsync(Expression)`. With a residual → `DeleteWhereAsync(Expression pushed, refine: residual(T))`: the pushed superset still prunes files by partition and metrics, and the residual decides each remaining row. Strict queries throw. |
| `query.ExecuteUpdateAsync(s => s.SetProperty(x => x.P, value).SetProperty(x => x.Q, x => x.Q + 1))` | `UpdateAsync(filter, RowUpdate)`; each value is a constant (captured values evaluated once) or a `Func<object?[], object?>` that maps the row to `T`, evaluates the expression and converts the result to the column's physical value. Filters must translate exactly. |
| `table.MergeAsync(items, on: x => x.Id, m => m.WhenMatchedUpdate(s => s.SetProperty(x => x.Qty, (t, src) => t.Qty + src.Qty)).WhenMatchedUpdateAll().WhenMatchedDelete((t, s) => …).WhenNotMatchedInsert(s => …).WhenNotMatchedBySourceDelete(x => …))` | `MergeAsync(rows, on, MergeBuilder)`; conditions see both rows as `T`; source values in assignments are looked up by the target row's key; the by-source filter must translate exactly |

`ExecuteDeleteAsync` and `ExecuteUpdateAsync` accept only `Where` before them, on the current snapshot.
`IcebergWrites.CompileSetters` exposes the setter → `RowUpdate` translation for providers and tests.

## Limitations

- One table per query: no joins. Join in memory after `ToListAsync()`.
- No filters through lists or maps, and no `Any()` over collection members, because the core cannot filter through
  them.
- `ToLower`/`Contains`/`EndsWith`, string ordering and arithmetic in filters are residuals.
- Projection decodes only the columns the query uses (the core reads by field ID, row group by row group), but a data
  file's bytes are still fetched whole.
- A time-travel filter on a column dropped since that snapshot (or added after it) is evaluated as a residual on `T`.
