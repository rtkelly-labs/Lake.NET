# EF Core provider (`Iceberg.Net.EntityFrameworkCore`)

A non-relational EF Core 8 provider (the major matches the repository's `net8.0` target), in the same family as the
Cosmos provider. It is built on [`Iceberg.Net.Linq`](linq.md): EF Core supplies the model, the change tracker
and the familiar API. The LINQ layer runs every query and bulk write, and `TableMutations` commits every change.

```csharp
public class Shop(DbContextOptions options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.UseIcebergSnakeCaseNames();                       // OrderLine.UnitPrice -> order_line.unit_price
        b.Entity<Order>(o =>
        {
            o.ToIcebergTable("orders");
            o.Property(x => x.Amount).HasPrecision(12, 2);  // decimal(12, 2)
            o.HasIcebergPartition(x => x.CreatedAt, "day");
        });
    }
}

var options = new DbContextOptionsBuilder<Shop>()
    .UseIceberg(catalog, "sales", o => o.DeleteStrategy(DeleteStrategy.PositionDeletes))
    .Options;
await using var db = new Shop(options);
await db.Database.EnsureCreatedAsync();

db.Orders.Add(new Order { Id = 1, Customer = "alice", CreatedAt = DateTime.UtcNow });
await db.SaveChangesAsync();                                // one snapshot

var open = await db.Orders.Where(o => o.Qty > 2).OrderBy(o => o.Id).Take(20).ToListAsync();  // qty > 2 pushed down
var then = await db.Orders.AsOfSnapshot(snapshotId).CountAsync();                             // time travel
await db.Orders.Where(o => o.Customer == "bob").ExecuteDeleteAsync();
await db.Orders.Where(o => o.Id == 7).ExecuteUpdateAsync(s => s.SetProperty(o => o.Qty, o => o.Qty + 1));
db.Orders.Where(o => o.Qty > 2).ToIcebergQueryString();     // scan filter, columns, residual, in-memory operators
```

The context does not own the catalog. Dispose the catalog yourself.

## Model

| EF Core | Iceberg |
|---|---|
| entity type | a table in the context's namespace. The name comes from `ToIcebergTable("…")`; otherwise it is the CLR type name, snake-cased with `UseIcebergSnakeCaseNames()` |
| scalar property | a column. The name comes from `HasIcebergColumnName`, then `[IcebergField(Name = …)]`, then the property name (snake-cased if configured). Columns follow the CLR declaration order |
| property type | the LINQ layer's default type ([linq.md](linq.md#mapping)). `HasPrecision(p, s)` gives `decimal(p, s)`. `HasIcebergType("…")` or `[IcebergField(Type = …)]` overrides it, e.g. `timestamptz` or `string` for an enum stored by name |
| nullability | EF Core's: a property is optional unless it is non-nullable, `[Required]`/`IsRequired()`, or part of the key |
| `HasKey` / `[Key]` / convention key | identifier fields, and the key `SaveChanges` merges on |
| `HasIcebergFieldId(n)` / `[IcebergField(n)]` | the field ID. It is kept when the table is created, and the member binds by ID, so the binding survives renames. Other fields get the lowest free IDs |
| `HasIcebergPartition(x => x.P, "day")` | partition fields, in call order. A transform is written in the spec's text form: `identity`, `year`, `month`, `day`, `hour`, `bucket[N]` or `truncate[W]` |
| `[ConcurrencyCheck]` / `IsConcurrencyToken()` | checked on update and delete (see [Concurrency](#concurrency)) |
| `[Timestamp]` / `IsRowVersion()` on an `int` or `long` | a row version. The provider writes 1 on insert and the old value + 1 on update |

Keys are never store-generated, because Iceberg has no identity columns. `Guid` keys still get EF Core's client-side
values. Set integer keys yourself.

**Not supported** (refused when the model is validated, so you find out at startup): navigations and relationships,
owned and complex types, inheritance, shadow properties, value converters, global query filters, and properties of
types the LINQ layer cannot map. Struct, list and map columns are not mapped by this provider; use the LINQ layer for
them. Two entity types may not share a table.

### Creating and evolving tables

- **`Database.EnsureCreated()`** creates the namespace and every table that is missing. Each table gets the model's
  schema, field IDs, identifier fields and partition spec. `FormatVersion(n)` and `TableProperties(…)` in `UseIceberg`
  apply to the tables it creates. Tables that already exist are left as they are.
- **`Database.EnsureDeleted()`** drops the model's tables with their data (purge). It leaves the namespace.
- **No migrations.** Evolve a table with `Table.UpdateSchemaAsync`, then change the model to match:

  ```csharp
  var table = await catalog.LoadTableAsync(TableIdentifier.Of("sales", "orders"));
  await table.UpdateSchemaAsync(u => u.AddColumn("channel", PrimitiveType.String).RenameColumn("note", "comment"));
  ```

  The table may have more columns than the model has properties. Reads ignore the extra columns, updates keep their
  values, and inserts leave them null; a required extra column without a write default cannot be inserted. A
  property whose column is missing fails on first use, with a message that names the fix. A column renamed in the
  table needs `HasIcebergColumnName`, or a field ID on the property, which keeps binding through renames.

## Queries

The provider replaces EF Core's query compiler (`IQueryCompiler`, an internal service, which is why the project
suppresses `EF1001`). A `DbSet` query is not translated by EF Core's pipeline. It is rewritten as a
`Iceberg.Net.Linq` query, with the `DbSet` swapped for `table.AsQueryable<T>()`, and runs there. As a result:

- **What is pushed down is exactly what [linq.md](linq.md#query-translation) lists.** Each `Where` conjunct becomes an
  exact, partial or residual filter, and the scan reads only the columns the query uses. `Count`, `LongCount` and
  `Any` answer from manifests where they can. `First` and `Take` stop the scan early. The other operators
  (`OrderBy`, `GroupBy`, `Sum`, `Max`, `Join`, …) run in LINQ to Objects over the streamed rows. An untranslatable
  filter is evaluated in memory, not refused. To refuse it, as EF Core refuses client evaluation, set
  `UseIceberg(…, o => o.Strict())`.
- **`EF.Property<T>(e, "Name")` works**, so `Find`/`FindAsync`, `Entry.Reload` and `GetDatabaseValues` work too.
- **Tracking.** Entities returned by a query are tracked, with identity resolution: an entity the context already
  tracks is returned as-is and is not overwritten. A projection is not tracked. `AsNoTracking()`, `AsTracking()` and
  `QueryTrackingBehavior` behave as in EF Core. `AsNoTrackingWithIdentityResolution()` is treated as no-tracking.
- **Time travel.** `AsOfSnapshot(DateTimeOffset)`, `AsOfSnapshot(long snapshotId)` and `AsOfRef("tag-or-branch")`
  play the role of SQL Server's `TemporalAsOf`. They read that snapshot with its own schema: a filter on a column
  renamed since then still prunes. Time-travel results are never tracked.
- **Other `DbSet`s in a query** (`Join(db.Customers, …)`, `Concat`) each run as a separate query when the in-memory
  part of the plan enumerates them. They read their current snapshot. A query scans one table; a join happens in
  memory.
- `TagWith`, `IgnoreQueryFilters`, `IgnoreAutoIncludes`, `AsSplitQuery` and `AsSingleQuery` are accepted and ignored.
  `Include` throws. `EF.CompileQuery` throws, because Iceberg queries are planned on every execution against the
  current metadata.
- Every query loads the table's current metadata, so it sees the latest commit.

`ToIcebergQueryString()` and `ToIcebergScanRequest()` show the plan without running it.

**Imports.** Do not import `Iceberg.Net.Linq` in a file that also uses `Microsoft.EntityFrameworkCore`'s async
operators. Both define `ToListAsync`, `CountAsync` and other operators with the same names, so the calls become
ambiguous. EF Core's operators route to the provider, so `Microsoft.EntityFrameworkCore` is the only import you need.

## Bulk updates and deletes

The relational package's `ExecuteDelete[Async]` and `ExecuteUpdate[Async]` work. The provider references
`Microsoft.EntityFrameworkCore.Relational` only for these operators and for `SetPropertyCalls<T>`; it registers no
relational services. They map onto the LINQ layer's `ExecuteDeleteAsync` and `ExecuteUpdateAsync`, and each commits
one snapshot. They bypass the change tracker, as in EF Core, and return the number of rows deleted or updated.

- **Delete.** The translated part of the filter prunes files, and a residual decides each remaining row.
- **Update.** The filter must translate completely. Setter values are constants, captured values, or expressions over
  the row.
- **Strategy.** `UseIceberg(…, o => o.DeleteStrategy(…))` chooses how rows are removed (copy-on-write, or position
  deletes / DVs). By default each table's `write.delete.mode` or `write.update.mode` decides.

## SaveChanges

For each table with changes, the added, modified and deleted entities become **one MERGE on the primary key**
(`TableMutations.MergeAsync`, or `StageMergeAsync` when several tables commit together), so the table gets **exactly one
snapshot**:

| Entity state | MERGE clause | When the key is… |
|---|---|---|
| Added | `WHEN NOT MATCHED THEN INSERT` | …already in the table: `DbUpdateException`, and nothing is committed for that table (for any table, when the save is [atomic](#atomicity-across-tables)) |
| Modified | `WHEN MATCHED THEN UPDATE`. Every mapped column comes from the entity; unmapped columns keep their values | …missing: `DbUpdateConcurrencyException` (EF Core's "expected one row, affected zero") |
| Deleted | `WHEN MATCHED THEN DELETE` | …missing: `DbUpdateConcurrencyException` |
| Deleted, then Added with the same key | `UPDATE` with the new entity's values | …missing: `DbUpdateConcurrencyException` |

Only files that the source keys can reach are read, through key and metrics pruning. All checks run **inside** the
merge, against the table state the commit lands on. When another writer commits first, the core re-plans the whole
merge on the new state and runs the checks again. An unrelated concurrent change therefore merges instead of being
lost, and a conflicting one is reported. When the commit retries run out, the result is
`DbUpdateConcurrencyException`. A `ValidationException` or `CommitStateUnknownException` becomes `DbUpdateException`.

### Atomicity across tables

When a `SaveChanges` spans several tables and the catalog can commit several tables atomically
(`ICatalog.SupportsMultiTableTransactions`: `InMemoryCatalog`, `JdbcCatalog`, and a `RestCatalog` whose server
advertises `POST /v1/{prefix}/transactions/commit`, as `Iceberg.Net.RestServer` does), **the whole save is one
catalog transaction**:

1. Each table's MERGE is **staged** (`TableMutations.StageMergeAsync`): planned against the table's current state, its
   files written, its commit (requirements and updates) returned instead of applied.
2. `StagedCommit.CommitAllAsync` sends every table's commit in one `CommitTransactionAsync`. Either every table gets
   its snapshot or none does.
3. When another writer moved any of the tables first (`CommitFailedException`), **every** table is refreshed and every
   MERGE planned again, so the key, token and `RequireUnchangedSnapshot` checks run against the state the transaction
   lands on. Then the transaction is retried with back-off (the largest `commit.retry.*` budget of the tables).

Nothing is accepted before the transaction succeeds. On any failure (a duplicate key found while staging table B, a
token found changed on a re-plan, retries spent) no table changes, every entity keeps its state, and the files written
for the abandoned plans are deleted (manifests, manifest lists and delete files; data files are left for orphan-file
cleanup, as for any lost attempt). A save of one table commits directly, as before.

**Catalogs without multi-table transactions** (Glue; a REST server that does not advertise the endpoint, such as
Gravitino 1.3) fall back to committing **one table after another**, in table-name order. Each table's commit is
atomic, the save is not: when table B fails after table A committed, A's change stays. To make a retry replay only what
failed, the provider then marks A's entities as saved (Unchanged, or Detached for deletes) and rethrows B's error; B's
entities keep their states. To refuse that instead, set `UseIceberg(…, o => o.RequireAtomicSaveChanges())`: a save
that spans tables then throws `NotSupportedException` before anything is written (a save of one table still works).

`BeginTransaction` throws `NotSupportedException`: a transaction cannot stay open across calls, because the catalog
commits all tables in one request.

### Concurrency

- **Concurrency tokens** (`[ConcurrencyCheck]`, `IsConcurrencyToken()`). On update and delete, the token's original
  value (what the context read) must equal the value in the row the merge matched. Otherwise the result is
  `DbUpdateConcurrencyException`, with the entry. Resolve it the EF Core way: `entry.Reload()` or
  `GetDatabaseValues()`, then save again.
- **Row versions** (`[Timestamp]`/`IsRowVersion()` on `int`/`long`). These are tokens that the provider generates: 1
  on insert, and the old value + 1 on update. The new value is written back to the entity after a successful save.
  `byte[]` row versions are not supported.
- **Table-level optimistic concurrency** (`UseIceberg(…, o => o.RequireUnchangedSnapshot())`). This is the Iceberg
  form: `SaveChanges` fails with `DbUpdateConcurrencyException` when a table holding modified or deleted entities has
  moved past the snapshot that the context first read it at, even if a different row changed. Inserts alone never
  conflict. The check also runs inside the merge, so a racing commit is caught. The read snapshot is reset after each
  successful save of that table.

Without tokens or `RequireUnchangedSnapshot`, the last writer of a row wins, at row granularity. Another writer's
changes to other rows are kept.

## Supported and tested

The `tests/Iceberg.Net.EntityFrameworkCore.Tests` suite runs against `InMemoryCatalog` and covers:

- the model, field IDs, partitioning, `EnsureCreated`/`EnsureDeleted`;
- pushdown, residuals and strict mode;
- the operators, sync and async;
- `Find`, tracking and identity resolution;
- time travel;
- joins across sets;
- `ExecuteUpdate`/`ExecuteDelete`;
- `SaveChanges` for every state, replace, duplicate keys, missing rows and preserved extra columns;
- row versions and tokens, including against a racing commit;
- `RequireUnchangedSnapshot`;
- atomic `SaveChanges` across tables: one transaction, a failure in a later table leaving earlier ones unchanged, a
  token changed by a racing commit caught on the re-plan, an unrelated racing commit merged;
- the per-table fallback and `RequireAtomicSaveChanges` on a catalog without transactions.

`tests/Iceberg.Net.RestServer.Tests/EfCoreTransactionTests` runs atomic `SaveChanges` against the REST server
in-process (`WebApplicationFactory`, both stores), including a conflict the server reports (409) on table B that rolls
back table A.

Not provided:

- migrations;
- explicit transactions (`BeginTransaction`);
- compiled queries;
- navigations, owned and complex types;
- struct, list and map columns;
- value converters.
