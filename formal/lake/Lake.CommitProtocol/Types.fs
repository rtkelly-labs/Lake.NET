/// The vocabulary of Lake.NET's commit protocol: who commits, what the catalog and object store hold, and the
/// actions that change them. Action is the shared contract: the TLC model steps by it, the conformance tests drive
/// the real catalog by it, and recorded runs (traces) are read as sequences of it.
namespace Lake.CommitProtocol

type Writer =
    | W1
    | W2
    | W3

type Part =
    | P
    | Q

/// What a writer commits: an append of one data file, an overwrite that replaces a partition's live files with one
/// (SnapshotProducer.Overwrite), or a delete of a partition's live files (SnapshotProducer.DeleteFiles).
type Op =
    | Append of Part
    | Overwrite of Part
    | Delete of Part

/// Files in the object store: the seed data file (partition P), one data file per writer, the initial metadata file,
/// and the metadata file each writer writes for each commit attempt.
type File =
    | Seed
    | Data of writer: Writer
    | Meta0
    | Meta of writer: Writer * attempt: int

/// One snapshot in the table history. SeqNo is the sequence number; Start is the last sequence number the writer saw
/// when it began (what serializable validation compares against).
type Snapshot =
    { SeqNo: int
      Writer: Writer option
      Attempt: int
      Adds: Set<File>
      Removes: Set<File>
      Start: int }

/// Where a writer's commit stands. Waiting: attempt `attempt` has been sent to the catalog. `removedAtStart` is what the
/// first attempt removed: each retry re-plans its removes, but validates that these are all still live.
type Phase =
    | Idle
    | Waiting of attempt: int * start: int * removedAtStart: Set<File>
    | Succeeded
    | Failed
    | Unknown

/// Deliberate protocol bugs, each of which TLC must catch.
type Bug =
    /// Retry after an unknown outcome without first reloading to see whether the commit landed.
    | RetryUnknownBlindly
    /// Delete the new metadata file after an unknown outcome, as if the commit had lost.
    | DeleteOnUnknown
    /// Skip retry-time validation of an overwrite or delete altogether (WithSnapshotIsolation): a retry only re-plans.
    | SnapshotIsolation
    /// A catalog that swaps the pointer without comparing it to the expected location.
    | BlindSwap
    /// A resent swap request carries no idempotency key, so the catalog evaluates it again as a new request.
    | NoIdempotencyKey
    /// Check a retry for concurrently added files but not for concurrently removed ones (no failMissingDeletePaths).
    | NoMissingDeleteCheck

type Config =
    { Ops: Map<Writer, Op>
      /// Commit attempts per writer: the first plus retries.
      MaxAttempts: int
      /// Failed replies (server errors and timeouts) the environment may inject in one run.
      Faults: int
      Bugs: Set<Bug> }

type State =
    {
        /// The catalog's pointer: the current metadata file.
        Pointer: File
        /// The table history each metadata file records.
        Metadata: Map<File, Set<Snapshot>>
        /// Files that exist in the object store.
        Store: Set<File>
        /// Swap requests sent but not yet applied: new metadata file -> the location expected to be current.
        InFlight: Map<File, File>
        /// Applied requests whose reply has not reached the writer: new metadata file -> whether the swap won.
        Replies: Map<File, bool>
        Writers: Map<Writer, Phase>
        /// Faults left to inject.
        Faults: int
        /// Every swap request sent: new metadata file -> the location it expects. An idempotency key lets the catalog
        /// recognise a resend of one of these.
        Sent: Map<File, File>
        /// Ghost: snapshots whose commit was acknowledged to the writer.
        Published: Set<Snapshot>
    }

/// One step of the protocol. Begin is the writer's; Apply is the catalog's; the rest are how the reply to attempt
/// `attempt` reaches the writer.
type Action =
    /// The writer writes its data file, reads the table, writes metadata attempt 1 and sends the swap.
    | Begin of writer: Writer
    /// The catalog performs the compare-and-swap of the request for this attempt.
    | Apply of writer: Writer * attempt: int
    /// The writer hears that the swap won.
    | Ack of writer: Writer * attempt: int
    /// The writer hears that the swap lost (CommitFailedException): it deletes that metadata file and retries.
    | Reject of writer: Writer * attempt: int
    /// The catalog answers with an error (CommitStateUnknownException, ServerResponded = true): the request is
    /// finished, applied or not, and the writer reloads to find out which.
    | ServerError of writer: Writer * attempt: int
    /// No answer (CommitStateUnknownException, ServerResponded = false): the request may still be applied later.
    | Timeout of writer: Writer * attempt: int
    /// No answer, and the client resends the same request with the same Idempotency-Key (RestCatalog's idempotent
    /// retry, made only when the server advertises a key lifetime). The writer keeps waiting for attempt `attempt`.
    | Resend of writer: Writer * attempt: int
