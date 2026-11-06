/// The commit protocol as a step function, and its invariants. Everything here is translated to TLA+, so it stays
/// inside FsTla's translatable subset (see FsTla.Translate).
///
/// It follows SnapshotProducer.CommitAsync and MetastoreCatalog's write-then-swap: a writer writes a new metadata file
/// on top of the current one and asks the catalog to swap the pointer. A lost swap deletes that file and retries
/// (re-validating an overwrite); a server error reloads the table and looks for the writer's snapshot before
/// retrying; a timeout ends the commit as unknown.
[<ReflectedDefinition>]
module Lake.CommitProtocol.Protocol

let has (bug: Bug) (cfg: Config) = Set.contains bug cfg.Bugs

let history (s: State) = s.Metadata.[s.Pointer]

let partOf (op: Op) =
    match op with
    | Append p -> p
    | Overwrite p -> p

let partitionOf (cfg: Config) (f: File) =
    match f with
    | Data w -> partOf cfg.Ops.[w]
    | _ -> P

let liveFiles (h: Set<Snapshot>) =
    Set.difference (Set.unionMany (Set.map (fun x -> x.Adds) h)) (Set.unionMany (Set.map (fun x -> x.Removes) h))

let byWriter (w: Writer) (h: Set<Snapshot>) = Set.filter (fun x -> x.Writer = Some w) h

let attemptOf (w: Writer) (k: int) (h: Set<Snapshot>) = Set.filter (fun x -> x.Writer = Some w && x.Attempt = k) h

let isMetaOf (w: Writer) (f: File) =
    match f with
    | Meta(x, _) -> x = w
    | _ -> false

let awaiting (w: Writer) (k: int) (s: State) =
    match s.Writers.[w] with
    | Waiting(a, _, _) -> a = k
    | _ -> false

let addsTo (cfg: Config) (p: Part) (x: Snapshot) = Set.exists (fun f -> partitionOf cfg f = p) x.Adds

/// Retry-time validation: an overwrite fails if a file was added to its partition after it began.
let valid (cfg: Config) (w: Writer) (start: int) (s: State) =
    match cfg.Ops.[w] with
    | Append _ -> true
    | Overwrite p -> has SnapshotIsolation cfg || not (Set.exists (fun x -> x.SeqNo > start && addsTo cfg p x) (history s))

/// Write metadata attempt k on top of the current metadata and send the swap.
let propose (w: Writer) (k: int) (start: int) (removes: Set<File>) (s: State) =
    let h = history s
    let snapshot =
        { SeqNo = Set.count h + 1
          Writer = Some w
          Attempt = k
          Adds = Set.singleton (Data w)
          Removes = removes
          Start = start }
    let meta = Meta(w, k)
    { s with
        Metadata = Map.add meta (Set.add snapshot h) s.Metadata
        Store = Set.add meta s.Store
        InFlight = Map.add meta s.Pointer s.InFlight
        Sent = Map.add meta s.Pointer s.Sent
        Writers = Map.add w (Waiting(k, start, removes)) s.Writers }

/// After attempt k definitely did not land: refresh, validate and try again, or give up.
let retry (cfg: Config) (w: Writer) (k: int) (s: State) =
    match s.Writers.[w] with
    | Waiting(_, start, removes) ->
        if k < cfg.MaxAttempts && valid cfg w start s then Some(propose w (k + 1) start removes s)
        else Some { s with Writers = Map.add w Failed s.Writers }
    | _ -> None

let beginCommit (cfg: Config) (w: Writer) (s: State) =
    if s.Writers.[w] <> Idle then None
    else
        let h = history s
        let removes =
            match cfg.Ops.[w] with
            | Append _ -> Set.empty
            | Overwrite p -> Set.filter (fun f -> partitionOf cfg f = p) (liveFiles h)
        Some(propose w 1 (Set.count h) removes { s with Store = Set.add (Data w) s.Store })

let apply (cfg: Config) (w: Writer) (k: int) (s: State) =
    let meta = Meta(w, k)
    if not (Map.containsKey meta s.InFlight) then None
    else
        let won = has BlindSwap cfg || s.Pointer = s.InFlight.[meta]
        Some
            { s with
                Pointer = (if won then meta else s.Pointer)
                InFlight = Map.remove meta s.InFlight
                Replies = (if awaiting w k s then Map.add meta won s.Replies else s.Replies) }

let ack (w: Writer) (k: int) (s: State) =
    let meta = Meta(w, k)
    if awaiting w k s && Map.containsKey meta s.Replies && s.Replies.[meta] then
        Some
            { s with
                Replies = Map.remove meta s.Replies
                Writers = Map.add w Succeeded s.Writers
                Published = Set.union s.Published (attemptOf w k s.Metadata.[meta]) }
    else None

let reject (cfg: Config) (w: Writer) (k: int) (s: State) =
    let meta = Meta(w, k)
    if awaiting w k s && Map.containsKey meta s.Replies && not s.Replies.[meta] then
        retry cfg w k { s with Replies = Map.remove meta s.Replies; Store = Set.remove meta s.Store }
    else None

/// The reply is lost or an error: the attempt's outcome is unknown to the writer. Consumes one fault.
let unknownOutcome (cfg: Config) (w: Writer) (k: int) (s: State) =
    let meta = Meta(w, k)
    s.Faults > 0
    && awaiting w k s
    && (Map.containsKey meta s.InFlight || Map.containsKey meta s.Replies)

let afterUnknown (cfg: Config) (w: Writer) (k: int) (s: State) =
    let meta = Meta(w, k)
    { s with
        Faults = s.Faults - 1
        Replies = Map.remove meta s.Replies
        Store = (if has DeleteOnUnknown cfg then Set.remove meta s.Store else s.Store) }

let serverError (cfg: Config) (w: Writer) (k: int) (s: State) =
    if not (unknownOutcome cfg w k s) then None
    else
        // The server answered, so the request is finished: it will never be applied now.
        let s1 = { afterUnknown cfg w k s with InFlight = Map.remove (Meta(w, k)) s.InFlight }
        let mine = attemptOf w k (history s1)
        if has RetryUnknownBlindly cfg then retry cfg w k s1
        elif not (Set.isEmpty mine) then
            Some
                { s1 with
                    Writers = Map.add w Succeeded s1.Writers
                    Published = Set.union s1.Published mine }
        else retry cfg w k s1

let timeout (cfg: Config) (w: Writer) (k: int) (s: State) =
    if not (unknownOutcome cfg w k s) then None
    else
        // The request may still be in flight and be applied later.
        let s1 = afterUnknown cfg w k s
        if has RetryUnknownBlindly cfg then retry cfg w k s1
        else Some { s1 with Writers = Map.add w Unknown s1.Writers }

/// The reply is lost and the client resends the request. With an idempotency key the catalog recognises it: a resend
/// of a request still in flight joins it, and a resend of an applied one gets the stored reply, so nothing changes but
/// the fault budget. Without a key it is a new request with the same expected location, applied (again) on its own.
let resend (cfg: Config) (w: Writer) (k: int) (s: State) =
    if not (unknownOutcome cfg w k s) then None
    elif has NoIdempotencyKey cfg then
        let meta = Meta(w, k)
        Some
            { s with
                Faults = s.Faults - 1
                InFlight = Map.add meta s.Sent.[meta] s.InFlight
                Replies = Map.remove meta s.Replies }
    else Some { s with Faults = s.Faults - 1 }

let step (cfg: Config) (action: Action) (s: State) : State option =
    match action with
    | Begin w -> beginCommit cfg w s
    | Apply(w, k) -> apply cfg w k s
    | Ack(w, k) -> ack w k s
    | Reject(w, k) -> reject cfg w k s
    | ServerError(w, k) -> serverError cfg w k s
    | Timeout(w, k) -> timeout cfg w k s
    | Resend(w, k) -> resend cfg w k s

// Invariants.

/// A commit is applied at most once: no writer has two snapshots in the current history.
let atMostOnce (cfg: Config) (s: State) =
    Map.forall (fun w _ -> Set.count (byWriter w (history s)) <= 1) cfg.Ops

/// Acknowledged commits stay in the history: a concurrent commit never loses one (appends never lose an append).
let noLostCommit (cfg: Config) (s: State) = Set.isSubset s.Published (history s)

/// A writer told its commit failed has nothing in the table and nothing left that could still land.
let failedNeverApplies (cfg: Config) (s: State) =
    Map.forall
        (fun w phase ->
            phase <> Failed
            || (Set.isEmpty (byWriter w (history s))
                && not (Map.exists (fun m _ -> isMetaOf w m) s.InFlight)))
        s.Writers

/// The current metadata file and every live data file exist.
let reachableFilesExist (cfg: Config) (s: State) =
    Set.contains s.Pointer s.Store && Set.isSubset (liveFiles (history s)) s.Store

/// No overwrite committed over a file added to its partition after it began (serializable overwrite).
let conflictFree (cfg: Config) (h: Set<Snapshot>) (o: Snapshot) =
    match o.Writer with
    | Some w ->
        match cfg.Ops.[w] with
        | Overwrite p -> not (Set.exists (fun x -> x.SeqNo > o.Start && x.SeqNo < o.SeqNo && addsTo cfg p x) h)
        | Append _ -> true
    | None -> true

let serializableOverwrite (cfg: Config) (s: State) =
    let h = history s
    Set.forall (fun o -> conflictFree cfg h o) h
