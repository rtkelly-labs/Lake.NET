/// A toy model for the translator's own tests: two counters that may each be bumped up to a limit, with an optional
/// bug that lets one overshoot. Small enough that TLC checks it in a second.
[<ReflectedDefinition>]
module FsTla.Tests.Toy

type Counter =
    | A
    | B

type Bump = { Counter: Counter; By: int }

type Config = { Limit: int; Buggy: bool }

type State =
    { Counts: Map<Counter, int>
      Last: Counter option
      Seen: Set<Counter> }

let bump (cfg: Config) (b: Bump) (s: State) : State option =
    let current = s.Counts.[b.Counter]
    let allowed = if cfg.Buggy then current < cfg.Limit else current + b.By <= cfg.Limit
    if allowed then
        Some
            { Counts = Map.add b.Counter (current + b.By) s.Counts
              Last = Some b.Counter
              Seen = Set.add b.Counter s.Seen }
    else
        None

let withinLimit (cfg: Config) (s: State) = Map.forall (fun _ n -> n <= cfg.Limit) s.Counts

let lastWasSeen (cfg: Config) (s: State) =
    match s.Last with
    | Some c -> Set.contains c s.Seen
    | None -> Set.isEmpty s.Seen

/// Not translatable: a mutable loop is outside the subset.
let unsupported (cfg: Config) (b: Bump) (s: State) : State option =
    let mutable n = 0
    while n < b.By do
        n <- n + 1
    if n > cfg.Limit then None else Some s
