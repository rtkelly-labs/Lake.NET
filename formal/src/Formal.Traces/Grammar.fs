namespace Formal.Traces

/// A grammar symbol: a terminal (one event of the trace vocabulary) or a nonterminal (a protocol state, by name).
type Symbol<'T> =
    | T of 'T
    | N of string

/// One alternative of a rule. <c>Name</c> labels it in derivations and conflict reports.
type Production<'T> =
    { Lhs: string
      Name: string
      Rhs: Symbol<'T> list }

/// A nonterminal with a sentence of documentation, which parse errors quote ("state: ...").
type Rule<'T> =
    { Name: string
      Doc: string
      Alternatives: Production<'T> list }

/// What a predictive parser looks at next: an event, or the end of the trace.
[<RequireQualifiedAccess>]
type Lookahead<'T> =
    | Event of 'T
    | End

/// A context-free grammar over a trace vocabulary <c>'T</c>. Build it with <see cref="Grammar.create"/>, which rejects
/// undefined, unreachable and unproductive nonterminals.
type Grammar<'T when 'T: comparison> =
    { Name: string
      Start: string
      Rules: Rule<'T> list }

    member g.Productions = g.Rules |> List.collect _.Alternatives
    member g.Doc(nonterminal: string) =
        g.Rules |> List.tryFind (fun r -> r.Name = nonterminal) |> Option.map _.Doc |> Option.defaultValue ""
    member g.Terminals =
        g.Productions
        |> List.collect (fun p -> p.Rhs |> List.choose (function T t -> Some t | N _ -> None))
        |> Set.ofList

/// Why an LL(1) table cell holds more than one production.
type ConflictKind =
    /// Two alternatives can both start with the lookahead.
    | FirstFirst
    /// One alternative can derive nothing, and the lookahead can both start another and follow the rule.
    | FirstFollow

/// An LL(1) conflict: with <c>Lookahead</c> next while expanding <c>Nonterminal</c>, more than one alternative applies.
type Conflict<'T> =
    { Nonterminal: string
      Lookahead: Lookahead<'T>
      Productions: Production<'T> list
      Kind: ConflictKind }

/// A conflict-free LL(1) parse table. Its existence is the proof that the grammar is unambiguous: every trace has
/// at most one leftmost derivation, so every violation has exactly one explanation.
type Ll1Table<'T when 'T: comparison> =
    { Grammar: Grammar<'T>
      Cells: Map<string * Lookahead<'T>, Production<'T>> }

    /// The lookaheads a nonterminal accepts, in a stable order (events first, end of trace last).
    member t.Expected(nonterminal: string) =
        t.Cells
        |> Map.toList
        |> List.choose (fun ((n, la), _) -> if n = nonterminal then Some la else None)

[<RequireQualifiedAccess>]
module Grammar =

    /// A rule: <c>rule "Fresh" "the writer's view is current" [ "swap", [ T Applied; N "Done" ]; ... ]</c>.
    let rule (name: string) (doc: string) (alternatives: (string * Symbol<'T> list) list) : Rule<'T> =
        { Name = name
          Doc = doc
          Alternatives = alternatives |> List.map (fun (label, rhs) -> { Lhs = name; Name = label; Rhs = rhs }) }

    let private nonterminals (rhs: Symbol<'T> list) =
        rhs |> List.choose (function N n -> Some n | T _ -> None)

    /// Nonterminals that derive at least one finite trace.
    let productive (rules: Rule<'T> list) =
        let rec fix (known: Set<string>) =
            let next =
                rules
                |> List.filter (fun r ->
                    r.Alternatives |> List.exists (fun p -> nonterminals p.Rhs |> List.forall known.Contains))
                |> List.map _.Name
                |> Set.ofList
            if next = known then known else fix next
        fix Set.empty

    /// Build a grammar, rejecting the mistakes that would make its analyses meaningless.
    let create (name: string) (start: string) (rules: Rule<'T> list) : Grammar<'T> =
        let defined = rules |> List.map _.Name
        let duplicates = defined |> List.countBy id |> List.filter (fun (_, c) -> c > 1) |> List.map fst
        if not duplicates.IsEmpty then invalidArg (nameof rules) $"Rules defined twice: %A{duplicates}"
        let definedSet = Set.ofList defined
        if not (definedSet.Contains start) then invalidArg (nameof start) $"Start rule %s{start} is not defined"
        let undefined =
            rules
            |> List.collect (fun r -> r.Alternatives |> List.collect (fun p -> nonterminals p.Rhs))
            |> List.filter (definedSet.Contains >> not)
            |> List.distinct
        if not undefined.IsEmpty then invalidArg (nameof rules) $"Undefined nonterminals: %A{undefined}"
        let byName = rules |> List.map (fun r -> r.Name, r) |> Map.ofList
        let rec reach (seen: Set<string>) (todo: string list) =
            match todo with
            | [] -> seen
            | n :: rest when seen.Contains n -> reach seen rest
            | n :: rest ->
                let next = byName[n].Alternatives |> List.collect (fun p -> nonterminals p.Rhs)
                reach (seen.Add n) (next @ rest)
        let unreachable = Set.difference definedSet (reach Set.empty [ start ])
        if not unreachable.IsEmpty then invalidArg (nameof rules) $"Unreachable rules: %A{Set.toList unreachable}"
        let unproductive = Set.difference definedSet (productive rules)
        if not unproductive.IsEmpty then
            invalidArg (nameof rules) $"Rules that derive no finite trace: %A{Set.toList unproductive}"
        { Name = name; Start = start; Rules = rules }

    /// Nonterminals that can derive the empty trace.
    let nullable (g: Grammar<'T>) : Set<string> =
        let rec fix (known: Set<string>) =
            let next =
                g.Productions
                |> List.filter (fun p ->
                    p.Rhs |> List.forall (function N n -> known.Contains n | T _ -> false))
                |> List.map _.Lhs
                |> Set.ofList
            if next = known then known else fix next
        fix Set.empty

    let private firstOfSeq (nullables: Set<string>) (first: Map<string, Set<'T>>) (rhs: Symbol<'T> list) =
        let rec go acc symbols =
            match symbols with
            | [] -> acc, true
            | T t :: _ -> Set.add t acc, false
            | N n :: rest ->
                let acc = Set.union acc first[n]
                if nullables.Contains n then go acc rest else acc, false
        go Set.empty rhs

    /// FIRST sets: the events each nonterminal's traces can start with.
    let first (g: Grammar<'T>) : Map<string, Set<'T>> =
        let nullables = nullable g
        let rec fix (current: Map<string, Set<'T>>) =
            let next =
                g.Productions
                |> List.fold
                    (fun (m: Map<string, Set<'T>>) p ->
                        let f, _ = firstOfSeq nullables current p.Rhs
                        m.Add(p.Lhs, Set.union m[p.Lhs] f))
                    current
            if next = current then current else fix next
        fix (g.Rules |> List.map (fun r -> r.Name, Set.empty) |> Map.ofList)

    /// FOLLOW sets: what can come after each nonterminal (<see cref="Lookahead.End"/> for the end of the trace).
    let follow (g: Grammar<'T>) : Map<string, Set<Lookahead<'T>>> =
        let nullables = nullable g
        let firsts = first g
        let init =
            g.Rules
            |> List.map (fun r -> r.Name, (if r.Name = g.Start then Set.singleton Lookahead.End else Set.empty))
            |> Map.ofList
        let rec fix (current: Map<string, Set<Lookahead<'T>>>) =
            let mutable m = current
            for p in g.Productions do
                let rec walk symbols =
                    match symbols with
                    | [] -> ()
                    | T _ :: rest -> walk rest
                    | N n :: rest ->
                        let f, restNullable = firstOfSeq nullables firsts rest
                        let add = f |> Set.map Lookahead.Event
                        let add = if restNullable then Set.union add m[p.Lhs] else add
                        m <- m.Add(n, Set.union m[n] add)
                        walk rest
                walk p.Rhs
            if m = current then current else fix m
        fix init

    /// The LL(1) parse table, or every conflict that prevents one.
    let ll1 (g: Grammar<'T>) : Result<Ll1Table<'T>, Conflict<'T> list> =
        let nullables = nullable g
        let firsts = first g
        let follows = follow g
        let entries =
            g.Productions
            |> List.collect (fun p ->
                let f, isNullable = firstOfSeq nullables firsts p.Rhs
                let viaFirst = f |> Set.toList |> List.map (fun t -> (p.Lhs, Lookahead.Event t), (p, FirstFirst))
                let viaFollow =
                    if isNullable then
                        follows[p.Lhs] |> Set.toList |> List.map (fun la -> (p.Lhs, la), (p, FirstFollow))
                    else []
                viaFirst @ viaFollow)
            |> List.groupBy fst
        let conflicts =
            entries
            |> List.choose (fun ((n, la), cells) ->
                let prods = cells |> List.map (snd >> fst) |> List.distinct
                if prods.Length > 1 then
                    let kind = if cells |> List.exists (fun (_, (_, k)) -> k = FirstFollow) then FirstFollow else FirstFirst
                    Some { Nonterminal = n; Lookahead = la; Productions = prods; Kind = kind }
                else None)
        if conflicts.IsEmpty then
            Ok { Grammar = g; Cells = entries |> List.map (fun (k, cells) -> k, fst (snd cells.Head)) |> Map.ofList }
        else Error conflicts

    /// True when every alternative is a run of events followed by at most one nonterminal: the grammar then
    /// describes a regular language (it is a finite automaton whose states are the nonterminals).
    let isRightLinear (g: Grammar<'T>) =
        g.Productions
        |> List.forall (fun p ->
            match List.rev p.Rhs with
            | [] -> true
            | _ :: before -> before |> List.forall (function T _ -> true | N _ -> false))

    let private showLookahead (show: 'T -> string) la =
        match la with
        | Lookahead.Event t -> show t
        | Lookahead.End -> "end of trace"

    let showSymbol (show: 'T -> string) symbol =
        match symbol with
        | T t -> show t
        | N n -> n

    /// One line per alternative, in the notation the README uses.
    let render (show: 'T -> string) (g: Grammar<'T>) : string =
        let width = g.Rules |> List.map _.Name.Length |> List.max
        g.Rules
        |> List.collect (fun r ->
            r.Alternatives
            |> List.mapi (fun i p ->
                let rhs = if p.Rhs.IsEmpty then "ε" else p.Rhs |> List.map (showSymbol show) |> String.concat " "
                let head = if i = 0 then r.Name.PadRight width + " := " else System.String(' ', width) + "  | "
                $"%s{head}%s{rhs}"))
        |> String.concat "\n"

    /// A conflict as a sentence, Menhir-style: which rule, which lookahead, which alternatives compete.
    let explainConflict (show: 'T -> string) (c: Conflict<'T>) : string =
        let alts =
            c.Productions
            |> List.map (fun p ->
                let rhs = if p.Rhs.IsEmpty then "ε" else p.Rhs |> List.map (showSymbol show) |> String.concat " "
                $"  %s{p.Name}: %s{c.Nonterminal} := %s{rhs}")
            |> String.concat "\n"
        let why =
            match c.Kind with
            | FirstFirst -> "both alternatives can start with it"
            | FirstFollow -> "one alternative can be empty and it can also follow the rule"
        $"In %s{c.Nonterminal}, with %s{showLookahead show c.Lookahead} next, %s{why}:\n%s{alts}"
