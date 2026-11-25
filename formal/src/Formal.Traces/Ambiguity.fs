namespace Formal.Traces

/// A sentence with two or more derivations from one nonterminal.
type Ambiguous<'T> = { Nonterminal: string; Sentence: 'T list }

/// The result of a bounded ambiguity search: every sentence of at most <c>MaxLength</c> events was enumerated with
/// the number of its derivations from each nonterminal.
type AmbiguityReport<'T> =
    { MaxLength: int
      /// Distinct sentences derivable from the start rule within the bound.
      Sentences: int
      Ambiguous: Ambiguous<'T> list }

/// Bounded ambiguity search, in the spirit of CFGAnalyzer but by enumeration instead of SAT: count derivations of every
/// sentence up to a length bound. It is independent of the LL(1) construction, so the two cross-check each other: a
/// conflict-free table must come with an empty report, and an ambiguity found here must show up as a table conflict.
[<RequireQualifiedAccess>]
module Ambiguity =

    // Derivation counts saturate at 2: one means unambiguous, two means "at least two".
    let private cap n = min n 2

    let private concat (maxLength: int) (left: Map<'T list, int>) (right: Map<'T list, int>) =
        seq {
            for KeyValue(a, ca) in left do
                for KeyValue(b, cb) in right do
                    if a.Length + b.Length <= maxLength then yield a @ b, ca * cb
        }
        |> Seq.fold (fun (m: Map<'T list, int>) (w, c) -> m.Add(w, cap (c + defaultArg (m.TryFind w) 0))) Map.empty

    /// Enumerate every sentence of at most <paramref name="maxLength"/> events and count its derivations. Kleene
    /// iteration: round k counts derivations of depth at most k; it stops when nothing changes.
    let search (maxLength: int) (g: Grammar<'T>) : AmbiguityReport<'T> =
        let empty = g.Rules |> List.map (fun r -> r.Name, Map.empty<'T list, int>) |> Map.ofList
        let step (current: Map<string, Map<'T list, int>>) =
            g.Rules
            |> List.map (fun r ->
                let language =
                    r.Alternatives
                    |> List.map (fun p ->
                        p.Rhs
                        |> List.fold
                            (fun acc symbol ->
                                let part =
                                    match symbol with
                                    | T t -> Map.ofList [ [ t ], 1 ]
                                    | N n -> current[n]
                                concat maxLength acc part)
                            (Map.ofList [ [], 1 ]))
                    |> List.fold
                        (fun (m: Map<'T list, int>) lang ->
                            lang |> Map.fold (fun m w c -> m.Add(w, cap (c + defaultArg (Map.tryFind w m) 0))) m)
                        Map.empty
                r.Name, language)
            |> Map.ofList
        let rec fix current =
            let next = step current
            if next = current then current else fix next
        let languages = fix empty
        let ambiguous =
            g.Rules
            |> List.collect (fun r ->
                languages[r.Name]
                |> Map.toList
                |> List.filter (fun (_, c) -> c > 1)
                |> List.map (fun (w, _) -> { Nonterminal = r.Name; Sentence = w }))
            |> List.sortBy (fun a -> a.Sentence.Length, a.Nonterminal)
        { MaxLength = maxLength; Sentences = languages[g.Start].Count; Ambiguous = ambiguous }

    /// Every sentence of the start rule up to the bound, shortest first (for differential tests of the parser).
    let sentences (maxLength: int) (g: Grammar<'T>) : 'T list list =
        let empty = g.Rules |> List.map (fun r -> r.Name, Set.empty<'T list>) |> Map.ofList
        let step (current: Map<string, Set<'T list>>) =
            g.Rules
            |> List.map (fun r ->
                r.Name,
                r.Alternatives
                |> List.map (fun p ->
                    p.Rhs
                    |> List.fold
                        (fun (acc: Set<'T list>) symbol ->
                            let part =
                                match symbol with
                                | T t -> Set.singleton [ t ]
                                | N n -> current[n]
                            seq {
                                for a in acc do
                                    for b in part do
                                        if a.Length + b.Length <= maxLength then yield a @ b
                            }
                            |> Set.ofSeq)
                        (Set.singleton []))
                |> Set.unionMany)
            |> Map.ofList
        let rec fix current =
            let next = step current
            if next = current then current else fix next
        (fix empty)[g.Start] |> Set.toList |> List.sortBy List.length
