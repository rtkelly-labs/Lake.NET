namespace Formal.Traces

/// One recorded event with the key of the slice it belongs to (a commit ID, a writer, a lock...).
type Keyed<'K, 'T> = { Key: 'K; Event: 'T }

/// The violations found in one recorded run.
type TraceReport<'K, 'T> =
    { Slices: int
      Events: int
      Violations: ('K * ParseError<'T>) list }

    member r.Passed = r.Violations.IsEmpty

/// Parametric trace slicing (Chen and Rosu, TACAS 2009): split an interleaved trace by a parameter, keep each
/// slice's order, and run the same non-parametric check on every slice. Interleavings of independent writers are thus
/// reduced to one sequential protocol per key, which a regular or LL(1) grammar can check.
[<RequireQualifiedAccess>]
module Slicing =

    /// Group events by key, preserving first-appearance order of keys and recording order within a slice.
    let slice (events: Keyed<'K, 'T> seq) : ('K * 'T list) list =
        let order = System.Collections.Generic.List<'K>()
        let slices = System.Collections.Generic.Dictionary<'K, System.Collections.Generic.List<'T>>(HashIdentity.Structural)
        for e in events do
            match slices.TryGetValue e.Key with
            | true, list -> list.Add e.Event
            | false, _ ->
                order.Add e.Key
                slices[e.Key] <- System.Collections.Generic.List<'T>([ e.Event ])
        [ for k in order -> k, List.ofSeq slices[k] ]

    /// Slice <paramref name="events"/> and parse every slice.
    let check (table: Ll1Table<'T>) (events: Keyed<'K, 'T> seq) : TraceReport<'K, 'T> =
        let slices = slice events
        { Slices = slices.Length
          Events = slices |> List.sumBy (snd >> List.length)
          Violations =
            slices
            |> List.choose (fun (k, trace) ->
                match Parser.parse table trace with
                | Ok _ -> None
                | Error e -> Some(k, e)) }
