/// The table builder, the parser and the bounded search are three independent readings of one grammar. On random
/// small grammars they must agree: translation validation of the LL(1) construction by enumeration.
module Formal.Traces.Tests.DifferentialTests

open System
open FsCheck
open FsCheck.FSharp
open Formal.Traces
open Xunit

let private names = [ "S"; "A"; "B" ]
let private alphabet = [ 'a'; 'b'; 'c' ]

let private symbol =
    Gen.elements (List.map T alphabet @ List.map N names)

let private alternative = Gen.choose (0, 3) |> Gen.bind (fun n -> Gen.listOfLength n symbol)

let private rules =
    names
    |> List.map (fun name ->
        Gen.choose (1, 3)
        |> Gen.bind (fun n -> Gen.listOfLength n alternative)
        |> Gen.map (fun alts -> name, alts))
    |> Gen.sequenceToList

/// A random grammar, pruned to the rules reachable from S, or None when what is left is unproductive.
let private tryGrammar (raw: (string * Symbol<char> list list) list) =
    let byName = Map.ofList raw
    let rec reach (seen: Set<string>) todo =
        match todo with
        | [] -> seen
        | n :: rest when seen.Contains n -> reach seen rest
        | n :: rest ->
            let next = byName[n] |> List.collect (List.choose (function N m -> Some m | T _ -> None))
            reach (seen.Add n) (next @ rest)
    let reachable = reach Set.empty [ "S" ]
    try
        raw
        |> List.filter (fst >> reachable.Contains)
        // Duplicate alternatives are a trivial ambiguity; drop them so the interesting cases dominate.
        |> List.map (fun (name, alts) ->
            Grammar.rule name "" (alts |> List.distinct |> List.mapi (fun i rhs -> $"%s{name}%d{i}", rhs)))
        |> Grammar.create "random" "S"
        |> Some
    with :? ArgumentException -> None

let rec private strings n =
    if n = 0 then [ [] ]
    else [] :: [ for c in alphabet do for rest in strings (n - 1) -> c :: rest ] |> List.distinct

let private bound = 4
let private allStrings = strings bound

let private agree (outcomes: Collections.Concurrent.ConcurrentBag<string>) (raw: (string * Symbol<char> list list) list) =
    match tryGrammar raw with
    | None -> true
    | Some g ->
        let ambiguous = (Ambiguity.search bound g).Ambiguous |> List.filter (fun a -> a.Nonterminal = g.Start)
        match Grammar.ll1 g with
        | Error _ ->
            outcomes.Add(if ambiguous.IsEmpty then "conflict" else "ambiguous")
            true // a conflict does not prove ambiguity, but ambiguity must produce a conflict: checked below
        | Ok table ->
            outcomes.Add "ll1"
            let language = Ambiguity.sentences bound g |> Set.ofList
            ambiguous.IsEmpty
            && allStrings |> List.forall (fun w -> (Parser.parse table w |> Result.isOk) = language.Contains w)

/// LL(1) implies unambiguous, so a table must never coexist with an ambiguity witness; and the parser must accept
/// exactly the sentences enumeration derives. The run must also have seen all three kinds of grammar.
[<Fact>]
let ``LL(1) tables, the parser and enumeration agree on random grammars`` () =
    let outcomes = Collections.Concurrent.ConcurrentBag<string>()
    Check.One(Config.QuickThrowOnFailure.WithMaxTest(1000), Prop.forAll (Arb.fromGen rules) (agree outcomes))
    let seen = outcomes |> Set.ofSeq
    Assert.True(Set.isSubset (set [ "ll1"; "conflict"; "ambiguous" ]) seen, $"%A{seen}")
