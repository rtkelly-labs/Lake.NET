module Formal.Traces.Tests.GrammarTests

open System
open Formal.Traces
open Xunit

let show (c: char) = string c

/// The textbook expression grammar (Aho et al.), left recursion removed: LL(1).
let expressions =
    Grammar.create "expr" "E" [
        Grammar.rule "E" "an expression" [ "sum", [ N "T"; N "E'" ] ]
        Grammar.rule "E'" "the rest of a sum" [ "plus", [ T '+'; N "T"; N "E'" ]; "done", [] ]
        Grammar.rule "T" "a term" [ "product", [ N "F"; N "T'" ] ]
        Grammar.rule "T'" "the rest of a product" [ "times", [ T '*'; N "F"; N "T'" ]; "done", [] ]
        Grammar.rule "F" "a factor" [ "group", [ T '('; N "E"; T ')' ]; "id", [ T 'x' ] ]
    ]

/// E := E + E | x: ambiguous ("x+x+x" groups two ways).
let ambiguousSum =
    Grammar.create "sum" "E" [ Grammar.rule "E" "an expression" [ "plus", [ N "E"; T '+'; N "E" ]; "id", [ T 'x' ] ] ]

[<Fact>]
let ``FIRST and FOLLOW match the textbook`` () =
    let first = Grammar.first expressions
    Assert.Equal<Set<char>>(set [ '('; 'x' ], first["E"])
    Assert.Equal<Set<char>>(set [ '+' ], first["E'"])
    Assert.Equal<Set<char>>(set [ '*' ], first["T'"])
    let follow = Grammar.follow expressions
    Assert.Equal<Set<Lookahead<char>>>(set [ Lookahead.Event ')'; Lookahead.End ], follow["E'"])
    Assert.Equal<Set<Lookahead<char>>>(set [ Lookahead.Event '+'; Lookahead.Event ')'; Lookahead.End ], follow["T'"])
    Assert.Equal<Set<Lookahead<char>>>(set [ Lookahead.Event '*'; Lookahead.Event '+'; Lookahead.Event ')'; Lookahead.End ], follow["F"])
    Assert.Equal<Set<string>>(set [ "E'"; "T'" ], Grammar.nullable expressions)

[<Fact>]
let ``An LL(1) grammar gets a table and no bounded ambiguity`` () =
    Assert.True(Grammar.ll1 expressions |> Result.isOk)
    let report = Ambiguity.search 7 expressions
    Assert.Empty(report.Ambiguous)
    Assert.True(report.Sentences > 10)

[<Fact>]
let ``An ambiguous grammar is rejected by the table and the search finds a witness`` () =
    match Grammar.ll1 ambiguousSum with
    | Ok _ -> failwith "E := E + E | x must not be LL(1)"
    | Error conflicts ->
        let text = conflicts |> List.map (Grammar.explainConflict show) |> String.concat "\n"
        Assert.Contains("In E, with x next, both alternatives can start with it", text)
        Assert.Contains("plus: E := E + E", text)
    let witness = (Ambiguity.search 5 ambiguousSum).Ambiguous |> List.map _.Sentence
    Assert.Contains([ 'x'; '+'; 'x'; '+'; 'x' ], witness)

[<Fact>]
let ``A FIRST/FOLLOW conflict is reported as one`` () =
    // S := A a ; A := a | ε  -- with "a" next, A cannot tell whether to consume it.
    let g =
        Grammar.create "ff" "S" [
            Grammar.rule "S" "" [ "s", [ N "A"; T 'a' ] ]
            Grammar.rule "A" "" [ "a", [ T 'a' ]; "empty", [] ]
        ]
    match Grammar.ll1 g with
    | Error [ c ] ->
        Assert.Equal(FirstFollow, c.Kind)
        Assert.Equal("A", c.Nonterminal)
    | other -> failwithf "expected one FIRST/FOLLOW conflict, got %A" other

[<Fact>]
let ``Malformed grammars are refused at construction`` () =
    let undefined () = Grammar.create "g" "S" [ Grammar.rule "S" "" [ "s", [ N "Missing" ] ] ] |> ignore
    let unreachable () =
        Grammar.create "g" "S" [ Grammar.rule "S" "" [ "s", [ T 'a' ] ]; Grammar.rule "X" "" [ "x", [ T 'b' ] ] ] |> ignore
    let unproductive () = Grammar.create "g" "S" [ Grammar.rule "S" "" [ "loop", [ T 'a'; N "S" ] ] ] |> ignore
    Assert.Contains("Missing", (Assert.Throws<ArgumentException> undefined).Message)
    Assert.Contains("Unreachable", (Assert.Throws<ArgumentException> unreachable).Message)
    Assert.Contains("no finite trace", (Assert.Throws<ArgumentException> unproductive).Message)

[<Fact>]
let ``Right-linear grammars are recognised`` () =
    Assert.False(Grammar.isRightLinear expressions)
    let regular = Grammar.create "r" "S" [ Grammar.rule "S" "" [ "more", [ T 'a'; T 'b'; N "S" ]; "stop", [] ] ]
    Assert.True(Grammar.isRightLinear regular)
