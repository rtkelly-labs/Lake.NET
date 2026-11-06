module Formal.Traces.Tests.ParserTests

open Formal.Traces
open Xunit

let table =
    match Grammar.ll1 GrammarTests.expressions with
    | Ok t -> t
    | Error e -> failwithf "%A" e

let chars (s: string) = List.ofSeq s

[<Fact>]
let ``Valid sentences parse to a leftmost derivation`` () =
    match Parser.parse table (chars "x+x*(x)") with
    | Ok derivation ->
        Assert.Equal("sum", derivation.Head.Name)
        Assert.Equal(16, derivation.Length)
    | Error e -> failwith (Parser.explain GrammarTests.show e)

[<Fact>]
let ``An unexpected event names its position, the state and what was allowed`` () =
    match Parser.parse table (chars "x+*x") with
    | Ok _ -> failwith "x+*x must not parse"
    | Error e ->
        Assert.Equal(2, e.Position)
        Assert.Equal(Lookahead.Event '*', e.Got)
        Assert.Equal("T", e.State)
        Assert.Equal<Lookahead<char> list>([ Lookahead.Event '('; Lookahead.Event 'x' ], e.Expected)
        Assert.Equal<char list>(chars "x+", e.Accepted)
        let text = Parser.explain GrammarTests.show e
        Assert.Contains("protocol violation at event 3", text)
        Assert.Contains("x, +, *  <-- here", text)
        Assert.Contains("state:    T - a term", text)
        Assert.Contains("expected: ( | x", text)

[<Fact>]
let ``A trace that stops early is reported at its end`` () =
    match Parser.parse table (chars "(x") with
    | Ok _ -> failwith "(x must not parse"
    | Error e ->
        Assert.Equal(2, e.Position)
        Assert.Equal(Lookahead.End, e.Got)
        Assert.Contains(Lookahead.Event ')', e.Expected)

[<Fact>]
let ``Slices are checked independently and keep their order`` () =
    let events =
        [ 1, 'x'; 2, '('; 1, '+'; 2, 'x'; 1, 'x'; 3, '+' ]
        |> List.map (fun (k, c) -> { Key = k; Event = c })
    let slices = Slicing.slice events
    Assert.Equal<(int * char list) list>([ 1, chars "x+x"; 2, chars "(x"; 3, chars "+" ], slices)
    let report = Slicing.check table events
    Assert.Equal(3, report.Slices)
    Assert.Equal(6, report.Events)
    Assert.Equal<int list>([ 2; 3 ], report.Violations |> List.map fst)

[<Fact>]
let ``Long traces do not overflow the stack`` () =
    let g = Grammar.create "loop" "S" [ Grammar.rule "S" "" [ "more", [ T 'a'; N "S" ]; "stop", [] ] ]
    let t = match Grammar.ll1 g with Ok t -> t | Error e -> failwithf "%A" e
    Assert.True(Parser.parse t (List.replicate 200_000 'a') |> Result.isOk)
