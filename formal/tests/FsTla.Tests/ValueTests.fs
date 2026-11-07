/// The value encoding: F# values to TLA+ literals and back, through the parser for TLC's output.
module FsTla.Tests.ValueTests

open FsCheck
open FsTla
open FsTla.Values
open FsTla.Tests.Toy
open Xunit

type Sample =
    { Name: string
      Flag: bool
      Count: int
      Pick: Counter option
      Bumps: Set<Bump>
      ByCounter: Map<Counter, Set<int>> }

let private roundTrip<'T when 'T: equality> (value: 'T) =
    let text = literalOf value
    Assert.Equal(value, decodeAs<'T> (TlaParse.parse text))

[<Fact>]
let ``records, unions, options, sets and maps round-trip`` () =
    roundTrip
        { Name = "a \"quoted\" \\ name"
          Flag = true
          Count = -3
          Pick = Some B
          Bumps = set [ { Counter = A; By = 1 }; { Counter = B; By = 2 } ]
          ByCounter = Map.ofList [ A, set [ 1; 2 ]; B, Set.empty ] }

[<Fact>]
let ``arbitrary values round-trip`` () =
    let property (value: Sample) = literalOf value |> TlaParse.parse |> decodeAs<Sample> = value
    Check.QuickThrowOnFailure property

[<Fact>]
let ``unions encode as tagged records and the empty map as the empty sequence`` () =
    Assert.Equal("[tag |-> \"Some\", Value |-> [tag |-> \"A\"]]", literalOf (Some A))
    Assert.Equal("<<>>", literalOf (Map.empty<Counter, int>))

[<Fact>]
let ``TLC's printed values parse`` () =
    let printed = "[Counts |-> ([tag |-> \"A\"] :> 2 @@ [tag |-> \"B\"] :> 0), Last |-> [tag |-> \"None\"], Seen |-> {[tag |-> \"A\"]}]"
    let state = decodeAs<State> (TlaParse.parse printed)
    Assert.True((Map.ofList [ A, 2; B, 0 ] = state.Counts))
    Assert.Equal(None, state.Last)
    Assert.Equal<Set<Counter>>(set [ A ], state.Seen)

[<Fact>]
let ``enumeration covers every case and every int in range`` () =
    let all = Values.all<Bump> [ 1; 2 ]
    Assert.Equal(4, all.Length)
    Assert.Equal<Set<Bump>>(set [ for c in [ A; B ] do for n in [ 1; 2 ] -> { Counter = c; By = n } ], set all)
