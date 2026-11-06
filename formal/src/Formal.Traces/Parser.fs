namespace Formal.Traces

/// Why a trace is not in the grammar's language. Exactly one explanation exists because the table is LL(1): the
/// parser never chose between alternatives, so the state it was in when the trace went wrong is the only one.
type ParseError<'T> =
    { Grammar: string
      /// 0-based index of the offending event; equal to the trace length when the trace ended too early.
      Position: int
      Got: Lookahead<'T>
      Expected: Lookahead<'T> list
      /// The nonterminal (protocol state) being expanded, or the one whose alternative required the missing event.
      State: string
      StateDoc: string
      /// The events accepted before the error.
      Accepted: 'T list }

/// A successful parse: the alternatives applied, in leftmost-derivation order.
type Derivation<'T> = Production<'T> list

[<RequireQualifiedAccess>]
module Parser =

    type private Frame<'T> =
        | Expand of string
        | Match of 'T * owner: string

    /// Parse <paramref name="trace"/> with a conflict-free table. Iterative, so long traces cannot overflow the stack.
    let parse (table: Ll1Table<'T>) (trace: 'T list) : Result<Derivation<'T>, ParseError<'T>> =
        let input = List.toArray trace
        let at i = if i < input.Length then Lookahead.Event input[i] else Lookahead.End
        let fail i state expected =
            Error
                { Grammar = table.Grammar.Name
                  Position = i
                  Got = at i
                  Expected = expected
                  State = state
                  StateDoc = table.Grammar.Doc state
                  Accepted = List.ofArray input[.. i - 1] }
        let rec run stack i (derivation: Production<'T> list) =
            match stack with
            | [] ->
                if i = input.Length then Ok(List.rev derivation)
                else fail i table.Grammar.Start [ Lookahead.End ]
            | Match(t, owner) :: rest ->
                if i < input.Length && input[i] = t then run rest (i + 1) derivation
                else fail i owner [ Lookahead.Event t ]
            | Expand n :: rest ->
                match table.Cells.TryFind((n, at i)) with
                | Some p ->
                    let pushed = p.Rhs |> List.map (function T t -> Match(t, n) | N m -> Expand m)
                    run (pushed @ rest) i (p :: derivation)
                | None -> fail i n (table.Expected n)
        run [ Expand table.Grammar.Start ] 0 []

    let private showLookahead (show: 'T -> string) la =
        match la with
        | Lookahead.Event t -> show t
        | Lookahead.End -> "end of trace"

    /// The error as a few readable lines: where, what came, what the protocol allowed, and in which state.
    let explain (show: 'T -> string) (e: ParseError<'T>) : string =
        let shown = e.Accepted |> List.map show
        let here =
            match e.Got with
            | Lookahead.Event t -> [ $"%s{show t}  <-- here" ]
            | Lookahead.End -> [ "<end of trace>  <-- here" ]
        let expected = e.Expected |> List.map (showLookahead show) |> String.concat " | "
        let trace = shown @ here |> String.concat ", "
        let doc = if System.String.IsNullOrEmpty e.StateDoc then "" else " - " + e.StateDoc
        [ $"%s{e.Grammar}: protocol violation at event %d{e.Position + 1}"
          $"  trace:    %s{trace}"
          $"  state:    %s{e.State}%s{doc}"
          $"  expected: %s{expected}"
          $"  got:      %s{showLookahead show e.Got}" ]
        |> String.concat "\n"
