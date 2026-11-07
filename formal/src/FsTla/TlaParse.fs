/// Reads values as TLC prints them (in error traces and state dumps) into <see cref="Values.TlaValue"/>. Only the
/// forms the encoding in <see cref="Values"/> produces are accepted: integers, strings, booleans, model values,
/// records, sets, sequences and functions written with :> and @@.
module FsTla.TlaParse

open System
open System.Text
open FsTla.Values

exception ParseException of string

type private Token =
    | Int of int64
    | Str of string
    | Ident of string
    | Sym of string

let private symbols = [ "|->"; "<<"; ">>"; ":>"; "@@"; "["; "]"; "{"; "}"; "("; ")"; "," ]

let private tokenize (text: string) =
    let tokens = ResizeArray()
    let mutable i = 0
    let at k = if k < text.Length then text.[k] else '\000'
    while i < text.Length do
        let c = text.[i]
        if Char.IsWhiteSpace c then i <- i + 1
        elif c = '"' then
            let sb = StringBuilder()
            i <- i + 1
            while i < text.Length && text.[i] <> '"' do
                if text.[i] = '\\' && i + 1 < text.Length then
                    let e = text.[i + 1]
                    sb.Append(match e with 'n' -> '\n' | 't' -> '\t' | other -> other) |> ignore
                    i <- i + 2
                else
                    sb.Append(text.[i]) |> ignore
                    i <- i + 1
            if i >= text.Length then raise (ParseException "unterminated string")
            i <- i + 1
            tokens.Add(Str(sb.ToString()))
        elif Char.IsDigit c || (c = '-' && Char.IsDigit(at (i + 1))) then
            let start = i
            i <- i + 1
            while i < text.Length && Char.IsDigit text.[i] do i <- i + 1
            tokens.Add(Int(Int64.Parse(text.Substring(start, i - start))))
        elif Char.IsLetter c || c = '_' then
            let start = i
            while i < text.Length && (Char.IsLetterOrDigit text.[i] || text.[i] = '_') do i <- i + 1
            tokens.Add(Ident(text.Substring(start, i - start)))
        else
            match symbols |> List.tryFind (fun s -> String.CompareOrdinal(text, i, s, 0, s.Length) = 0) with
            | Some s ->
                tokens.Add(Sym s)
                i <- i + s.Length
            | None -> raise (ParseException(sprintf "unexpected '%c' at %d in %s" c i text))
    tokens.ToArray()

/// Parses one TLC-printed value.
let parse (text: string) : TlaValue =
    let tokens = tokenize text
    let mutable pos = 0
    let peek () = if pos < tokens.Length then Some tokens.[pos] else None
    let next () =
        let t = tokens.[pos]
        pos <- pos + 1
        t
    let expect s =
        match peek () with
        | Some(Sym x) when x = s -> pos <- pos + 1
        | other -> raise (ParseException(sprintf "expected '%s', found %A in %s" s other text))
    let rec list closing (item: unit -> 'a) =
        match peek () with
        | Some(Sym x) when x = closing ->
            pos <- pos + 1
            []
        | _ ->
            let first = item ()
            let rest = ResizeArray [ first ]
            while (match peek () with Some(Sym ",") -> true | _ -> false) do
                pos <- pos + 1
                rest.Add(item ())
            expect closing
            List.ofSeq rest
    let rec value () : TlaValue =
        match next () with
        | Int n -> TInt n
        | Str s -> TString s
        | Ident "TRUE" -> TBool true
        | Ident "FALSE" -> TBool false
        | Ident name -> TModelValue name
        | Sym "<<" -> TSeq(list ">>" value)
        | Sym "{" -> TSet(list "}" value)
        | Sym "[" ->
            TRecord(
                list "]" (fun () ->
                    match next () with
                    | Ident field ->
                        expect "|->"
                        field, value ()
                    | other -> raise (ParseException(sprintf "expected a field name, found %A in %s" other text)))
            )
        | Sym "(" ->
            let first = value ()
            match peek () with
            | Some(Sym ":>") ->
                pos <- pos + 1
                let pairs = ResizeArray [ first, value () ]
                while (match peek () with Some(Sym "@@") -> true | _ -> false) do
                    pos <- pos + 1
                    let k = value ()
                    expect ":>"
                    pairs.Add(k, value ())
                expect ")"
                TFun(List.ofSeq pairs)
            | _ ->
                expect ")"
                first
        | other -> raise (ParseException(sprintf "unexpected %A in %s" other text))
    let result = value ()
    if pos <> tokens.Length then raise (ParseException(sprintf "trailing input after a value in %s" text))
    result
