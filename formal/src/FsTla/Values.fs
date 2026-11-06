/// How F# values are represented in TLA+, both ways: F# value → TLA+ literal (for constants, the initial state and
/// the action set) and TLC-printed value → F# value (for counterexamples and state dumps).
///
/// The encoding, which the translator follows too:
///   bool, int, string      TRUE/FALSE, integers, strings
///   union case (and option) a record [tag |-> "Case", Field1 |-> …] (option's field is Value)
///   record                  a record [Field |-> …]
///   tuple                   a sequence <<a, b>>
///   Set&lt;'T&gt;               a set {…}
///   Map&lt;'K, 'V&gt;            a function (k :> v @@ …); empty is <<>>
///   unit                    <<>>
module FsTla.Values

open System
open System.Collections
open System.Text
open Microsoft.FSharp.Reflection

/// A value as TLC prints it. Sets, sequences and functions keep TLC's order; compare decoded F# values, not these.
type TlaValue =
    | TInt of int64
    | TBool of bool
    | TString of string
    | TRecord of (string * TlaValue) list
    | TSet of TlaValue list
    | TSeq of TlaValue list
    | TFun of (TlaValue * TlaValue) list
    | TModelValue of string

exception EncodingException of string

let private fail fmt = Printf.kprintf (fun s -> raise (EncodingException s)) fmt

let isSet (t: Type) = t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Set<int>>
let isMap (t: Type) = t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<Map<int, int>>
let isOption (t: Type) = t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<option<int>>

/// The name the encoding gives every union case's tag field. A union field may not use it.
[<Literal>]
let TagField = "tag"

let quote (s: string) =
    let sb = StringBuilder("\"")
    for c in s do
        match c with
        | '"' -> sb.Append("\\\"") |> ignore
        | '\\' -> sb.Append("\\\\") |> ignore
        | '\n' -> sb.Append("\\n") |> ignore
        | c -> sb.Append(c) |> ignore
    sb.Append('"').ToString()

let unionFields (case: UnionCaseInfo) =
    let fields = case.GetFields()
    if fields |> Array.exists (fun f -> f.Name = TagField) then
        fail "Union case %s.%s has a field named '%s', which the TLA+ encoding reserves" case.DeclaringType.Name case.Name TagField
    fields

/// The TLA+ literal for an F# value of type t.
let rec literal (t: Type) (value: obj) : string =
    if t = typeof<bool> then (if unbox<bool> value then "TRUE" else "FALSE")
    elif t = typeof<int> then string (unbox<int> value)
    elif t = typeof<int64> then string (unbox<int64> value)
    elif t = typeof<string> then quote (unbox<string> value)
    elif t = typeof<unit> then "<<>>"
    elif isSet t then
        let elem = t.GetGenericArguments().[0]
        let items = [ for v in (value :?> IEnumerable) -> literal elem v ]
        "{" + String.Join(", ", items) + "}"
    elif isMap t then
        let args = t.GetGenericArguments()
        let pairs =
            [ for kv in (value :?> IEnumerable) ->
                  let kt = kv.GetType()
                  let k = kt.GetProperty("Key").GetValue(kv)
                  let v = kt.GetProperty("Value").GetValue(kv)
                  sprintf "%s :> %s" (literal args.[0] k) (literal args.[1] v) ]
        if pairs.IsEmpty then "<<>>" else "(" + String.Join(" @@ ", pairs) + ")"
    elif FSharpType.IsTuple t then
        let items = Array.map2 literal (FSharpType.GetTupleElements t) (FSharpValue.GetTupleFields value)
        "<<" + String.Join(", ", items) + ">>"
    elif FSharpType.IsRecord(t, true) then
        let fields = FSharpType.GetRecordFields(t, true)
        let values = FSharpValue.GetRecordFields(value, true)
        let items = Array.map2 (fun (f: Reflection.PropertyInfo) v -> sprintf "%s |-> %s" f.Name (literal f.PropertyType v)) fields values
        "[" + String.Join(", ", items) + "]"
    elif FSharpType.IsUnion(t, true) then
        let case, values = FSharpValue.GetUnionFields(value, t, true)
        let fields = unionFields case
        let items =
            Array.append
                [| sprintf "%s |-> %s" TagField (quote case.Name) |]
                (Array.map2 (fun (f: Reflection.PropertyInfo) v -> sprintf "%s |-> %s" f.Name (literal f.PropertyType v)) fields values)
        "[" + String.Join(", ", items) + "]"
    else fail "Type %s has no TLA+ encoding (supported: bool, int, string, unit, unions, records, tuples, Set, Map)" t.FullName

let literalOf<'T> (value: 'T) = literal typeof<'T> (box value)

let private makeSet (elem: Type) (items: obj list) =
    let ofSeq = typeof<Set<int>>.Assembly.GetType("Microsoft.FSharp.Collections.SetModule").GetMethod("OfSeq").MakeGenericMethod(elem)
    let arr = Array.CreateInstance(elem, items.Length)
    items |> List.iteri (fun i v -> arr.SetValue(v, i))
    ofSeq.Invoke(null, [| arr |])

let private makeMap (k: Type) (v: Type) (pairs: (obj * obj) list) =
    let tupleType = FSharpType.MakeTupleType [| k; v |]
    let arr = Array.CreateInstance(tupleType, pairs.Length)
    pairs |> List.iteri (fun i (a, b) -> arr.SetValue(FSharpValue.MakeTuple([| a; b |], tupleType), i))
    let ofSeq = typeof<Map<int, int>>.Assembly.GetType("Microsoft.FSharp.Collections.MapModule").GetMethod("OfSeq").MakeGenericMethod(k, v)
    ofSeq.Invoke(null, [| arr |])

/// The F# value of type t that a TLC-printed value stands for.
let rec decode (t: Type) (value: TlaValue) : obj =
    let mismatch () = fail "Cannot read %A as %s" value t.Name
    if t = typeof<bool> then (match value with TBool b -> box b | _ -> mismatch ())
    elif t = typeof<int> then (match value with TInt i -> box (int i) | _ -> mismatch ())
    elif t = typeof<int64> then (match value with TInt i -> box i | _ -> mismatch ())
    elif t = typeof<string> then (match value with TString s -> box s | _ -> mismatch ())
    elif t = typeof<unit> then box ()
    elif isSet t then
        let elem = t.GetGenericArguments().[0]
        match value with
        | TSet items -> makeSet elem (items |> List.map (decode elem))
        | TSeq [] -> makeSet elem []
        | _ -> mismatch ()
    elif isMap t then
        let args = t.GetGenericArguments()
        let k, v = args.[0], args.[1]
        // TLC prints a function by the shape of its domain: 1..n as a sequence, strings as a record.
        let pairs =
            match value with
            | TFun pairs -> pairs
            | TSeq items -> items |> List.mapi (fun i x -> TInt(int64 (i + 1)), x)
            | TRecord fields -> fields |> List.map (fun (n, x) -> TString n, x)
            | TSet [] -> []
            | _ -> mismatch ()
        makeMap k v (pairs |> List.map (fun (a, b) -> decode k a, decode v b))
    elif FSharpType.IsTuple t then
        match value with
        | TSeq items ->
            let types = FSharpType.GetTupleElements t
            if items.Length <> types.Length then mismatch ()
            FSharpValue.MakeTuple(Array.map2 decode types (Array.ofList items), t)
        | _ -> mismatch ()
    elif FSharpType.IsRecord(t, true) then
        match value with
        | TRecord fields ->
            let byName = dict fields
            let values =
                FSharpType.GetRecordFields(t, true)
                |> Array.map (fun f ->
                    match byName.TryGetValue f.Name with
                    | true, x -> decode f.PropertyType x
                    | _ -> fail "Record %s is missing field %s in %A" t.Name f.Name value)
            FSharpValue.MakeRecord(t, values, true)
        | _ -> mismatch ()
    elif FSharpType.IsUnion(t, true) then
        match value with
        | TRecord fields ->
            let byName = dict fields
            let tag =
                match byName.TryGetValue TagField with
                | true, TString s -> s
                | _ -> mismatch ()
            let case =
                FSharpType.GetUnionCases(t, true)
                |> Array.tryFind (fun c -> c.Name = tag)
                |> Option.defaultWith (fun () -> fail "%s has no case %s" t.Name tag)
            let values = case.GetFields() |> Array.map (fun f -> decode f.PropertyType byName.[f.Name])
            FSharpValue.MakeUnion(case, values, true)
        | _ -> mismatch ()
    else fail "Type %s has no TLA+ encoding" t.FullName

let decodeAs<'T> (value: TlaValue) = decode typeof<'T> value :?> 'T

/// Every value of a finite type: unions whose fields are finite, bool, tuples and records of finite types, and int
/// from <paramref name="ints"/>. For action sets.
let rec enumerate (ints: int list) (t: Type) : obj list =
    let product (types: Type array) =
        types
        |> Array.fold (fun acc ft -> [ for prefix in acc do for v in enumerate ints ft -> prefix @ [ v ] ]) [ [] ]
        |> List.map Array.ofList
    if t = typeof<bool> then [ box false; box true ]
    elif t = typeof<int> then ints |> List.map box
    elif t = typeof<unit> then [ box () ]
    elif FSharpType.IsTuple t then product (FSharpType.GetTupleElements t) |> List.map (fun vs -> FSharpValue.MakeTuple(vs, t))
    elif FSharpType.IsRecord(t, true) then
        product (FSharpType.GetRecordFields(t, true) |> Array.map (fun f -> f.PropertyType))
        |> List.map (fun vs -> FSharpValue.MakeRecord(t, vs, true))
    elif FSharpType.IsUnion(t, true) then
        [ for case in FSharpType.GetUnionCases(t, true) do
              for vs in product (case.GetFields() |> Array.map (fun f -> f.PropertyType)) -> FSharpValue.MakeUnion(case, vs, true) ]
    else fail "Type %s is not finite: cannot enumerate it" t.FullName

/// Every value of 'T (see <see cref="enumerate"/>).
let all<'T> (ints: int list) = enumerate ints typeof<'T> |> List.map unbox<'T>
