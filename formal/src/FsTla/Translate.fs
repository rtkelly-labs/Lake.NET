/// Translates F# functions marked [<ReflectedDefinition>] into TLA+ operators.
///
/// The translatable subset is fenced: anything outside it raises a TranslationException that names the construct and
/// the F# function it appeared in. Inside the fence:
///   let, if/then/else, match on unions and options (union-case tests and field reads), && and ||
///   records ({ r with … } becomes EXCEPT), tuples, union cases, option
///   int arithmetic and comparison, =, <>, not, fst, snd, max, min
///   Set: contains add remove union difference intersect isSubset count isEmpty exists forall filter map
///        singleton empty unionMany ofList; s.Contains s.Add s.Remove s.Count s.IsEmpty
///   Map: find add remove containsKey tryFind forall exists filter map count isEmpty empty ofList;
///        m.[k] m.Add m.Remove m.ContainsKey m.TryFind m.Count m.IsEmpty
///   Option: isSome isNone defaultValue get; o.IsSome o.IsNone o.Value
///   lambdas, only as the argument of the Set/Map functions above
///   calls to other reflected functions, which become operators (recursion is rejected)
///   module-level values and zero-argument calls outside the model, evaluated natively and emitted as literals
module FsTla.Translate

open System
open System.Collections.Generic
open System.Reflection
// Tla first: its Expr type is always written Tla.Expr, and the quotation Expr and patterns must win.
open FsTla.Tla
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open Microsoft.FSharp.Reflection
open FsTla.Values

exception TranslationException of string

let private fail fmt = Printf.kprintf (fun s -> raise (TranslationException s)) fmt

/// A translated F# function.
type Operator =
    { Name: string
      Parameters: string list
      Body: Tla.Expr
      Method: MethodInfo }

/// Names the generated module defines itself, TLA+ keywords, and operators of the standard modules it extends.
let reserved =
    HashSet<string>(
        [ "state"; "Cfg"; "Actions"; "Init"; "Next"; "Spec"
          "ASSUME"; "ASSUMPTION"; "AXIOM"; "BOOLEAN"; "CASE"; "CHOOSE"; "CONSTANT"; "CONSTANTS"; "DOMAIN"; "ELSE"
          "ENABLED"; "EXCEPT"; "EXTENDS"; "FALSE"; "IF"; "IN"; "INSTANCE"; "LAMBDA"; "LET"; "LOCAL"; "MODULE"
          "OTHER"; "RECURSIVE"; "STRING"; "SUBSET"; "THEN"; "THEOREM"; "TRUE"; "UNCHANGED"; "UNION"; "VARIABLE"
          "VARIABLES"; "WITH"; "Nat"; "Int"; "Seq"; "Len"; "Append"; "Head"; "Tail"; "SubSeq"; "SelectSeq"
          "Cardinality"; "IsFiniteSet"; "Print"; "PrintT"; "Assert"; "JavaTime"; "TLCGet"; "TLCSet"; "Permutations"
          "SortSeq"; "RandomElement"; "Any"; "ToString"; "TLCEval" ]
    )

let private genericDefinition (mi: MethodInfo) =
    if mi.IsGenericMethod then mi.GetGenericMethodDefinition() else mi

/// The quotation of a reflected function, if it has one.
let reflectedBody (mi: MethodInfo) =
    match Expr.TryGetReflectedDefinition mi with
    | Some body -> Some body
    | None -> Expr.TryGetReflectedDefinition(genericDefinition mi)

let private describe (mi: MethodInfo) = mi.DeclaringType.FullName.Replace('+', '.') + "." + mi.Name

/// The method a quotation such as <@ step @> refers to.
let methodOf (e: Expr) : MethodInfo =
    let rec go e =
        match e with
        | Lambda(_, body) -> go body
        | Let(_, _, body) -> go body
        | Call(None, mi, _) -> mi
        | _ -> fail "Expected a quotation of a module function, such as <@ step @>; found %A" e
    go e

let private isInt (t: Type) = t = typeof<int> || t = typeof<int64>
let private tag (case: string) = "tag", Atom(quote case)
let private isUnit (t: Type) = t = typeof<unit> || t = typeof<Void>

let private unionType (t: Type) =
    if FSharpType.IsUnion(t, true) then Some t
    elif not (isNull t.BaseType) && FSharpType.IsUnion(t.BaseType, true) then Some t.BaseType
    else None

let private isList (t: Type) = t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<list<int>>

/// Splits the args of an F# list literal [a; b; c].
let rec private listItems (e: Expr) =
    match e with
    | NewUnionCase(case, [ head; tail ]) when isList case.DeclaringType -> head :: listItems tail
    | NewUnionCase(case, []) when isList case.DeclaringType -> []
    | Coerce(inner, _) -> listItems inner
    | _ -> fail "Only list literals [a; b] are supported (as the argument of Set.ofList / Map.ofList); found %A" e

/// A literal for a value computed natively.
let private nativeLiteral (t: Type) (value: obj) =
    let text = literal t value
    Atom(if text.StartsWith "-" then "(" + text + ")" else text)

/// Simple expressions are inlined where a let binds them, instead of becoming a LET.
let rec private isSimple (e: Tla.Expr) =
    match e with
    | Atom _ -> true
    | Field(x, _) -> isSimple x
    | _ -> false

type private Context =
    { Done: Dictionary<MethodInfo, Operator>
      Order: ResizeArray<Operator>
      InProgress: HashSet<MethodInfo>
      Names: Dictionary<string, MethodInfo> }

let private operatorName (ctx: Context) (mi: MethodInfo) =
    let baseName =
        let n = mi.Name.Replace('\'', '_')
        string (Char.ToUpperInvariant n.[0]) + n.Substring 1
    let rec pick (candidate: string) =
        if reserved.Contains candidate then pick (candidate + "_")
        else
            match ctx.Names.TryGetValue candidate with
            | true, other when other <> mi -> pick (mi.DeclaringType.Name + "_" + candidate)
            | _ -> candidate
    let name = pick baseName
    ctx.Names.[name] <- mi
    name

/// Translates one reflected function, and everything it calls, into the context.
let rec private ensure (ctx: Context) (mi: MethodInfo) : string =
    let def = genericDefinition mi
    match ctx.Done.TryGetValue def with
    | true, op -> op.Name
    | _ ->
        if ctx.InProgress.Contains def then
            fail "%s is recursive: TLA+ operators generated from F# may not call themselves" (describe def)
        let body =
            match reflectedBody mi with
            | Some b -> b
            | None -> fail "%s has no [<ReflectedDefinition>]" (describe def)
        ctx.InProgress.Add def |> ignore
        let name = operatorName ctx def
        let op =
            try
                translateBody ctx name def body
            with
            | TranslationException msg when not (msg.StartsWith "In ") -> fail "In %s: %s" (describe def) msg
        ctx.InProgress.Remove def |> ignore
        ctx.Done.[def] <- op
        ctx.Order.Add op
        name

and private translateBody (ctx: Context) (name: string) (mi: MethodInfo) (body: Expr) : Operator =
    let used = HashSet<string>()
    let fresh (hint: string) =
        let cleaned = String(hint |> Seq.map (fun c -> if Char.IsLetterOrDigit c || c = '_' then c else '_') |> Array.ofSeq)
        let start =
            if cleaned.Length > 0 && Char.IsLower cleaned.[0] && Char.IsLetter cleaned.[0] then cleaned
            else "v_" + cleaned
        let mutable candidate = start
        let mutable n = 2
        while used.Contains candidate || reserved.Contains candidate do
            candidate <- sprintf "%s_%d" start n
            n <- n + 1
        used.Add candidate |> ignore
        candidate

    // Parameters: curried (one lambda per parameter) or tupled (one lambda of a tuple of several parameters).
    let parameters = mi.GetParameters()
    let rec peel (i: int) (env: Map<Var, Tla.Expr>) (names: string list) (e: Expr) =
        if i >= parameters.Length then
            match e with
            | Lambda(v, rest) when parameters.Length = 0 && isUnit v.Type -> env.Add(v, Atom "<<>>"), names, rest
            | _ -> env, names, e
        else
            match e with
            | Lambda(v, rest) when v.Type = parameters.[i].ParameterType ->
                if isUnit v.Type then peel (i + 1) (env.Add(v, Atom "<<>>")) names rest
                else
                    let n = fresh v.Name
                    peel (i + 1) (env.Add(v, Atom n)) (names @ [ n ]) rest
            | Lambda(v, rest) when FSharpType.IsTuple v.Type ->
                let elems = FSharpType.GetTupleElements v.Type
                if i + elems.Length > parameters.Length then
                    fail "cannot match the parameters of %s to its definition" (describe mi)
                let ns = [ for k in 0 .. elems.Length - 1 -> fresh parameters.[i + k].Name ]
                peel (i + elems.Length) (env.Add(v, Tuple(ns |> List.map Atom))) (names @ ns) rest
            | _ -> fail "cannot match the parameters of %s to its definition %A" (describe mi) e

    let env0, parameterNames, body = peel 0 Map.empty [] body

    let rec lambdas (arity: int) (e: Expr) =
        match e with
        | _ when arity = 0 -> [], e
        | Lambda(v, rest) ->
            let vs, b = lambdas (arity - 1) rest
            v :: vs, b
        | Coerce(inner, _) -> lambdas arity inner
        | _ -> fail "expected a lambda (fun x -> …) here, found %A" e

    let rec expr (env: Map<Var, Tla.Expr>) (e: Expr) : Tla.Expr =
        let x = expr env
        let bind (v: Var) = fresh v.Name
        /// fun x -> body over the elements of s, as (binder, body).
        let over1 (f: Expr) =
            match lambdas 1 f with
            | [ v ], b ->
                let n = bind v
                n, expr (env.Add(v, Atom n)) b
            | _ -> fail "expected fun x -> …"
        /// fun k v -> body over the entries of m, as (binder, body); v reads m[k].
        let over2 (f: Expr) (m: Tla.Expr) =
            match lambdas 2 f with
            | [ k; v ], b ->
                let n = bind k
                n, expr (env.Add(k, Atom n).Add(v, Index(m, Atom n))) b
            | _ -> fail "expected fun k v -> …"
        let domain m = Prefix("DOMAIN ", m)
        let some v = Record [ tag "Some"; "Value", v ]
        let none = Record [ tag "None" ]
        let isSome o = Infix("=", Field(o, "tag"), Atom(quote "Some"))
        let removeKey k m =
            let n = fresh "key"
            FunctionOf(n, Infix("\\", domain m, SetOf [ k ]), Index(m, Atom n))
        let tryFind k m = If(Infix("\\in", k, domain m), some (Index(m, k)), none)
        let mapOfList (items: Expr) =
            listItems items
            |> List.fold
                (fun acc item ->
                    match item with
                    | NewTuple [ k; v ] -> Infix("@@", Infix(":>", x k, x v), acc)
                    | _ -> fail "Map.ofList needs a literal list of (key, value) tuples; found %A" item)
                (Atom "<<>>")
        let integers (args: Expr list) =
            if not (args |> List.forall (fun a -> isInt a.Type)) then
                fail "arithmetic and ordering are only translated for int, not %s" (args.Head.Type.Name)
        let isNone o = Infix("=", Field(o, "tag"), Atom(quote "None"))
        // Each shape of expression has its own function, with plain if/elif inside: one large match of quotation
        // patterns and guards makes the F# compiler's pattern compilation blow up.
        let propertyGet (target: Expr option) (p: PropertyInfo) (args: Expr list) =
            let owner = p.DeclaringType
            match target, args with
            | Some o, [] when isOption owner && p.Name = "IsSome" -> isSome (x o)
            | Some o, [] when isOption owner && p.Name = "IsNone" -> isNone (x o)
            | Some m, [] when isMap owner && p.Name = "Count" -> Apply("Cardinality", [ domain (x m) ])
            | Some m, [] when isMap owner && p.Name = "IsEmpty" -> Infix("=", domain (x m), SetOf [])
            | Some s, [] when isSet owner && p.Name = "Count" -> Apply("Cardinality", [ x s ])
            | Some s, [] when isSet owner && p.Name = "IsEmpty" -> Infix("=", x s, SetOf [])
            | Some m, [ k ] when isMap owner && p.Name = "Item" -> Index(x m, x k)
            | Some r, [] when FSharpType.IsRecord(owner, true) || (unionType owner).IsSome -> Field(x r, p.Name)
            | None, [] -> nativeLiteral p.PropertyType (p.GetValue null)
            | _ -> fail "property %s.%s is outside the translatable subset" owner.Name p.Name
        let instanceCall (target: Expr) (mi: MethodInfo) (args: Expr list) =
            let owner = mi.DeclaringType
            if isSet owner then
                match mi.Name, args with
                | "Contains", [ a ] -> Infix("\\in", x a, x target)
                | "Add", [ a ] -> Infix("\\cup", x target, SetOf [ x a ])
                | "Remove", [ a ] -> Infix("\\", x target, SetOf [ x a ])
                | _ -> fail "Set method %s is outside the translatable subset" mi.Name
            elif isMap owner then
                match mi.Name, args with
                | "ContainsKey", [ k ] -> Infix("\\in", x k, domain (x target))
                | "get_Item", [ k ] -> Index(x target, x k)
                | "Add", [ k; v ] -> Infix("@@", Infix(":>", x k, x v), x target)
                | "Remove", [ k ] -> removeKey (x k) (x target)
                | "TryFind", [ k ] -> tryFind (x k) (x target)
                | _ -> fail "Map method %s is outside the translatable subset" mi.Name
            else fail "method %s.%s is outside the translatable subset" owner.Name mi.Name
        let operators (mi: MethodInfo) (args: Expr list) =
            let binary symbol =
                match args with
                | [ a; b ] -> Infix(symbol, x a, x b)
                | _ -> fail "%s expects two arguments" mi.Name
            let arithmetic symbol =
                integers args
                binary symbol
            let element i =
                match args with
                | [ a ] ->
                    match x a with
                    | Tuple items -> items.[i]
                    | t -> Index(t, Atom(string (i + 1)))
                | _ -> fail "%s expects one argument" mi.Name
            match mi.Name with
            | "op_Equality" -> binary "="
            | "op_Inequality" -> binary "#"
            | "op_LessThan" -> arithmetic "<"
            | "op_GreaterThan" -> arithmetic ">"
            | "op_LessThanOrEqual" -> arithmetic "<="
            | "op_GreaterThanOrEqual" -> arithmetic ">="
            | "op_Addition" -> arithmetic "+"
            | "op_Subtraction" -> arithmetic "-"
            | "op_Multiply" -> arithmetic "*"
            | "op_Division" -> arithmetic "\\div"
            | "op_Modulus" -> arithmetic "%"
            | "op_UnaryNegation" ->
                integers args
                Prefix("-", x args.Head)
            | "Not" -> Prefix("~", x args.Head)
            | "Fst" -> element 0
            | "Snd" -> element 1
            | "Max"
            | "Min" ->
                integers args
                let a, b = x args.[0], x args.[1]
                If(Infix((if mi.Name = "Max" then ">=" else "<="), a, b), a, b)
            | _ -> fail "operator %s is outside the translatable subset" mi.Name
        let setModule (mi: MethodInfo) (args: Expr list) =
            match mi.Name, args with
            | "Contains", [ a; s ] -> Infix("\\in", x a, x s)
            | "Add", [ a; s ] -> Infix("\\cup", x s, SetOf [ x a ])
            | "Remove", [ a; s ] -> Infix("\\", x s, SetOf [ x a ])
            | "Union", [ a; b ] -> Infix("\\cup", x a, x b)
            | "Difference", [ a; b ] -> Infix("\\", x a, x b)
            | "Intersect", [ a; b ] -> Infix("\\cap", x a, x b)
            | "IsSubset", [ a; b ] -> Infix("\\subseteq", x a, x b)
            | "IsSuperset", [ a; b ] -> Infix("\\subseteq", x b, x a)
            | "Count", [ s ] -> Apply("Cardinality", [ x s ])
            | "IsEmpty", [ s ] -> Infix("=", x s, SetOf [])
            | "Singleton", [ a ] -> SetOf [ x a ]
            | "Empty", [] -> SetOf []
            | "UnionMany", [ s ] -> Prefix("UNION ", x s)
            | "OfList", [ items ] -> SetOf(listItems items |> List.map x)
            | "Exists", [ f; s ] ->
                let n, b = over1 f
                Quantifier("\\E", n, x s, b)
            | "ForAll", [ f; s ] ->
                let n, b = over1 f
                Quantifier("\\A", n, x s, b)
            | "Filter", [ f; s ] ->
                let n, b = over1 f
                SetFilter(n, x s, b)
            | "Map", [ f; s ] ->
                let n, b = over1 f
                SetMap(b, n, x s)
            | _ -> fail "Set.%s is outside the translatable subset" mi.Name
        let mapModule (mi: MethodInfo) (args: Expr list) =
            match mi.Name, args with
            | "Find", [ k; m ] -> Index(x m, x k)
            | "Add", [ k; v; m ] -> Infix("@@", Infix(":>", x k, x v), x m)
            | "Remove", [ k; m ] -> removeKey (x k) (x m)
            | "ContainsKey", [ k; m ] -> Infix("\\in", x k, domain (x m))
            | "TryFind", [ k; m ] -> tryFind (x k) (x m)
            | "Count", [ m ] -> Apply("Cardinality", [ domain (x m) ])
            | "IsEmpty", [ m ] -> Infix("=", domain (x m), SetOf [])
            | "Empty", [] -> Atom "<<>>"
            | "OfList", [ items ] -> mapOfList items
            | ("ForAll" | "Exists" | "Filter" | "Map"), [ f; m ] ->
                let m' = x m
                let n, b = over2 f m'
                match mi.Name with
                | "ForAll" -> Quantifier("\\A", n, domain m', b)
                | "Exists" -> Quantifier("\\E", n, domain m', b)
                | "Filter" ->
                    let k = fresh "key"
                    FunctionOf(k, SetFilter(n, domain m', b), Index(m', Atom k))
                | _ -> FunctionOf(n, domain m', b)
            | _ -> fail "Map.%s is outside the translatable subset" mi.Name
        let optionModule (mi: MethodInfo) (args: Expr list) =
            match mi.Name, args with
            | "IsSome", [ o ] -> isSome (x o)
            | "IsNone", [ o ] -> isNone (x o)
            | "GetValue", [ o ] -> Field(x o, "Value")
            | "DefaultValue", [ d; o ] ->
                let o' = x o
                If(isSome o', Field(o', "Value"), x d)
            | _ -> fail "Option.%s is outside the translatable subset" mi.Name
        let staticCall (mi: MethodInfo) (args: Expr list) =
            let owner = mi.DeclaringType
            if (reflectedBody mi).IsSome then
                // Another model function: an operator call.
                Apply(ensure ctx mi, args |> List.filter (fun a -> not (isUnit a.Type)) |> List.map x)
            elif isOption owner && mi.Name = "get_IsSome" then isSome (x args.Head)
            elif isOption owner && mi.Name = "get_IsNone" then isNone (x args.Head)
            elif mi.Name = "CreateSet" && args.Length = 1 then SetOf(listItems args.Head |> List.map x)
            else
                match owner.FullName with
                | "Microsoft.FSharp.Core.Operators" -> operators mi args
                | "Microsoft.FSharp.Collections.SetModule" -> setModule mi args
                | "Microsoft.FSharp.Collections.MapModule" -> mapModule mi args
                | "Microsoft.FSharp.Core.OptionModule" -> optionModule mi args
                | _ when args.IsEmpty && not (isUnit mi.ReturnType) ->
                    // A value computed outside the model.
                    nativeLiteral mi.ReturnType (mi.Invoke(null, [||]))
                | _ ->
                    fail "%s is outside the translatable subset (mark it [<ReflectedDefinition>] if it is part of the model)" (describe mi)
        match e with
        | Value(v, t) when FSharpType.IsFunction t -> fail "function values cannot be translated: %A" v
        | Value(v, t) -> nativeLiteral t v
        | Var v ->
            match env.TryFind v with
            | Some tla -> tla
            | None -> fail "unbound variable %s" v.Name
        | Coerce(inner, _) -> x inner
        | Let(v, _, _) when FSharpType.IsFunction v.Type ->
            fail "local function %s: lift it to a module-level function with [<ReflectedDefinition>]" v.Name
        | Let _ ->
            // Consecutive lets become one LET block; simple values are inlined.
            let rec gather env defs e =
                match e with
                | Let(v, value, rest) when not (FSharpType.IsFunction v.Type) ->
                    let tla = expr env value
                    if isSimple tla then gather (Map.add v tla env) defs rest
                    else
                        let n = bind v
                        gather (Map.add v (Atom n) env) (defs @ [ n, tla ]) rest
                | _ -> defs, expr env e
            match gather env [] e with
            | [], result -> result
            | defs, result -> LetIn(defs, result)
        | IfThenElse(c, a, Value(:? bool as b, _)) when not b -> Infix("/\\", x c, x a)
        | IfThenElse(c, Value(:? bool as b, _), a) when b -> Infix("\\/", x c, x a)
        | IfThenElse(c, a, b) -> If(x c, x a, x b)
        | NewUnionCase(case, _) when isList case.DeclaringType ->
            fail "lists are not translated (use Set or Map); a list literal may only be the argument of Set.ofList or Map.ofList"
        | NewUnionCase(case, args) ->
            let fields = unionFields case
            Record(tag case.Name :: (List.map2 (fun (f: PropertyInfo) a -> f.Name, x a) (List.ofArray fields) args))
        | UnionCaseTest(_, case) when isList case.DeclaringType -> fail "lists are not translated (use Set or Map)"
        | UnionCaseTest(target, case) -> Infix("=", Field(x target, "tag"), Atom(quote case.Name))
        | NewTuple items -> Tuple(List.map x items)
        | TupleGet(target, i) ->
            match x target with
            | Tuple items -> items.[i]
            | t -> Index(t, Atom(string (i + 1)))
        | NewRecord(t, args) ->
            let fields = FSharpType.GetRecordFields(t, true)
            // { s with A = … } arrives as a full record whose other fields read s.
            let copiedFrom =
                List.zip (List.ofArray fields) args
                |> List.choose (fun (f, a) ->
                    match a with
                    | PropertyGet(Some(Var src), p, []) when p.Name = f.Name && p.DeclaringType = t -> Some src
                    | _ -> None)
            match copiedFrom with
            | src :: rest when rest |> List.forall ((=) src) ->
                let updates =
                    List.zip (List.ofArray fields) args
                    |> List.choose (fun (f, a) ->
                        match a with
                        | PropertyGet(Some(Var s), p, []) when s = src && p.Name = f.Name -> None
                        | _ -> Some(f.Name, x a))
                if updates.IsEmpty then x (Expr.Var src) else Except(x (Expr.Var src), updates)
            | _ -> Record(List.map2 (fun (f: PropertyInfo) a -> f.Name, x a) (List.ofArray fields) args)

        | PropertyGet(target, p, args) -> propertyGet target p args
        | Call(Some target, mi, args) -> instanceCall target mi args
        | Call(None, mi, args) -> staticCall mi args
        | Lambda _ -> fail "lambdas are only translated as the argument of a Set or Map function"
        | Application _ -> fail "applying a function value is not translated; call a module function directly"
        | Sequential _ -> fail "sequencing (a; b) is not translated: models are pure expressions"
        | _ -> fail "%A is outside the translatable subset" e

    { Name = name
      Parameters = parameterNames
      Body = expr env0 body
      Method = mi }

/// Translates the given reflected functions and everything they call. Operators come out in dependency order, each
/// after the ones it calls, as TLA+ requires.
let translate (roots: MethodInfo list) : Operator list * (MethodInfo -> string) =
    let ctx =
        { Done = Dictionary()
          Order = ResizeArray()
          InProgress = HashSet()
          Names = Dictionary() }
    for mi in roots do
        ensure ctx mi |> ignore
    List.ofSeq ctx.Order, (fun mi -> ctx.Done.[genericDefinition mi].Name)

/// One operator definition as TLA+ text.
let printOperator (op: Operator) =
    let head = if op.Parameters.IsEmpty then op.Name else op.Name + "(" + String.Join(", ", op.Parameters) + ")"
    head + " ==\n    " + Tla.print 4 op.Body
