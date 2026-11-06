/// The fragment of TLA+ expression syntax the translator emits, and its printer. Every compound expression is
/// printed in parentheses, so operator precedence never matters, and no junction lists are used, so layout never
/// changes meaning: line breaks are for people reading the generated module.
module FsTla.Tla

open System

type Expr =
    /// A name or a literal, printed as is.
    | Atom of string
    /// Op(a, b); Op alone when there are no arguments.
    | Apply of string * Expr list
    /// (a op b)
    | Infix of string * Expr * Expr
    /// (op a), where op carries any space it needs ("DOMAIN ").
    | Prefix of string * Expr
    /// e.f
    | Field of Expr * string
    /// e[k]
    | Index of Expr * Expr
    /// [f |-> e, …]
    | Record of (string * Expr) list
    /// &lt;&lt;a, b&gt;&gt;
    | Tuple of Expr list
    /// {a, b}
    | SetOf of Expr list
    /// [e EXCEPT !.f = v, …]
    | Except of Expr * (string * Expr) list
    | If of Expr * Expr * Expr
    /// LET a == e … IN body
    | LetIn of (string * Expr) list * Expr
    /// (\E x \in S : P) or (\A x \in S : P)
    | Quantifier of string * string * Expr * Expr
    /// {e : x \in S}
    | SetMap of Expr * string * Expr
    /// {x \in S : P}
    | SetFilter of string * Expr * Expr
    /// [x \in S |-> e]
    | FunctionOf of string * Expr * Expr

let private join (sep: string) (items: string seq) = String.Join(sep, items)

/// Prints an expression; continuation lines start with indent spaces.
let rec print (indent: int) (e: Expr) : string =
    let pad n = String(' ', n)
    let p = print indent
    match e with
    | Atom s -> s
    | Apply(f, []) -> f
    | Apply(f, args) -> f + "(" + join ", " (List.map p args) + ")"
    | Infix(op, a, b) -> "(" + p a + " " + op + " " + p b + ")"
    | Prefix(op, a) -> "(" + op + p a + ")"
    | Field(x, f) -> p x + "." + f
    | Index(x, k) -> p x + "[" + p k + "]"
    | Record fields -> "[" + join ", " (fields |> List.map (fun (n, v) -> n + " |-> " + p v)) + "]"
    | Tuple items -> "<<" + join ", " (List.map p items) + ">>"
    | SetOf items -> "{" + join ", " (List.map p items) + "}"
    | Except(x, updates) -> "[" + p x + " EXCEPT " + join ", " (updates |> List.map (fun (n, v) -> "!." + n + " = " + p v)) + "]"
    | If(c, a, b) ->
        let inner = indent + 2
        "(IF " + print (indent + 4) c
        + "\n" + pad inner + "THEN " + print (inner + 5) a
        + "\n" + pad inner + "ELSE " + print (inner + 5) b + ")"
    | LetIn(defs, body) ->
        let inner = indent + 1
        let def (n: string, v) = n + " == " + print (inner + 4 + n.Length + 4) v
        "(LET " + join ("\n" + pad (inner + 4)) (List.map def defs)
        + "\n" + pad inner + "IN  " + print (inner + 4) body + ")"
    | Quantifier(q, x, s, body) -> "(" + q + " " + x + " \\in " + p s + " : " + print (indent + 2) body + ")"
    | SetMap(body, x, s) -> "{" + print (indent + 1) body + " : " + x + " \\in " + p s + "}"
    | SetFilter(x, s, body) -> "{" + x + " \\in " + p s + " : " + print (indent + 1) body + "}"
    | FunctionOf(x, s, body) -> "[" + x + " \\in " + p s + " |-> " + print (indent + 1) body + "]"
