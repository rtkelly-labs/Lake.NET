/// A protocol model: an F# step function, an initial state and a finite set of actions, plus invariants. The same
/// definition runs natively (for random walks against real code) and compiles to a TLA+ module (for TLC).
///
/// Shape the functions must have, all marked [<ReflectedDefinition>]:
///   step      : 'Config -> 'Action -> 'State -> 'State option   (None: the action is not enabled)
///   invariant : 'Config -> 'State -> bool
/// The step function is deterministic; all nondeterminism comes from which action is taken.
module FsTla.Model

open System
open System.Collections.Generic
open System.Reflection
open System.Text
open Microsoft.FSharp.Quotations
open FsTla.Values

type Invariant<'Config, 'State> =
    { Name: string
      Method: MethodInfo
      Holds: 'Config -> 'State -> bool }

type Model<'Config, 'Action, 'State when 'Action: equality and 'State: equality> =
    { Name: string
      StepMethod: MethodInfo
      Step: 'Config -> 'Action -> 'State -> 'State option
      Init: 'Config -> 'State
      Actions: 'Config -> 'Action list
      Invariants: Invariant<'Config, 'State> list }

/// A step that no action explains: the TLA+ translation and the F# model disagree, so the translator has a bug.
exception TranslationMismatch of string

let private invoke (mi: MethodInfo) (args: obj array) =
    try
        mi.Invoke(null, args)
    with :? TargetInvocationException as e when not (isNull e.InnerException) ->
        Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e.InnerException).Throw()
        failwith "unreachable"

/// Defines a model. name becomes the TLA+ module name.
let define<'Config, 'Action, 'State when 'Action: equality and 'State: equality>
    (name: string)
    (step: Expr<'Config -> 'Action -> 'State -> 'State option>)
    (init: 'Config -> 'State)
    (actions: 'Config -> 'Action list)
    : Model<'Config, 'Action, 'State> =
    let mi = Translate.methodOf step
    { Name = name
      StepMethod = mi
      Step = fun c a s -> invoke mi [| box c; box a; box s |] :?> 'State option
      Init = init
      Actions = actions
      Invariants = [] }

/// Adds an invariant; name is how TLC reports it (as Inv_name).
let invariant (name: string) (check: Expr<'Config -> 'State -> bool>) (model: Model<'Config, 'Action, 'State>) =
    let mi = Translate.methodOf check
    let inv =
        { Name = name
          Method = mi
          Holds = fun c s -> invoke mi [| box c; box s |] :?> bool }
    { model with Invariants = model.Invariants @ [ inv ] }

/// The actions enabled in a state, with the state each leads to.
let enabled (model: Model<'C, 'A, 'S>) (cfg: 'C) (state: 'S) : ('A * 'S) list =
    model.Actions cfg |> List.choose (fun a -> model.Step cfg a state |> Option.map (fun s -> a, s))

/// The names of the invariants a state breaks.
let violations (model: Model<'C, 'A, 'S>) (cfg: 'C) (state: 'S) =
    model.Invariants |> List.filter (fun i -> not (i.Holds cfg state)) |> List.map (fun i -> i.Name)

/// Runs a list of actions from the initial state; fails if one is not enabled. Returns every state, initial first.
let replay (model: Model<'C, 'A, 'S>) (cfg: 'C) (actions: 'A list) : 'S list =
    let folder (states: 'S list) (a: 'A) =
        match model.Step cfg a states.Head with
        | Some next -> next :: states
        | None -> failwithf "%A is not enabled after %d steps" a (states.Length - 1)
    actions |> List.fold folder [ model.Init cfg ] |> List.rev

/// A random walk driven by a list of choices: at each step, choice % (number of enabled actions) picks the action.
/// Stops when the choices run out or nothing is enabled. Shrinking the choices shrinks the walk.
let walk (model: Model<'C, 'A, 'S>) (cfg: 'C) (choices: int list) : ('A * 'S) list =
    let rec go state choices acc =
        match choices, enabled model cfg state with
        | _, []
        | [], _ -> List.rev acc
        | c :: rest, options ->
            let a, next = options.[(abs (c % options.Length))]
            go next rest ((a, next) :: acc)
    go (model.Init cfg) choices []

/// Recovers the action behind each step of a behaviour (initial state first). Raises TranslationMismatch when the F#
/// model cannot take a step that TLC took.
let explain (model: Model<'C, 'A, 'S>) (cfg: 'C) (states: 'S list) : ('A * 'S) list =
    match states with
    | [] -> []
    | first :: _ ->
        if first <> model.Init cfg then
            raise (TranslationMismatch(sprintf "TLC's initial state differs from the F# one:\n%A" first))
        states
        |> List.pairwise
        |> List.mapi (fun i (s, next) ->
            match model.Actions cfg |> List.tryFind (fun a -> model.Step cfg a s = Some next) with
            | Some a -> a, next
            | None ->
                raise (
                    TranslationMismatch(
                        sprintf "No F# action leads from step %d to step %d of TLC's trace.\nFrom:\n%A\nTo:\n%A" i (i + 1) s next
                    )
                ))

/// The generated TLA+ module and its TLC configuration.
type Generated = { Module: string; Config: string }

/// Compiles the model, for one configuration, to a TLA+ module.
let toTla (model: Model<'C, 'A, 'S>) (cfg: 'C) : Generated =
    let roots = model.StepMethod :: (model.Invariants |> List.map (fun i -> i.Method))
    let operators, nameOf = Translate.translate roots
    let sb = StringBuilder()
    let line (s: string) = sb.Append(s).Append('\n') |> ignore
    let actions = model.Actions cfg |> List.map (literalOf<'A>)
    line (sprintf "---- MODULE %s ----" model.Name)
    line (sprintf "\\* Generated by FsTla from %s. Do not edit: change the F# model and regenerate."
              (model.StepMethod.DeclaringType.FullName.Replace('+', '.')))
    line "EXTENDS Integers, FiniteSets, TLC"
    line ""
    line "VARIABLE state"
    line ""
    line ("Cfg == " + literalOf<'C> cfg)
    line ""
    line ("Actions == {\n    " + String.Join(",\n    ", actions) + "\n}")
    for op in operators do
        line ""
        line (Translate.printOperator op)
    line ""
    line ("Init == state = " + literalOf<'S> (model.Init cfg))
    line ""
    line (sprintf "Next == \\E a \\in Actions : LET r == %s(Cfg, a, state) IN (r.tag = \"Some\") /\\ (state' = r.Value)"
              (nameOf model.StepMethod))
    line ""
    line "Spec == Init /\\ [][Next]_state"
    for inv in model.Invariants do
        line ""
        line (sprintf "Inv_%s == %s(Cfg, state)" inv.Name (nameOf inv.Method))
    line ""
    line "===="
    let config = StringBuilder()
    config.Append("INIT Init\nNEXT Next\n") |> ignore
    for inv in model.Invariants do
        config.Append(sprintf "INVARIANT Inv_%s\n" inv.Name) |> ignore
    { Module = sb.ToString(); Config = config.ToString() }

/// What TLC concluded, in F# terms.
type Verdict<'A, 'S> =
    /// Every invariant holds in every reachable state.
    | Holds of Tlc.Stats option
    /// An invariant fails: the actions that reach the failing state, each with the state it leads to.
    | Violated of invariant: string * initial: 'S * steps: ('A * 'S) list

type Checked<'A, 'S> =
    { Verdict: Verdict<'A, 'S>
      Run: Tlc.Run
      /// Every distinct reachable state, when a dump was asked for.
      States: 'S list option }

/// Model-checks one configuration with TLC.
let check (model: Model<'C, 'A, 'S>) (cfg: 'C) (jar: string) (dump: bool) (timeout: TimeSpan) : Checked<'A, 'S> =
    let generated = toTla model cfg
    let run = Tlc.run jar model.Name generated.Module generated.Config dump timeout
    let verdict =
        match run.Outcome with
        | Tlc.NoError -> Holds run.Stats
        | Tlc.InvariantViolated(name, trace) ->
            let states = trace |> List.map decodeAs<'S>
            let invariant = if name.StartsWith "Inv_" then name.Substring 4 else name
            Violated(invariant, List.head states, explain model cfg states)
        | Tlc.Failure message -> failwithf "TLC failed (exit %d):\n%s\n\nFull output:\n%s" run.ExitCode message run.Output
    { Verdict = verdict
      Run = run
      States = run.Dump |> Option.map (List.map decodeAs<'S>) }

/// Differential check of the F# model against the set of states TLC explored: the initial state is in it, every F#
/// successor of every state in it is in it, every state in it but the initial one has an F# predecessor in it, and
/// (when TLC found no error) the F# invariants hold on all of it. Each problem found is a translator bug.
let differential (model: Model<'C, 'A, 'S>) (cfg: 'C) (states: 'S list) (invariantsHold: bool) : string list =
    let known = HashSet<'S>(states, HashIdentity.Structural)
    let problems = ResizeArray<string>()
    let report (s: string) = if problems.Count < 10 then problems.Add s
    let init = model.Init cfg
    if not (known.Contains init) then report (sprintf "TLC's states lack the F# initial state %A" init)
    let reached = HashSet<'S>(HashIdentity.Structural)
    reached.Add init |> ignore
    for s in states do
        for a, next in enabled model cfg s do
            reached.Add next |> ignore
            if not (known.Contains next) then report (sprintf "F# takes %A from\n%A\nto a state TLC never found:\n%A" a s next)
        if invariantsHold then
            for name in violations model cfg s do
                report (sprintf "F# invariant %s fails on a state TLC accepted:\n%A" name s)
    for s in states do
        if not (reached.Contains s) then report (sprintf "TLC found a state no F# step reaches:\n%A" s)
    List.ofSeq problems
