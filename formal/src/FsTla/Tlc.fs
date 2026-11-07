/// Runs TLC (tla2tools.jar) on a generated module and reads its plain-text output. TLC is the trusted checker: this
/// module only starts it and parses what it prints.
module FsTla.Tlc

open System
open System.Diagnostics
open System.IO
open System.Text.RegularExpressions
open FsTla.Values

type Stats = { Generated: int64; Distinct: int64 }

type Outcome =
    /// "Model checking completed. No error has been found."
    | NoError
    /// An invariant failed; the behaviour that reaches the failing state, initial state first.
    | InvariantViolated of invariant: string * trace: TlaValue list
    /// Anything else (parse or semantic errors, evaluation errors): see the output.
    | Failure of message: string

type Run =
    { Outcome: Outcome
      Stats: Stats option
      /// Every distinct state TLC found, when the run asked for a dump.
      Dump: TlaValue list option
      Output: string
      ExitCode: int
      Elapsed: TimeSpan }

/// The environment variable that points at tla2tools.jar.
[<Literal>]
let JarVariable = "TLA2TOOLS_JAR"

/// tla2tools.jar, if TLA2TOOLS_JAR names a file that exists.
let jar () =
    match Environment.GetEnvironmentVariable JarVariable with
    | null
    | "" -> None
    | path when File.Exists path -> Some path
    | _ -> None

/// Splits TLC's state listings ("State N: …" followed by "state = <value>", possibly over several lines, ending at a
/// blank line) into values. Used for both error traces and -dump files.
let parseStates (lines: string seq) : TlaValue list =
    let states = ResizeArray()
    let current = Text.StringBuilder()
    let mutable inState = false
    let flush () =
        if inState && current.Length > 0 then
            let text = current.ToString().Trim()
            let text = if text.StartsWith "/\\ " then text.Substring 3 else text
            let text = if text.StartsWith "state = " then text.Substring 8 else text
            states.Add(TlaParse.parse text)
        current.Clear() |> ignore
        inState <- false
    for line in lines do
        if Regex.IsMatch(line, @"^State \d+:") then
            flush ()
            inState <- true
        elif inState && String.IsNullOrWhiteSpace line then flush ()
        elif inState then current.AppendLine line |> ignore
        elif line.StartsWith "state = " || line.StartsWith "/\\ state = " then
            // The initial state when it already violates an invariant has no "State 1:" header.
            inState <- true
            current.AppendLine line |> ignore
    flush ()
    List.ofSeq states

let private statsPattern = Regex(@"([\d,]+) states generated, ([\d,]+) distinct states found")
let private violatedPattern = Regex(@"^Error: Invariant (\S+) is violated")

/// Reads TLC's console output.
let parseOutput (output: string) : Outcome * Stats option =
    let lines = output.Replace("\r", "").Split('\n')
    let stats =
        lines
        |> Array.tryPick (fun l ->
            let m = statsPattern.Match l
            if m.Success then
                let n (g: Group) = Int64.Parse(g.Value.Replace(",", ""))
                Some { Generated = n m.Groups.[1]; Distinct = n m.Groups.[2] }
            else None)
    let violated = lines |> Array.tryFindIndex violatedPattern.IsMatch
    let outcome =
        match violated with
        | Some i ->
            let name = violatedPattern.Match(lines.[i]).Groups.[1].Value.TrimEnd('.')
            let rest = lines |> Array.skip (i + 1) |> Array.takeWhile (fun l -> not (statsPattern.IsMatch l))
            InvariantViolated(name, parseStates rest)
        | None when lines |> Array.exists (fun l -> l.Contains "Model checking completed. No error has been found.") -> NoError
        | None ->
            let errors = lines |> Array.filter (fun l -> l.StartsWith "Error" || l.Contains "***") |> Array.truncate 20
            Failure(if errors.Length = 0 then "TLC did not report a result" else String.Join("\n", errors))
    outcome, stats

/// Runs TLC on moduleName.tla / moduleName.cfg in a fresh directory. With dump, also returns every distinct state.
let run (jarPath: string) (moduleName: string) (tla: string) (cfg: string) (dump: bool) (timeout: TimeSpan) : Run =
    let dir = Directory.CreateTempSubdirectory("fstla-")
    try
        File.WriteAllText(Path.Combine(dir.FullName, moduleName + ".tla"), tla)
        File.WriteAllText(Path.Combine(dir.FullName, moduleName + ".cfg"), cfg)
        let dumpFile = Path.Combine(dir.FullName, "states.dump")
        let psi = ProcessStartInfo("java")
        // SANY extracts the standard modules (TLC.tla, Integers.tla, ...) into java.io.tmpdir. Concurrent runs that share
        // /tmp race on those files, so each run gets its own.
        let javaTemp = Directory.CreateDirectory(Path.Combine(dir.FullName, "tmp")).FullName
        for a in
            [ "-XX:+UseParallelGC"; "-Djava.io.tmpdir=" + javaTemp
              "-cp"; jarPath; "tlc2.TLC"; "-workers"; "auto"; "-deadlock"
              "-metadir"; Path.Combine(dir.FullName, "states"); "-config"; moduleName + ".cfg" ] do
            psi.ArgumentList.Add a
        if dump then
            psi.ArgumentList.Add "-dump"
            psi.ArgumentList.Add dumpFile
        psi.ArgumentList.Add(moduleName + ".tla")
        psi.WorkingDirectory <- dir.FullName
        psi.RedirectStandardOutput <- true
        psi.RedirectStandardError <- true
        psi.UseShellExecute <- false
        let watch = Stopwatch.StartNew()
        use proc = Process.Start psi
        let stdout = proc.StandardOutput.ReadToEndAsync()
        let stderr = proc.StandardError.ReadToEndAsync()
        if not (proc.WaitForExit timeout) then
            proc.Kill true
            failwithf "TLC did not finish within %O" timeout
        let output = stdout.Result + stderr.Result
        let outcome, stats = parseOutput output
        let states =
            if dump && File.Exists dumpFile then Some(parseStates (File.ReadLines dumpFile))
            else None
        { Outcome = outcome
          Stats = stats
          Dump = states
          Output = output
          ExitCode = proc.ExitCode
          Elapsed = watch.Elapsed }
    finally
        try
            dir.Delete true
        with _ ->
            ()
