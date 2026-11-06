module Lake.CommitProtocol.Tests.Support

open System
open System.IO
open Xunit

/// formal/: the nearest ancestor of the test binaries holding Formal.slnx.
let formalRoot =
    let rec up (dir: DirectoryInfo) =
        if isNull dir then failwith "Formal.slnx not found above the test binaries"
        elif File.Exists(Path.Combine(dir.FullName, "Formal.slnx")) then dir.FullName
        else up dir.Parent
    up (DirectoryInfo AppContext.BaseDirectory)

let private flag (name: string) = Environment.GetEnvironmentVariable name = "1"

/// The TLC jar (TLA2TOOLS_JAR). Skips the test when it is absent, unless FORMAL_REQUIRE_TLC=1, as in CI.
let tlcJar () =
    match FsTla.Tlc.jar () with
    | Some jar -> jar
    | None ->
        if flag "FORMAL_REQUIRE_TLC" then failwith "FORMAL_REQUIRE_TLC=1 but TLA2TOOLS_JAR does not name a jar"
        Skip.If(true, "TLC is not installed: set TLA2TOOLS_JAR to tla2tools.jar")
        failwith "unreachable"

/// Compares text with a committed file; FORMAL_UPDATE_GOLDEN=1 rewrites the file instead.
let golden (relativePath: string) (actual: string) =
    let path = Path.Combine(formalRoot, relativePath)
    if flag "FORMAL_UPDATE_GOLDEN" then
        Directory.CreateDirectory(Path.GetDirectoryName path) |> ignore
        File.WriteAllText(path, actual)
    else
        if not (File.Exists path) then failwithf "%s is missing: run the tests with FORMAL_UPDATE_GOLDEN=1" relativePath
        let expected = File.ReadAllText(path).Replace("\r\n", "\n")
        if expected <> actual then
            failwithf "%s is out of date: the F# model changed. Review the diff, then run with FORMAL_UPDATE_GOLDEN=1" relativePath
