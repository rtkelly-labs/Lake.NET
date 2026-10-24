// -----------------------------------------------------------------------------
// ParseChangelog.cs
// Makes CHANGELOG.md the single source of truth for release versioning and
// release-note content, mirroring the Keep a Changelog layout the file uses.
//
// Default mode (CI): validates the changelog structure — [Unreleased] first,
// and, if a cut version section exists, that its heading carries a valid
// SemVer 2.0.0 label and an ISO-8601 date, with a non-empty body. A changelog
// with no cut version section yet is valid (nothing has been released).
//
// --release mode (release.yml): additionally requires a cut version section,
// and emits version / is_prerelease / tag outputs plus the sliced section body
// used as the GitHub release notes.
// -----------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

string? changelogPath = null;
string? notesOutPath = null;
string? githubOutputPath = null;
bool releaseMode = false;

var argv = Environment.GetCommandLineArgs();
for (int i = 1; i < argv.Length; i++)
{
    switch (argv[i])
    {
        case "--release":
            releaseMode = true;
            break;
        case "--changelog":
            changelogPath = RequireValue(argv, ref i);
            break;
        case "--notes-out":
            notesOutPath = RequireValue(argv, ref i);
            break;
        case "--github-output":
            githubOutputPath = RequireValue(argv, ref i);
            break;
        default:
            if (changelogPath is null)
            {
                changelogPath = argv[i];
            }
            else
            {
                Console.Error.WriteLine($"❌ Unknown argument: {argv[i]}");
                return 1;
            }
            break;
    }
}

changelogPath ??= "CHANGELOG.md";

if (!File.Exists(changelogPath))
{
    Console.Error.WriteLine($"❌ Changelog not found: {changelogPath}");
    return 1;
}

var headingRegex = new Regex(@"^## \[(?<label>[^\]]+)\](?:\s+-\s+(?<date>\d{4}-\d{2}-\d{2}))?\s*$");
var anyH2Regex = new Regex(@"^## ");
var semverRegex = new Regex(
    @"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
        + @"(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
        + @"(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
);

string[] lines = File.ReadAllLines(changelogPath);

// Collect every level-2 heading: its line index, and (if bracketed) its label and date.
var headings = new List<(int Line, string? Label, string? Date)>();
for (int i = 0; i < lines.Length; i++)
{
    if (!anyH2Regex.IsMatch(lines[i]))
    {
        continue;
    }

    Match m = headingRegex.Match(lines[i]);
    headings.Add(
        m.Success
            ? (i, m.Groups["label"].Value, m.Groups["date"].Success ? m.Groups["date"].Value : null)
            : (i, null, null)
    );
}

if (headings.Count == 0)
{
    Console.Error.WriteLine($"❌ {changelogPath} contains no '## ' sections.");
    return 1;
}

if (headings[0].Label != "Unreleased")
{
    Console.Error.WriteLine(
        "❌ The first '## ' section must be '## [Unreleased]' (Keep a Changelog order: newest first)."
    );
    return 1;
}

if (headings.Count < 2)
{
    if (releaseMode)
    {
        Console.Error.WriteLine(
            "❌ Nothing to release: the latest changes are still under [Unreleased].\n"
                + "   Cut a release PR that moves [Unreleased] under a '## [x.y.z] - YYYY-MM-DD' heading first."
        );
        return 1;
    }

    Console.WriteLine(
        "ℹ️ No version section has been cut yet; [Unreleased] holds everything. Nothing to validate."
    );
    return 0;
}

(int Line, string? Label, string? Date) release = headings[1];
int bodyEnd = headings.Count > 2 ? headings[2].Line : lines.Length;

if (release.Label is null)
{
    Console.Error.WriteLine(
        $"❌ Line {release.Line + 1}: expected a version heading '## [x.y.z] - YYYY-MM-DD', found: {lines[release.Line]}"
    );
    return 1;
}

if (release.Label == "Unreleased")
{
    Console.Error.WriteLine($"❌ Line {release.Line + 1}: duplicate '## [Unreleased]' section.");
    return 1;
}

Match semver = semverRegex.Match(release.Label);
if (!semver.Success)
{
    Console.Error.WriteLine(
        $"❌ Line {release.Line + 1}: '[{release.Label}]' is not a valid SemVer 2.0.0 version."
    );
    return 1;
}

if (release.Date is null)
{
    Console.Error.WriteLine(
        $"❌ Line {release.Line + 1}: version heading must carry its release date: '## [{release.Label}] - YYYY-MM-DD'."
    );
    return 1;
}

string body = string.Join("\n", lines, release.Line + 1, bodyEnd - release.Line - 1).Trim();
if (body.Length == 0)
{
    Console.Error.WriteLine(
        $"❌ The '[{release.Label}]' section has no content; it would publish an empty release."
    );
    return 1;
}

bool isPrerelease = semver.Groups[4].Success;

Console.WriteLine(
    $"✅ Changelog OK: releasing {release.Label} ({release.Date}), {(isPrerelease ? "prerelease" : "full release")}, {body.Split('\n').Length} note line(s)."
);

if (notesOutPath is not null)
{
    File.WriteAllText(notesOutPath, body + "\n");
    Console.WriteLine($"   Release notes written to {notesOutPath}");
}

if (githubOutputPath is not null)
{
    using var writer = new StreamWriter(githubOutputPath, append: true);
    writer.WriteLine($"version={release.Label}");
    writer.WriteLine($"tag=v{release.Label}");
    writer.WriteLine($"is_prerelease={((isPrerelease ? "true" : "false"))}");
}

return 0;

static string RequireValue(string[] argv, ref int i)
{
    if (i + 1 >= argv.Length)
    {
        Console.Error.WriteLine($"❌ {argv[i]} requires a value.");
        Environment.Exit(1);
    }

    return argv[++i];
}
