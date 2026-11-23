# Parquet.Net is retired upstream

**Status:** [aloneguid/parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) no longer exists, and neither does
the `aloneguid` GitHub account. Parquet.Net will get no further releases. The NuGet package stays published.
**Recorded:** 2026-11-23.
**Our copy:** [rtkelly-labs/parquet-dotnet](https://github.com/rtkelly-labs/parquet-dotnet), the Lake.Parquet fork,
holds the full upstream history. See [What the fork holds](#what-the-fork-holds).

## The author's statement

Ivan Gavryliuk (aloneguid) explained the decision on the project's page,
<https://www.aloneguid.uk/projects/parquet-dotnet/#why-am-i-killing-it>. The section is kept here verbatim because
the page may not stay up, and the site refuses automated fetches. It was copied by hand on 2026-11-23.

> **Why am I killing it?**
>
> I personally believe .NET is the wrong ecosystem for handling Apache Parquet files. There are much better
> alternatives like Apache Spark, DuckDB, Apache Arrow, etc. While this hobby lasted for quite some time, practically
> in 2026 it makes no sense whatsoever. I'd personally advise to move to other more suitable platforms for data
> processing rather than "switching" to other .NET libraries, but that's just me.
>
> I personally don't have time to support this project, nor any desire to transfer ownership, mostly due to the
> current trend in software development and having zero trust that a potential future owner won't screw it up.
> Unfortunately the last year was spent mostly rejecting LLM-generated slop making the precious rare hobby time
> unpleasant, rather than something I was looking forward to.
>
> I haven't used this project in any of my code for some time now. It was used in the wild extensively in the past
> in some very critical scenarios, therefore the library is considered more or less stable.
>
> I no longer agree with GitHub's terms & conditions or its roadmap. When I joined, GitHub was awesome, but today I
> see it as a place where everyone throws their LLM-generated slop, or a platform for training LLMs. Unlike popular
> opinions, I am not anti-"AI" or any kind of nonsense like that, just to be clear. Therefore it would be morally
> wrong to continue working on that platform. Changing hosting providers could work if I had the energy to do so.
>
> General disappointment in .NET 11 and the current state of .NET probably added quite a bit to it. I can go on about
> this indefinitely, and it was really hard for me to give up this, in my opinion, great platform. But, in my
> personal opinion, it never had or will have a future I am looking for.
>
> If you were relying on Parquet.Net in your hobby projects, don't worry: the NuGet package is not going anywhere, so
> you have plenty of time to migrate to something better. If you were a company relying on this library, that was a
> huge mistake. My personal GitHub account was sponsored at $10/month minus taxes, and most likely not because of
> this project, so I know you were not serious.
>
> In general, I really enjoyed working on this project in the past, it taught me a lot about .NET internals,
> performance optimisations, trade-offs between performance and usability, backward compatibility and on and on and
> on. I have also created a companion library that reads and writes Delta Lake tables, implementing locking,
> transactions, schema evolution, partitioning, z-ordering and so on (hence some visible slowdown in contributing to
> the Parquet.Net in the recent years). Unfortunately, for the similar reasons, the library has never left the
> private Gitea instance before I could kill it, but was used in production for a couple of years with mostly
> success.

## What the fork holds

Checked on 2026-11-23:

- **History:** upstream `master` up to its last commit, `658e1caf` "LLM stance update" (2026-10-26T12:11:50Z). The
  fork was created the next day, 2026-10-27, so it missed nothing.
- **Tags:** all 143 of upstream's tags, up to `6.1.0`. That was the last Parquet.Net release, published to NuGet on
  2026-08-11.
- **Branches:** upstream's other branches as well, e.g. `feature/variant-encoding` and `copilot/sub-pr-682`.
- **Checked against a separate download:** a source zip of 6.1.0, downloaded separately, has the same content as
  `658e1caf`, so it has no commits we lack.
  - It is missing `src/Parquet.Test` and the binaries in `tools/`.
  - Its only extra files are local IDE settings that the repository ignores (`*.csproj.user`, `*.DotSettings.user`,
    `launchSettings.json`).
- **License:** MIT, copyright 2017 Ivan Gavryliuk. Keep `LICENSE` and the attribution in anything built from it.

## What changes for us

- **Nothing to sync from, but the stack stays.** Oxbow was set up to keep the fork as upstream plus a short stack of
  recorded changes so it could follow upstream. There is no upstream to follow now, so that purpose is gone.
  The structure is still worth keeping. `658e1caf` becomes a fixed base, and oxbow can still rebuild the
  `lake(NNNN)` stack on top of it from its record, in the same way every time: reordering, folding in `fixup!`
  commits, splitting or dropping a change. That keeps each change separate and easy to review. The
  `aloneguid/parquet-dotnet` upstream recorded in rig and oxbow stays as a record of where the code came from;
  nothing can be fetched from it.
- **The fork's parent on GitHub is wrong.** With the original repository deleted, GitHub re-rooted the fork network
  at `TikiBill/parquet-dotnet`, an unrelated copy last pushed in 2020. GitHub Support can detach the fork if that
  matters.
- **The upstream proposal is moot.** The [group field IDs proposal](parquet-net-group-field-ids.md) has nowhere to
  be filed. If we still want it, it becomes a change in the fork.
- **Respect the author's position.** The author chose not to hand the project to anyone. Lake.Parquet already ships
  under its own name and package id. Keep it that way, and never present the fork as Parquet.Net, as its successor,
  or as endorsed by its author.
