----------------------------- MODULE CommitRetry -----------------------------
EXTENDS Integers, Sequences, FiniteSets

\* Two commands, each with at most MaxAttempts immutable prepared files.
\* A replacement changes key 0; appends use distinct keys 1 and 2.
CONSTANTS MaxAttempts, Modes, BugCAS, BugStaleSources, BugUnknownRetry, BugCleanup
Writers == 1..2
AppendAppend == [w \in Writers |-> "append"]
AppendReplace == [w \in Writers |-> IF w = 1 THEN "append" ELSE "replace"]
ReplaceReplace == [w \in Writers |-> "replace"]
Files == 0..(2 * MaxAttempts)
File(w, a) == (w - 1) * MaxAttempts + a
Owner(f) == IF f <= MaxAttempts THEN 1 ELSE 2
Rows(fs) == UNION {IF f = 0 THEN {0}
                  ELSE IF Modes[Owner(f)] = "replace" THEN {3 + Owner(f)}
                  ELSE {Owner(f)} : f \in fs}
Targets(fs) == {f \in fs : IF f = 0 THEN TRUE ELSE Modes[Owner(f)] = "replace"}
Intent(w, rs) == IF Modes[w] = "append" THEN rs \cup {w}
                 ELSE (rs \ {0, 4, 5}) \cup {3 + w}

VARIABLES version, head, live, retained, snapshots, disk, phase, attempt,
          observed, planned, sources, committed, expectedRows, uncertain,
          expired, lastAction, actor
vars == <<version, head, live, retained, snapshots, disk, phase, attempt,
          observed, planned, sources, committed, expectedRows, uncertain,
          expired, lastAction, actor>>
Reachable == UNION {snapshots[s] : s \in retained}

Init == /\ version = 0 /\ head = 0 /\ live = {0} /\ retained = {0}
        /\ snapshots = [f \in Files |-> IF f = 0 THEN {0} ELSE {}]
        /\ disk = {0} /\ phase = [w \in Writers |-> "new"]
        /\ attempt = [w \in Writers |-> 1]
        /\ observed = [w \in Writers |-> 0]
        /\ planned = [w \in Writers |-> {}]
        /\ sources = [w \in Writers |-> {}]
        /\ committed = <<>> /\ expectedRows = {0} /\ uncertain = {}
        /\ expired = FALSE /\ lastAction = "init" /\ actor = 0

Prepare(w) ==
    LET f == File(w, attempt[w])
        remove == IF Modes[w] = "append" THEN {}
                  ELSE IF attempt[w] = 1 THEN Targets(live) ELSE sources[w]
    IN /\ phase[w] = "new"
       /\ observed' = [observed EXCEPT ![w] = version]
       /\ sources' = [sources EXCEPT ![w] = remove]
       /\ planned' = [planned EXCEPT ![w] = (live \ remove) \cup {f}]
       /\ disk' = disk \cup {f}
       /\ phase' = [phase EXCEPT ![w] = "prepared"]
       /\ lastAction' = "prepare" /\ actor' = w
       /\ UNCHANGED <<version, head, live, retained, snapshots, attempt,
                       committed, expectedRows, uncertain, expired>>

Commit(w, ack) ==
    LET f == File(w, attempt[w])
    IN /\ phase[w] = "prepared"
       /\ (observed[w] = version \/ BugCAS)
       /\ version' = version + 1 /\ head' = f /\ live' = planned[w]
       /\ snapshots' = [snapshots EXCEPT ![f] = planned[w]]
       /\ retained' = retained \cup {f}
       /\ committed' = Append(committed, w)
       /\ expectedRows' = Intent(w, expectedRows)
       /\ phase' = [phase EXCEPT ![w] = IF ack THEN "done" ELSE "unknown"]
       /\ uncertain' = IF ack THEN uncertain ELSE uncertain \cup {f}
       /\ lastAction' = IF ack THEN "commit" ELSE "commit-lost-response"
       /\ actor' = w
       /\ UNCHANGED <<disk, attempt, observed, planned, sources, expired>>

Conflict(w) ==
    /\ phase[w] = "prepared" /\ observed[w] # version /\ ~BugCAS
    /\ phase' = [phase EXCEPT ![w] = "conflict"]
    /\ lastAction' = "conflict" /\ actor' = w
    /\ UNCHANGED <<version, head, live, retained, snapshots, disk, attempt,
                    observed, planned, sources, committed, expectedRows, uncertain, expired>>

Retry(w) ==
    /\ phase[w] = "conflict"
    /\ IF attempt[w] < MaxAttempts
           /\ (Modes[w] = "append" \/ sources[w] \subseteq live \/ BugStaleSources)
       THEN /\ phase' = [phase EXCEPT ![w] = "new"]
            /\ attempt' = [attempt EXCEPT ![w] = @ + 1]
            /\ lastAction' = "retry"
       ELSE /\ phase' = [phase EXCEPT ![w] = "rejected"]
            /\ UNCHANGED attempt /\ lastAction' = "reject"
    /\ actor' = w
    /\ UNCHANGED <<version, head, live, retained, snapshots, disk, observed,
                    planned, sources, committed, expectedRows, uncertain, expired>>

\* A request whose response was lost may not have applied. Its files stay protected.
LoseRequest(w) ==
    /\ phase[w] = "prepared"
    /\ phase' = [phase EXCEPT ![w] = "unknown"]
    /\ uncertain' = uncertain \cup {File(w, attempt[w])}
    /\ lastAction' = "request-lost" /\ actor' = w
    /\ UNCHANGED <<version, head, live, retained, snapshots, disk, attempt,
                    observed, planned, sources, committed, expectedRows, expired>>

\* Retained snapshot presence is positive evidence. Absence alone is not proof of failure.
ResolvePresent(w) ==
    /\ phase[w] = "unknown" /\ File(w, attempt[w]) \in retained
    /\ phase' = [phase EXCEPT ![w] = "done"]
    /\ lastAction' = "resolve-present" /\ actor' = w
    /\ UNCHANGED <<version, head, live, retained, snapshots, disk, attempt,
                    observed, planned, sources, committed, expectedRows, uncertain, expired>>

UnsafeResolveAbsent(w) ==
    /\ BugUnknownRetry /\ phase[w] = "unknown"
    /\ File(w, attempt[w]) \notin retained
    /\ phase' = [phase EXCEPT ![w] = "conflict"]
    /\ lastAction' = "resolve-absent-as-failure" /\ actor' = w
    /\ UNCHANGED <<version, head, live, retained, snapshots, disk, attempt,
                    observed, planned, sources, committed, expectedRows, uncertain, expired>>

\* One maintenance action may expire any non-head snapshot evidence, without changing data.
Expire(s) ==
    /\ ~expired /\ s \in retained \ {head}
    /\ retained' = retained \ {s} /\ expired' = TRUE
    /\ lastAction' = "expire" /\ actor' = s
    /\ UNCHANGED <<version, head, live, snapshots, disk, phase, attempt,
                    observed, planned, sources, committed, expectedRows, uncertain>>

\* Cleanup concerns abandoned attempts, not normal snapshot-expiry garbage collection.
Cleanup(w, a) ==
    LET f == File(w, a)
    IN /\ f \in disk
       /\ (a < attempt[w] \/ phase[w] = "rejected"
            \/ (BugCleanup /\ phase[w] = "unknown"))
       /\ (BugCleanup \/ (f \notin Reachable /\ f \notin uncertain))
       /\ disk' = disk \ {f}
       /\ lastAction' = "cleanup" /\ actor' = w
       /\ UNCHANGED <<version, head, live, retained, snapshots, phase, attempt,
                       observed, planned, sources, committed, expectedRows, uncertain, expired>>

Next == \/ \E w \in Writers : Prepare(w) \/ Conflict(w) \/ Retry(w)
                              \/ LoseRequest(w) \/ ResolvePresent(w)
                              \/ UnsafeResolveAbsent(w)
        \/ \E w \in Writers, ack \in BOOLEAN : Commit(w, ack)
        \/ \E s \in Files : Expire(s)
        \/ \E w \in Writers, a \in 1..MaxAttempts : Cleanup(w, a)

TypeOK == /\ version \in 0..(2 * MaxAttempts) /\ head \in Files
          /\ live \subseteq Files /\ retained \subseteq Files
          /\ snapshots \in [Files -> SUBSET Files] /\ disk \subseteq Files
          /\ phase \in [Writers -> {"new", "prepared", "conflict", "done", "unknown", "rejected"}]
          /\ attempt \in [Writers -> 1..MaxAttempts]
          /\ observed \in [Writers -> 0..(2 * MaxAttempts)]
          /\ planned \in [Writers -> SUBSET Files] /\ sources \in [Writers -> SUBSET Files]
          /\ committed \in Seq(Writers) /\ expectedRows \subseteq {0, 1, 2, 4, 5}
          /\ uncertain \subseteq Files /\ expired \in BOOLEAN
RowsMatchSerialIntent == Rows(live) = expectedRows
AtMostOnce == \A w \in Writers : Cardinality({i \in 1..Len(committed) : committed[i] = w}) <= 1
ReachableFilesExist == Reachable \subseteq disk
UnknownFilesPreserved == uncertain \subseteq disk
HeadRetained == head \in retained /\ snapshots[head] = live

\* Deliberately false coverage property: TLC emits a shortest successful retry schedule.
NoAppendRetryWitness == ~(phase[1] = "done" /\ phase[2] = "done"
                         /\ attempt[1] = 2 /\ attempt[2] = 1)
=============================================================================
