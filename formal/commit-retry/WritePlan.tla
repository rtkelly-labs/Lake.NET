------------------------------ MODULE WritePlan ------------------------------
EXTENDS Integers

\* The writer binds rows before the catalog loads the current metadata pointer.
\* A metadata-only change can therefore precede catalog CAS without changing the data head.
CONSTANTS BugAuthority, BugRetryRebind
VARIABLES schemaVersion, specVersion, metadataVersion, originalSchema, fileSpec,
          cachedSchema, expectedVersion, phase, retried, publishedSchemaCorrect,
          publishedSpecCorrect, publishedSpecVersion
vars == <<schemaVersion, specVersion, metadataVersion, originalSchema, fileSpec,
          cachedSchema, expectedVersion, phase, retried, publishedSchemaCorrect,
          publishedSpecCorrect, publishedSpecVersion>>

Init == /\ schemaVersion = 0 /\ specVersion = 0 /\ metadataVersion = 0
        /\ originalSchema = -1 /\ fileSpec = -1 /\ cachedSchema = 0
        /\ expectedVersion = 0 /\ phase = "new" /\ retried = FALSE
        /\ publishedSchemaCorrect = TRUE /\ publishedSpecCorrect = TRUE
        /\ publishedSpecVersion = -1

Plan == /\ phase = "new" /\ originalSchema' = schemaVersion
        /\ cachedSchema' = schemaVersion /\ fileSpec' = specVersion
        /\ phase' = "planned"
        /\ UNCHANGED <<schemaVersion, specVersion, metadataVersion, expectedVersion,
                        retried, publishedSchemaCorrect, publishedSpecCorrect, publishedSpecVersion>>

ChangeSchema == /\ schemaVersion = 0 /\ schemaVersion' = 1
                /\ metadataVersion' = metadataVersion + 1
                /\ UNCHANGED <<specVersion, originalSchema, fileSpec, cachedSchema,
                                expectedVersion, phase, retried, publishedSchemaCorrect,
                                publishedSpecCorrect, publishedSpecVersion>>

\* Spec 0 remains archived when the default becomes spec 1.
ChangeSpec == /\ specVersion = 0 /\ specVersion' = 1
              /\ metadataVersion' = metadataVersion + 1
              /\ UNCHANGED <<schemaVersion, originalSchema, fileSpec, cachedSchema,
                              expectedVersion, phase, retried, publishedSchemaCorrect,
                              publishedSpecCorrect, publishedSpecVersion>>

CatalogRead == /\ phase = "planned"
               /\ expectedVersion' = metadataVersion
               /\ phase' = IF ~BugAuthority /\ cachedSchema # schemaVersion
                            THEN "conflict" ELSE "catalog-ready"
               /\ UNCHANGED <<schemaVersion, specVersion, metadataVersion, originalSchema,
                               fileSpec, cachedSchema, retried, publishedSchemaCorrect,
                               publishedSpecCorrect, publishedSpecVersion>>

CatalogSwap ==
    /\ phase = "catalog-ready"
    /\ IF expectedVersion = metadataVersion
       THEN /\ phase' = "done" /\ metadataVersion' = metadataVersion + 1
            /\ publishedSchemaCorrect' = (originalSchema = schemaVersion)
            /\ publishedSpecCorrect' = (fileSpec \in 0..specVersion)
            /\ publishedSpecVersion' = specVersion
       ELSE /\ phase' = "conflict"
            /\ UNCHANGED <<metadataVersion, publishedSchemaCorrect, publishedSpecCorrect, publishedSpecVersion>>
    /\ UNCHANGED <<schemaVersion, specVersion, originalSchema, fileSpec, cachedSchema,
                    expectedVersion, retried>>

Refresh == /\ phase = "conflict" /\ ~retried
           /\ cachedSchema' = schemaVersion /\ retried' = TRUE
           /\ phase' = IF ~BugRetryRebind /\ originalSchema # schemaVersion
                        THEN "rejected" ELSE "planned"
           /\ UNCHANGED <<schemaVersion, specVersion, metadataVersion, originalSchema,
                           fileSpec, expectedVersion, publishedSchemaCorrect,
                           publishedSpecCorrect, publishedSpecVersion>>

Stop == /\ phase = "conflict" /\ retried /\ phase' = "rejected"
        /\ UNCHANGED <<schemaVersion, specVersion, metadataVersion, originalSchema,
                        fileSpec, cachedSchema, expectedVersion, retried,
                        publishedSchemaCorrect, publishedSpecCorrect, publishedSpecVersion>>

Next == Plan \/ ChangeSchema \/ ChangeSpec \/ CatalogRead \/ CatalogSwap \/ Refresh \/ Stop

TypeOK == /\ schemaVersion \in 0..1 /\ specVersion \in 0..1 /\ metadataVersion \in 0..3
          /\ originalSchema \in -1..1 /\ fileSpec \in -1..1 /\ cachedSchema \in 0..1
          /\ expectedVersion \in 0..3 /\ phase \in {"new", "planned", "catalog-ready", "conflict", "done", "rejected"}
          /\ retried \in BOOLEAN /\ publishedSchemaCorrect \in BOOLEAN
          /\ publishedSpecCorrect \in BOOLEAN /\ publishedSpecVersion \in -1..1
PublishedSchemaCorrect == publishedSchemaCorrect
PublishedSpecCorrect == publishedSpecCorrect
\* Deliberately false coverage property proving an old-spec append can commit under a new default spec.
NoArchivedSpecWitness == ~(phase = "done" /\ fileSpec = 0 /\ publishedSpecVersion = 1)
=============================================================================
