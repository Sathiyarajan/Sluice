# Incubator Submission — Email Templates

Ready-to-send drafts for the actual ASF submission process. Send these
yourself from your own mail client to `general@incubator.apache.org` (you
must be subscribed first — see `docs/APACHE_ONBOARDING.md` step 5,
`general-subscribe@incubator.apache.org`). Fill every `<...>` placeholder
before sending. Send in this order, each only after the previous step
settles.

---

## 1. Recruiting a Champion + Mentors (send first)

No proposal should go to `[DISCUSS]` without at least informal Champion
interest lined up — this email is how you find one. Champions/Mentors are
existing ASF Members or officers; they self-nominate in reply.

**To:** general@incubator.apache.org
**Subject:** Seeking Champion/Mentors for Sluice (Spark-based config-driven ETL)

```
Hello Incubator community,

I'm looking to propose "Sluice" for incubation and am seeking a Champion
and Mentors before posting the formal [DISCUSS] thread.

Sluice is a config-driven ingestion/ETL framework built on Apache Spark:
one YAML file per dataset drives both the Spark ingestion job (readers ->
transforms -> data-quality checks -> writers) and its orchestration —
generating Apache Airflow DAGs or Autosys JIL from the same config, so two
schedulers never drift out of sync.

Project repo: <your repo URL>
Draft proposal: <link to docs/INCUBATOR_PROPOSAL.md, e.g. a raw GitHub URL>

I believe this fits well alongside existing data-engineering podlings/TLPs
(Airflow, Iceberg, Hop) and would appreciate guidance from anyone familiar
with Incubator process, or willing to serve as Champion or Mentor.

Thanks,
<Your name>
<your email>
```

---

## 2. [DISCUSS] thread (send once you have at least informal Champion interest)

**To:** general@incubator.apache.org
**Subject:** [DISCUSS] Sluice Incubator Proposal

```
Hello Incubator community,

I'd like to propose Sluice for the Apache Incubator. Full draft proposal
below / attached (also at <link>). Feedback welcome before I call a vote.

<paste full contents of docs/INCUBATOR_PROPOSAL.md here>

Thanks,
<Your name>
```

Leave this thread open long enough for substantive feedback (commonly a
week or more) and incorporate changes into `docs/INCUBATOR_PROPOSAL.md`
before moving to a vote.

---

## 3. [VOTE] thread (send only after DISCUSS settles and you have 3+ Mentors)

**To:** general@incubator.apache.org
**Subject:** [VOTE] Accept Sluice into the Apache Incubator

```
Hello Incubator community,

Following the [DISCUSS] thread (<link to archived thread>), I'd like to
call a vote to accept Sluice into the Apache Incubator.

Proposal: <link to final docs/INCUBATOR_PROPOSAL.md>

[ ] +1 Accept Sluice into the Apache Incubator
[ ] +0 Abstain
[ ] -1 Reject (please state reason)

This vote will run for at least 72 hours per Incubator policy.

Thanks,
<Your name>
```

Passing requires a minimum of 3 binding +1 votes from the Incubator PMC and
no unaddressed vetoes (lazy consensus otherwise).

---

## 4. After acceptance: INFRA ticket for project resources

**Where:** https://issues.apache.org/jira/projects/INFRA (file a new issue)
**Summary:** Set up ASF infrastructure for incubating podling: Sluice

```
Requesting standard podling infrastructure now that Sluice has been
accepted into the Incubator (vote result: <link to vote-result email>):

- Git repository: sluice.git
- Mailing lists: dev@sluice.apache.org, private@sluice.apache.org
- JIRA project: SLUICE
- Podling website space (per Incubator website requirements)

Vote thread: <link>
Champion: <name>
Mentors: <names>
```

---

## Status tracking while this proposal is in flight

Keep `docs/INCUBATOR_PROPOSAL.md` as the single source of truth — update it
in place as DISCUSS feedback comes in, and only copy-paste the *final*
version into the VOTE email so there's no drift between what was voted on
and what's in the repo.
