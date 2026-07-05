# Apache Onboarding Checklist

This is a **human checklist**, not something that can be automated by a
bot or CLI tool. Every step below involves your real identity (legal
signature, email account, ASF-issued credentials) — an AI assistant cannot
sign an ICLA, create an Apache account, or send email on your behalf.

Do these in order.

## 1. Get an email address you'll use long-term for ASF work

Use a personal address you control indefinitely (not a corporate address
you'll lose on job change) — this becomes your identity for every ASF
system (JIRA, mailing lists, Git commits, PGP key).

## 2. Sign the Individual Contributor License Agreement (ICLA)

1. Read and fill out the ICLA: https://www.apache.org/licenses/icla.pdf
2. Sign it (wet signature, scan, or the ASF's electronic signing service
   at https://selfserve.apache.org/ if available for this form).
3. Submit per https://www.apache.org/licenses/#clas — either email the
   signed PDF to `secretary@apache.org` or use Apache Self-Serve Portal.
4. Wait for confirmation (the ASF Secretary processes these manually;
   allow a few business days).

If your employer owns IP in your contributions, you additionally need a
Corporate CLA (CCLA) signed by an authorized company representative —
template at https://www.apache.org/licenses/cla-corporate.pdf.

## 3. Create your Apache account (once ICLA is on file)

- The Incubator PMC (or a project's root committer) files an account
  request via `id.apache.org` on your behalf, referencing your ICLA.
  You don't self-register — someone with existing ASF access sponsors you.
- You'll receive your `<username>@apache.org` email forwarding address and
  LDAP credentials, which unlock JIRA, Confluence, Git, and mailing list
  admin.

## 4. Request a JIRA project space

- If the project is entering the Incubator: JIRA project keys for
  incubating projects are typically requested via an INFRA ticket
  (https://issues.apache.org/jira/projects/INFRA) once the Incubator
  proposal is accepted — file "Request new JIRA project: SLUICE" with
  the Incubator PPMC's approval linked.
- Until then, you can track issues in GitHub Issues or a self-hosted JIRA;
  just be ready to migrate history when the ASF JIRA project is granted.

## 5. Subscribe to mailing lists

Apache mailing lists are self-service via email, not a web form:

- To subscribe: send a blank email to `dev-subscribe@sluice.apache.org`
  (or, pre-incubation, `general-subscribe@incubator.apache.org` to follow
  Incubator-wide discussion).
- You'll get a confirmation email — reply to it (or click the link) to
  complete the subscription.
- To unsubscribe later: `dev-unsubscribe@sluice.apache.org`.
- Archives are public: https://lists.apache.org/list.html?dev@sluice.apache.org
  (once the list exists).

## 6. Generate a PGP key (needed before your first release)

```bash
gpg --full-generate-key
gpg --keyserver keyserver.ubuntu.com --send-keys <YOUR_KEY_ID>
```

Add the key's fingerprint to the project's `KEYS` file once the project has
one, and get it cross-signed by at least one existing ASF committer if
possible (builds the web of trust ASF release verification relies on).

## 7. File the Incubator proposal

See `docs/INCUBATOR_PROPOSAL.md` in this repo for the drafted proposal.
Submission steps:

1. Find a **Champion** — an existing ASF member willing to sponsor the
   proposal (required; the Incubator won't accept a proposal with no
   champion).
2. Post the proposal to the Incubator general list
   (`general@incubator.apache.org`) as a `[DISCUSS]` thread for feedback.
3. Once discussion settles, call a `[VOTE]` on the same list. Passing
   requires lazy consensus / a minimum of 3 binding +1 votes from the
   Incubator PMC, no vetoes.
4. On passing, INFRA sets up the project's Git repo, mailing lists, and
   JIRA project under the ASF Incubator umbrella.

## 8. Ongoing hygiene

- Renew nothing — ICLA/account are permanent, but keep your `@apache.org`
  forwarding address active (some ASF systems periodically check mail
  deliverability).
- Report security issues privately (see `CONTRIBUTING.md`).
- All project decisions of substance happen on-list — if it didn't happen
  on the mailing list, it didn't happen (ASF's core operating principle).

## Where this stands today

This repository is **pre-Incubator**: no ASF JIRA project, no
`@sluice.apache.org` mailing list, no ASF Git hosting yet. `CONTRIBUTING.md`
and `DEVELOPER_GUIDELINES.md` describe the target-state workflow so
contributors already work in the ASF style, easing the eventual transition.
