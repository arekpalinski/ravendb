# Prompt for the Claude session on the Ubuntu box

Paste the block below, replacing `<R>` with the subtask: R1 (RavenDB-27651), R2 (RavenDB-27652), R3 (RavenDB-27653),
R4 (RavenDB-27654), R5 (RavenDB-27655) or R6 (RavenDB-27656).

```text
You are running the Linux half of a RavenDB 8.0 storage retest (Voron v8 Testing, Phase III, RavenDB-27650).
The write-path performance changes (RavenDB-27377) merged into v8.0 changed journal writes, recovery, journal reuse,
the Voron format and hole punching, so earlier test results have to be re-confirmed.

Rules:
- Do not push, do not open PRs, do not post to YouTrack or GitHub, do not create git worktrees.
- Do not change product code (src/). Test-tool fixes are allowed only when a tool cannot run on Linux; describe each one.
- Stop at the first unexpected failure: keep the data dir, collect evidence, report. Do not try to fix the product.
- Ask me before anything that needs sudo, deletes data outside the test dirs, or downloads files.

Setup:
- Repo: git@github.com:arekpalinski/ravendb.git, branch RavenDB-Phase-III-retest (pull it). .NET 10 SDK.
- Build: dotnet build test/Tryouts/Tryouts.csproj -c Release (and -c Debug when the runbook says so).
- Check `findmnt -T /tmp`; if it is tmpfs, put every data dir under ~ (pass --dir / --data / env vars as the runbook says).
- Check `df -h ~` before every row. With less than about 25 GB free, use the "Low disk" settings in the README and delete
  each row's data dir once it passed (ask me first if a dir is outside the test dirs).
- Read test/RunBooks/RavenDB-27650/README.md first (tools, profiles, Linux notes), then test/RunBooks/RavenDB-27650/<R>-*.md.

Do:
- Run every Linux row of the <R> runbook, in order, with the commands given there.
- For each row record: command, verdict (PASS/FAIL), the numbers the runbook asks for, and anything unexpected.

Report, at the end, exactly this:
1. Header: commit (git log -1 --format=%h), `uname -r`, distro, filesystem of the data dir (`df -T <dir>`), disk model (`lsblk -d -o NAME,MODEL,ROTA`).
2. A markdown table: | Row | Pass/Fail | Notes / evidence |
3. For every FAIL: the exact command, the last 50 lines of tool output, and a tarball made with
   `tar czf evidence-<row>-<date>.tgz <data dir> <logs>`, with its path.
```

The user pastes the report back into the Windows session, which drafts the YouTrack comment.
