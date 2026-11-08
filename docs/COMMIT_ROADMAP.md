# Ten-day commit roadmap

The whole project exists today, but the history must be **ten real commits on ten real days**.
`tools/commit_day.sh N` stages exactly that day's files from `tools/days.txt` and makes one commit.
It uses the real clock (no backdating), refuses empty commits, refuses out-of-order days, and
refuses a second commit on the same calendar day (override only with `--allow-same-day`, which
would defeat your ten-day goal).

| Day | Commit message | Files added |
|---|---|---|
| 1 | Initial MeshWeaver project architecture and setup | config, interfaces, package skeleton, `docs/ARCHITECTURE.md`, tools |
| 2 | Add Flask backend and frontend structure | `app.py`, mock adapters (Modules 1, 2, 4), `base.html`, logo |
| 3 | Develop responsive MeshWeaver dashboard | five page templates, CSS, `app.js`, `dashboard_api.py` |
| 4 | Implement CPU and RAM monitoring | `load_monitor.py` + tests |
| 5 | Develop Gossip Protocol Engine | `engine.py` |
| 6 | Add distributed load table and periodic updates | `load_table.py` + tests |
| 7 | Implement heartbeat monitoring and peer failure detection | `heartbeat.py` + tests |
| 8 | Add least-loaded node selection and REST APIs | `gossip_api.py` + API tests |
| 9 | Integrate monitoring charts and module testing | `charts.js`, engine and UDP integration tests, `launch_mesh.py` |
| 10 | Finalize project documentation and integration | `README.md`, `docs/API.md`, `docs/INTEGRATION.md`, this file |

## One-time setup
```bash
cd meshweaver
git config user.name "Your Name" && git config user.email "you@example.com"
git init -b main
git remote add origin https://github.com/<you>/meshweaver.git    # create the empty repo on GitHub first
```

## Each day (one command, on a different calendar day)
```bash
tools/commit_day.sh 1 --push      # Day 1
tools/commit_day.sh 2 --push      # tomorrow
# ... up to 10
```
Check what will be committed before running it: `git add -n $(grep '^3|' tools/days.txt | cut -d'|' -f3)`.

## Honest notes
* Because the code was written in one session, each commit adds *finished* files for that milestone;
  it is not a record of code that evolved over ten days. Use the days to read, run and tweak each
  part (and commit those improvements on top) so the history reflects your own work on Module 3.
* Intermediate commits import cleanly but the app first runs end-to-end at Day 8
  (the engine imports its table/heartbeat lazily so earlier files do not break).
* Day 3's gossip page asks for `charts.js`, which arrives on Day 9; the page works without it.
* `--push` only works after you add the `origin` remote and authenticate with GitHub yourself.
