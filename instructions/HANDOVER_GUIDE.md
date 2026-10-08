# Handover Guide

For you (Joseph), not the assistant — this is the one-time process of moving
operation onto the assistant's PC. `ASSISTANT_RUNBOOK.md` is what you hand
them once this is done; this doc is how you get there.

## What needs to move

1. **The code** — everything tracked in git (the repo itself).
2. **The database** — `outputs/bingen_greenroof.duckdb` (~1.8 GB, gitignored,
   won't come along with a normal `git clone`/pull).
3. Optionally, the raw CSV archive (`data/raw/`, gitignored) if you want the
   assistant able to fully rebuild from scratch later — otherwise the .duckdb
   file alone is enough for them to keep updating going forward, since every
   update just needs whatever new CSVs arrive from here on.

## Steps

1. **Build and validate the database once, here, before transferring anything.**
   Run a full update (`Run Update.bat`, or `python scripts/run_update.py`)
   so `outputs/bingen_greenroof.duckdb` reflects the latest data. Confirm it
   with `python scripts/verify_pipeline.py`.

2. **Get the code onto the new PC.** Easiest: `git clone` the repo there
   directly if the PC has git and access to wherever the repo is hosted.
   Otherwise, copy the folder over by USB drive or however you'd move any
   other project folder — just exclude `.venv/`, `data/raw/`, and `outputs/`
   (all gitignored, and `.venv`/`outputs` in particular are large and
   regenerated anyway).

3. **Transfer the database file separately.** It's gitignored on purpose (too
   large for git), so it needs its own transfer:
   - **USB drive** — simplest for a one-time ~1.8 GB copy if you're physically
     at both machines.
   - **Seafile** — upload `bingen_greenroof.duckdb` to a shared folder the
     assistant already has access to (the same tool used for sensor data), and
     have them download it. Convenient if you're not on-site.
   - Either way, it goes at `outputs/bingen_greenroof.duckdb` inside the copied
     repo folder on the new PC — same relative path as here.

4. **Run setup on the new PC.** Double-click `setup.bat`. This creates the
   Python environment; it does not touch the database file you already copied.

5. **Verify it worked.** Open the dashboard there and confirm the row count /
   date range in the sidebar matches what you saw here. The ⚙️ Operations view
   also shows the database size and row count directly.

6. **Hand off `instructions/ASSISTANT_RUNBOOK.md`** as their ongoing reference,
   and walk them through adding new data once, live, so they've done it before
   you're not around to ask.

7. **Optional:** set up the Windows Task Scheduler job described in the
   runbook, on their PC, if you want updates to happen automatically rather
   than relying on them remembering.

## If the PC changes again later

Nothing here is tied to a specific machine — the same three steps (copy code,
copy the `.duckdb` file, run `setup.bat`) work for moving to any future PC.
