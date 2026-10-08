# Operator Runbook

For whoever is keeping this dataset and dashboard up to date day-to-day — no
programming or database knowledge needed. This covers the two things you'll
actually do: adding new sensor data, and opening the dashboard.

## One-time setup (first time only, on this PC)

1. Double-click **`setup.bat`** in this folder and wait for it to finish.
   It installs everything needed. You'll only need to do this once, or again
   if this folder is copied to a different PC.

If it says Python was not found, install Python from https://python.org first
(tick "Add python.exe to PATH" during install), then run `setup.bat` again.

## When new sensor data arrives

1. **Download the new files from Seafile** (the shared folder where the
   sensors' data is stored).
2. **Sort them into the matching folder** in `data\raw\`:

   | Where the file came from | Put it in |
   |---|---|
   | Empower, green roof site | `data\raw\greenroof\Empower_Greenroof\` |
   | Kissel, green roof site | `data\raw\greenroof\Kissel_GreenRoof_Data\` |
   | Empower, parking lot site | `data\raw\parkplatz\Empower_Parkplatz_data\` |
   | Kissel, parking lot site | `data\raw\parkplatz\Kissel_Parkplatz_data\` |
   | Black-globe temperature logger | `data\raw\black_globes\` |

   Don't worry about duplicates or getting the exact filename right — just
   drop the files in (`.csv` for greenroof/parkplatz, `.dat` for black-globe).
   You don't need to remove old files first, and you don't need to remove
   anything you've already dropped in: every update automatically figures out
   which files are new or have changed and only processes those, so it's
   always safe to just add files and run the update.

3. **Double-click `Run Update.bat`** and wait. This can take a few minutes —
   the window will tell you when it's done. You can also do this from inside
   the dashboard itself (see the ⚙️ Operations view, described below).

   *Disk space:* an update briefly needs roughly double the database's size
   free (it builds the new version alongside the old one before swapping) —
   check the Operations view for the current size if you're ever unsure.

4. **Check it worked**: the window prints `UPDATE COMPLETED` at the end. If
   instead it says the update failed, the live dashboard is *not* affected —
   it keeps showing the previous data untouched. See "If something goes
   wrong" below.

## Opening the dashboard

Double-click **`Open Dashboard.bat`**. It opens in your web browser after a
few seconds. Leave that window open while you're using the dashboard —
closing it stops the dashboard.

Inside the dashboard, the left sidebar has a **View** switch:
- **📊 Dashboard** — the analysis views (this is what you'll use most).
- **⚙️ Operations** — an in-browser version of steps 3–4 above (a "Run
  Update Now" button, current database size/row count, a preview of the 20
  most recent rows, and the latest run's log), if you'd rather not use the
  `.bat` files directly.
- **📄 Browse Data** — see below.
- **🔍 Query Data** — see below.

## Looking at the raw data yourself

Two views cover this, depending on what you're after — both read-only:

- **📄 Browse Data** — just page through the whole dataset, no SQL needed.
  Pick a table, pick how many rows per page, click Previous/Next or jump
  straight to a page number. This is the one to reach for right after an
  update if you want to actually look through the data yourself, or the
  quick "Preview the 20 most recent rows" panel on the Operations page
  covers the common "did the new data show up" check without leaving that
  page at all.
- **🔍 Query Data** — a SQL box for your own queries, plus a way to download
  the *complete* result of a query as a CSV (not just the on-screen
  preview) — useful for getting a slice of the data into Excel or another
  tool. One thing to know: Excel itself can only open about 1.05 million
  rows in one sheet, so a CSV of the entire dataset (2M+ rows) won't fully
  open there — narrow the query to a date range or a specific year first if
  you want it to open cleanly in Excel.

See `instructions/DATASET_ACCESS_GUIDE.md` for other ways to access the data
too (including from Python/pandas or a desktop database tool).

**Getting a full export (e.g. to open in Excel):** the on-screen preview is
capped at 10,000 rows so the browser doesn't choke on it, but if your query
returns more than that, a **"📥 Prepare full result for download"** button
appears — click it, then **"⬇ Download full result as CSV"**, and you get
every row your query matched, not just the preview. One thing to know:
Excel itself can only open about 1.05 million rows in one sheet, so if your
query is broader than that (e.g. the entire dataset, 2M+ rows), narrow it
with a date range or a specific year first — the CSV will still have
everything, but Excel would silently cut off what it can't hold.

## Optional: run updates automatically on a schedule

If you'd rather not remember to click `Run Update.bat` yourself, Windows can
run it for you every couple of weeks:

1. Open **Task Scheduler** (search for it in the Start menu).
2. **Create Basic Task** → name it e.g. "Bingen Green Roof Update".
3. Trigger: **Weekly** (or **Monthly**), pick a day/time.
4. Action: **Start a program** → Program/script: browse to `Run Update.bat`
   in this folder.
5. Finish. The PC needs to be on (or wake on schedule) at that time for it
   to run — it doesn't need the dashboard open.

This is optional — the manual double-click always works too, whenever you
know new data has actually arrived.

## If something goes wrong

- An update failure never breaks the dashboard — it always keeps showing the
  last successful data until an update actually succeeds.
- Open the ⚙️ Operations view in the dashboard and expand "Show log" to see
  exactly what happened on the last run.
- Most failures are a malformed or unexpected CSV file. Check whether a
  recently-added file looks different from the others in its folder (wrong
  columns, empty, wrong format) and try removing just that one file, then
  run the update again.
- If the log mentions disk space or "no space left", free up some space
  (see the disk-space note above) and run the update again.
- If you're stuck, the original project write-up
  (`instructions/PIPELINE_GUIDE.md`) has more technical detail, or reach out
  to whoever handed this project over to you.
