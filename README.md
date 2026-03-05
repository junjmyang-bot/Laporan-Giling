# Laporan Giling Web App

Single-entry `Laporan Giling` app with dynamic form branching:
- `Barang tidak butuh steril` (non-steril)
- `Barang butuh steril` (steril-required)

## Features
- Mobile-first single form flow.
- Kupas top structure: `Tim laporan + Tanggal kerja + Pelapor + PIN Tim`.
- Team lock flow: `Buka Tim` / `Ambil Alih Tim` before form entry.
- System timestamp (read-only behavior at submit time).
- Telegram-first delivery with edit-first fallback to send new message.
- Auto root-message tracking by `(team_id + work_date + report_type)` for future edits.
- Google Sheets backup append via webhook.
- Pending queue + explicit retry for unstable network.
- Idempotency key per submission lifecycle.
- Team lock model: `Open Team` / `Take Over Team` with lock token+version checks.
- Work-state persistence by `(team_id + work_date)` and quick context reload.
- 30-minute anti-miss reminder from latest successful report.
- Destructive reset requires explicit confirmation.

## Run
```powershell
py -m pip install -r requirements.txt
py -m streamlit run app.py --server.address 127.0.0.1 --server.port 8501
```

Or use helper script:
```powershell
./run.ps1 -Install
./run.ps1 -CheckTelegram
./run.ps1
```

## Environment Variables
Copy `.env.example` and set:
- `APP_TIMEZONE` default `Asia/Jakarta`
- `TEAM_PASSWORDS` JSON map for team PINs
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `SHEETS_WEBHOOK_URL`
- `SHEETS_WEBHOOK_SECRET` optional but recommended
- `SHEETS_REQUIRED=true|false`
- `TELEGRAM_SAFE_LIMIT` default `3500`

## Storage Files
All runtime data saved under `.appdata/`:
- `pending_submissions.json`
- `work_states.json`
- `idempotency_success.json`
- `team_locks.json`
- `root_tracking.json`

## Operational Notes
- If Telegram or Sheets fails, submission is kept in pending queue.
- Retry uses same idempotency key to avoid duplicates.
- If `SHEETS_REQUIRED=true`, success is only finalized when both Telegram and Sheets succeed.

## Google Sheets Webhook Template
- Use [scripts/google_apps_script_webhook.gs](C:\Users\denma\OneDrive\Desktop\깃허브 폴더들\생산리포트\Kupas team\Laporan Situasi\Laporan Giling\scripts\google_apps_script_webhook.gs) in Google Apps Script.
- Deploy as Web App and copy URL into `SHEETS_WEBHOOK_URL`.
- If using webhook secret, set same value in `SHEETS_WEBHOOK_SECRET` and script `WEBHOOK_SECRET`.

## Telegram Connectivity Check
- Run:
```powershell
py scripts/check_telegram.py
```
- Requires `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in environment or `.env` loaded via `run.ps1`.

## Deploy Order (GitHub + Streamlit + Sheets)
1. Push this folder to GitHub repository.
2. Deploy Google Apps Script webhook and copy the Web App URL.
3. In Streamlit Cloud:
   - Create app from the GitHub repo
   - Set main file to `app.py`
   - Add secrets in app settings using `.streamlit/secrets.toml.example` format
4. Confirm:
   - Telegram send/edit works
   - Sheets append/dedupe works
   - Retry pending works
