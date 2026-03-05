/**
 * Google Apps Script Webhook for Laporan Giling backup.
 *
 * Deploy:
 * 1) Create new Apps Script project
 * 2) Paste this file
 * 3) Set SHEET_NAME and (optional) WEBHOOK_SECRET
 * 4) Deploy as Web App (Anyone with link)
 * 5) Put Web App URL into SHEETS_WEBHOOK_URL
 */

const SHEET_NAME = 'LaporanGiling';
const WEBHOOK_SECRET = ''; // optional, set same value as SHEETS_WEBHOOK_SECRET in app env

function doPost(e) {
  try {
    const body = e && e.postData && e.postData.contents ? e.postData.contents : '{}';
    const payload = JSON.parse(body);

    if (WEBHOOK_SECRET) {
      const incoming = String(payload.webhook_secret || '');
      if (incoming !== WEBHOOK_SECRET) {
        return jsonOut({ ok: false, error: 'unauthorized' }, 401);
      }
    }

    const required = [
      'idempotency_key',
      'work_date',
      'system_timestamp',
      'team_id',
      'shift',
      'pelapor',
      'report_type'
    ];
    for (var i = 0; i < required.length; i++) {
      if (!payload[required[i]]) {
        return jsonOut({ ok: false, error: 'missing_' + required[i] }, 400);
      }
    }

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sh = ss.getSheetByName(SHEET_NAME);
    if (!sh) {
      sh = ss.insertSheet(SHEET_NAME);
    }

    ensureHeader(sh);

    // Idempotency dedupe: skip append if key already exists in column A.
    const key = String(payload.idempotency_key);
    const found = findIdempotency(sh, key);
    if (found > 0) {
      return jsonOut({ ok: true, deduped: true, row: found }, 200);
    }

    const detailsJson = JSON.stringify(payload.details || {});
    const row = [
      key,
      payload.work_date,
      payload.system_timestamp,
      payload.team_id,
      payload.shift,
      payload.pelapor,
      payload.report_type,
      detailsJson
    ];
    sh.appendRow(row);

    return jsonOut({ ok: true, deduped: false }, 200);
  } catch (err) {
    return jsonOut({ ok: false, error: String(err) }, 500);
  }
}

function ensureHeader(sh) {
  const hasHeader = sh.getLastRow() >= 1;
  if (hasHeader) return;
  sh.appendRow([
    'idempotency_key',
    'work_date',
    'system_timestamp',
    'team_id',
    'shift',
    'pelapor',
    'report_type',
    'details_json'
  ]);
}

function findIdempotency(sh, key) {
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return -1;
  const range = sh.getRange(2, 1, lastRow - 1, 1);
  const values = range.getValues();
  for (var i = 0; i < values.length; i++) {
    if (String(values[i][0]) === key) {
      return i + 2;
    }
  }
  return -1;
}

function jsonOut(obj, code) {
  const out = ContentService.createTextOutput(JSON.stringify(obj));
  out.setMimeType(ContentService.MimeType.JSON);
  return out;
}
