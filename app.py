import json
import os
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib import error, request

import streamlit as st
from zoneinfo import ZoneInfo


def read_setting(name: str, default: str = "") -> str:
    env_value = os.getenv(name, "")
    if env_value:
        return str(env_value)
    try:
        secret_value = st.secrets.get(name, "")
        if secret_value is None:
            return default
        value = str(secret_value)
        return value if value else default
    except Exception:
        return default


APP_TZ = ZoneInfo(read_setting("APP_TIMEZONE", "Asia/Jakarta"))
DATA_DIR = Path(".appdata")
PENDING_FILE = DATA_DIR / "pending_submissions.json"
STATE_FILE = DATA_DIR / "work_states.json"
IDEMP_LOG = DATA_DIR / "idempotency_success.json"
LOCK_FILE = DATA_DIR / "team_locks.json"
ROOT_TRACK_FILE = DATA_DIR / "root_tracking.json"

TELEGRAM_BOT_TOKEN = read_setting("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = read_setting("TELEGRAM_CHAT_ID", "").strip()
SHEETS_WEBHOOK_URL = read_setting("SHEETS_WEBHOOK_URL", "").strip()
SHEETS_WEBHOOK_SECRET = read_setting("SHEETS_WEBHOOK_SECRET", "").strip()
SHEETS_REQUIRED = read_setting("SHEETS_REQUIRED", "true").lower() == "true"
TELEGRAM_SAFE_LIMIT = int(read_setting("TELEGRAM_SAFE_LIMIT", "3500"))
TEAM_PASSWORDS_ERROR: Optional[str] = None


def load_team_passwords() -> Dict[str, str]:
    global TEAM_PASSWORDS_ERROR
    defaults = {
        "KUPAS-1": "abcd",
        "KUPAS-2": "1234",
        "KUPAS-3": "ab12",
    }
    raw = read_setting("TEAM_PASSWORDS", "").strip()
    if not raw:
        TEAM_PASSWORDS_ERROR = "TEAM_PASSWORDS belum diatur. Sementara pakai PIN default."
        return defaults
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict) and parsed:
            TEAM_PASSWORDS_ERROR = None
            return {str(k): str(v) for k, v in parsed.items()}
    except Exception:
        pass
    TEAM_PASSWORDS_ERROR = "TEAM_PASSWORDS tidak valid. Sementara pakai PIN default."
    return defaults


TEAM_PASSWORDS = load_team_passwords()
TEAM_LABELS = {
    "KUPAS-1": "Kupas team Erika",
    "KUPAS-2": "Kupas team Elok",
    "KUPAS-3": "Kupas Extra",
}


def ensure_storage() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    for file_path, default in [
        (PENDING_FILE, []),
        (STATE_FILE, {}),
        (IDEMP_LOG, {}),
        (LOCK_FILE, {}),
        (ROOT_TRACK_FILE, {}),
    ]:
        if not file_path.exists():
            file_path.write_text(json.dumps(default), encoding="utf-8")


def load_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def save_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def now_local() -> datetime:
    return datetime.now(APP_TZ)


def ts_str() -> str:
    return now_local().strftime("%Y-%m-%d %H:%M:%S")


def hhmm_now() -> str:
    return now_local().strftime("%H:%M")


def normalize_name(value: str) -> str:
    return " ".join(value.strip().split()).title()


def parse_optional_float(value: str) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip().replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def parse_optional_int(value: Any, default: int = 0) -> int:
    try:
        raw = str(value).strip()
        if not raw:
            return default
        return int(float(raw))
    except Exception:
        return default


def parse_hhmm_time(value: Any, default_hhmm: str) -> time:
    raw = str(value or "").strip()
    if raw:
        try:
            return datetime.strptime(raw, "%H:%M").time()
        except Exception:
            pass
    return datetime.strptime(default_hhmm, "%H:%M").time()


def parse_name_lines(value: str) -> List[str]:
    out: List[str] = []
    for line in str(value or "").splitlines():
        name = normalize_name(line)
        if name:
            out.append(name)
    return out


def informative_lines(pairs: List[Tuple[str, Any]]) -> List[str]:
    lines: List[str] = []
    for label, value in pairs:
        if value is None:
            continue
        text = str(value).strip()
        if not text or text in {"-", "0", "0.0"}:
            continue
        lines.append(f"- {label}: {text}")
    return lines


def chunk_sections(sections: List[str], safe_limit: int) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    current_len = 0

    def flush() -> None:
        nonlocal current, current_len
        if current:
            parts.append("\n".join(current).strip())
            current = []
            current_len = 0

    for block in sections:
        block = block.strip()
        if not block:
            continue
        block_len = len(block) + 2
        if current_len + block_len > safe_limit and current:
            flush()
        if len(block) > safe_limit:
            # Last resort split by lines to avoid mid-sentence slicing.
            lines = [ln for ln in block.split("\n") if ln.strip()]
            local: List[str] = []
            local_len = 0
            for ln in lines:
                ln_len = len(ln) + 1
                if local_len + ln_len > safe_limit and local:
                    parts.append("\n".join(local).strip())
                    local = []
                    local_len = 0
                local.append(ln)
                local_len += ln_len
            if local:
                parts.append("\n".join(local).strip())
            continue
        current.append(block)
        current_len += block_len
    flush()
    return parts


def http_post_json(url: str, payload: Dict[str, Any], timeout: int = 20) -> Tuple[bool, str, Dict[str, Any]]:
    if not url:
        return False, "Missing URL", {}
    try:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
            try:
                data = json.loads(text) if text else {}
            except Exception:
                data = {"raw": text}
            return 200 <= resp.status < 300, f"HTTP {resp.status}", data
    except error.HTTPError as exc:
        return False, f"HTTPError {exc.code}", {"error": exc.read().decode('utf-8', errors='ignore')}
    except Exception as exc:
        return False, str(exc), {}


def tg_api(method: str, payload: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    if not TELEGRAM_BOT_TOKEN:
        return False, "Missing TELEGRAM_BOT_TOKEN", {}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    ok, msg, data = http_post_json(url, payload)
    if isinstance(data, dict) and "ok" in data:
        return bool(data.get("ok")), msg, data
    return ok, msg, data


@dataclass
class SubmitResult:
    telegram_ok: bool
    sheets_ok: bool
    telegram_message_ids: List[int]
    telegram_error: str
    sheets_error: str


def render_header_block(payload: Dict[str, Any]) -> str:
    return "\n".join(
        [
            f"LAPORAN GILING ({payload['report_type_label']})",
            f"Team: {payload['team_id']} | Shift: {payload['shift']}",
            f"Pelapor: {normalize_name(payload['pelapor'])}",
            f"Tanggal kerja: {payload['work_date']}",
            f"Waktu sistem: {payload['system_timestamp']}",
        ]
    )


def render_non_steril_blocks(payload: Dict[str, Any]) -> List[str]:
    d = payload["details"]
    status_defrost = str(d.get("status_defrost", "")).strip() or "-"
    status_giling = str(d.get("status_giling", "")).strip() or "-"
    status_vacum = str(d.get("status_vacum", "")).strip() or "-"
    nama_petugas = d.get("nama_petugas_list", [])
    nama_petugas_txt = ", ".join([str(x) for x in nama_petugas if str(x).strip()]) or "-"
    return [
        "\n".join(
            [
                "1. PRODUK",
                f"- Jam kerja: {d.get('jam_kerja_mulai', '-')} - {d.get('jam_kerja_selesai', '-')}",
                f"- Produk: {d.get('produk', '-') or '-'}",
                f"- 1-1. Nama alat: {d.get('alat', '-') or '-'}",
                f"- 1-2. Jumlah isi barang dalam pillow: {d.get('isi_pillow_kg', '-') or '-'}",
                f"- 1-3. Nama petugas: {nama_petugas_txt}",
                f"- 1-4. Timer ada?: {d.get('timer_ada', '-') or '-'}",
                f"- Petugas vakum / PIC: {d.get('petugas_vacum', '-') or '-'}",
            ]
        ),
        "\n".join(
            [
                "2-1. STATUS DEFROST",
                status_defrost,
                f"-> Total barang beku diambil: {d.get('total_beku', '-') or '-'}",
                f"-> Total bb fresh dipakai: {d.get('total_fresh_kg', 0)} kg",
                f"-> Total bb dibuang: {d.get('total_buang_kg', 0)} kg",
                f"-> Total: {d.get('total_akhir_kg', 0)} kg (jenis barang 1 + jenis barang 2)",
            ]
        ),
        "\n".join(
            [
                "2-2. Tempat buang pillow defrost sudah siap dekat meja/rak?",
                f"-> {d.get('tempat_buang_siap', '-') or '-'}",
            ]
        ),
        "\n".join(
            [
                "3-1. STATUS GILING",
                status_giling,
                f"--> Total Giling: {d.get('total_giling', '-') or '-'} resep",
            ]
        ),
        "\n".join(
            [
                "3-2. STATUS VACUM",
                status_vacum,
                f"-> Total hasil vakum: {d.get('total_hasil_vakum', '-') or '-'} pack",
                f"-> Sudah dikirim semua?: {d.get('sudah_dikirim_semua', '-') or '-'}",
                f"-> Petugas cek: {d.get('nama_pic_cek', '-') or '-'}",
            ]
        ),
        "\n".join(
            [
                "4. TOTAL BARANG ADA MASALAH",
                d.get("masalah_total_barang", "").strip() or "- Tidak ada masalah barang",
            ]
        ),
        "\n".join(
            [
                "5. TOTAL BARANG DIKIRIM KE PACKING (ATAU PRESS)",
                f"-> {d.get('total_dikirim_packing', '-') or '-'}",
            ]
        ),
        "\n".join(["CATATAN", d.get("catatan", "-") or "-"]),
    ]


def render_steril_blocks(payload: Dict[str, Any]) -> List[str]:
    d = payload["details"]
    nama_petugas = d.get("nama_petugas_list", [])
    nama_petugas_txt = ", ".join([str(x) for x in nama_petugas if str(x).strip()]) or "-"
    petugas_lines = informative_lines(
        [
            ("Nama petugas", nama_petugas_txt),
            ("Petugas steril", normalize_name(d.get("petugas_steril", ""))),
            ("Timer ada", d.get("timer_ada", "")),
            ("Target steril", d.get("rencana_steril", "")),
        ]
    )
    detail_lines = informative_lines(
        [
            ("Jam kerja", f"{d.get('jam_kerja_mulai', '-')} - {d.get('jam_kerja_selesai', '-')}"),
            ("Produk", d.get("produk", "")),
            ("Nama alat", d.get("alat", "")),
            ("Jumlah isi untuk steril", d.get("isi_steril", "")),
            ("Total barang beku diambil", d.get("total_beku", "")),
            ("Total barang beku (kg)", d.get("total_beku_kg", "")),
            ("Total BB fresh dipakai", f"{d.get('total_fresh_kg', '')} kg"),
            ("Total BB dibuang", f"{d.get('total_buang_kg', '')} kg"),
            ("Total akhir", f"{d.get('total_akhir_kg', '')} kg"),
            ("Tempat buang pillow siap", d.get("tempat_buang_siap", "")),
            ("Total giling", d.get("total_giling", "")),
            ("Total produk steril", d.get("total_produk_steril", "")),
            ("CB siap", d.get("cb_siap", "")),
            ("CB dinyalakan", d.get("cb_nyala", "")),
            ("Produk diambil <=20 menit", d.get("ambil_20_menit", "")),
            ("Tidak ada sisa CB", d.get("tidak_ada_sisa_cb", "")),
        ]
    )
    return [
        "\n".join(["PETUGAS"] + (petugas_lines or ["- Tidak ada data petugas"])),
        "\n".join(["DETAIL KERJA"] + (detail_lines or ["- Tidak ada data detail"])),
        "\n".join(["CATATAN", d.get("catatan", "-") or "-"]),
    ]


def build_telegram_parts(payload: Dict[str, Any]) -> List[str]:
    header = render_header_block(payload)
    if payload["report_type"] == "non_steril":
        body_blocks = render_non_steril_blocks(payload)
    else:
        body_blocks = render_steril_blocks(payload)

    sections = [header] + body_blocks
    raw_parts = chunk_sections(sections, TELEGRAM_SAFE_LIMIT)
    if len(raw_parts) <= 1:
        return raw_parts

    titled_parts: List[str] = []
    for idx, part in enumerate(raw_parts, start=1):
        title = f"Lanjutan laporan mulai {hhmm_now()} (part {idx})"
        titled_parts.append(f"{title}\n{part}" if idx > 1 else part)
    return titled_parts


def send_telegram_edit_first(payload: Dict[str, Any]) -> Tuple[bool, List[int], str]:
    if not TELEGRAM_CHAT_ID:
        return False, [], "Missing TELEGRAM_CHAT_ID"
    parts = build_telegram_parts(payload)

    existing_ids = payload.get("existing_message_ids", [])
    result_ids: List[int] = []
    err_msgs: List[str] = []

    for idx, text in enumerate(parts):
        edited = False
        if idx < len(existing_ids):
            ok, _, data = tg_api(
                "editMessageText",
                {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "message_id": existing_ids[idx],
                    "text": text,
                },
            )
            if ok:
                result_ids.append(existing_ids[idx])
                edited = True
            else:
                err_msgs.append(f"edit part {idx+1} failed")

        if not edited:
            ok, _, data = tg_api(
                "sendMessage",
                {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": text,
                },
            )
            if not ok:
                desc = ""
                if isinstance(data, dict):
                    desc = str(data.get("description", ""))
                return False, result_ids, f"send part {idx+1} failed: {desc or 'unknown'}"
            msg_id = data.get("result", {}).get("message_id")
            if isinstance(msg_id, int):
                result_ids.append(msg_id)
    return True, result_ids, "; ".join(err_msgs)


def append_sheets(payload: Dict[str, Any]) -> Tuple[bool, str]:
    row_payload = {
        "idempotency_key": payload["idempotency_key"],
        "work_date": payload["work_date"],
        "system_timestamp": payload["system_timestamp"],
        "team_id": payload["team_id"],
        "shift": payload["shift"],
        "pelapor": payload["pelapor"],
        "report_type": payload["report_type"],
        "details": payload["details"],
    }
    if SHEETS_WEBHOOK_SECRET:
        row_payload["webhook_secret"] = SHEETS_WEBHOOK_SECRET
    ok, msg, data = http_post_json(SHEETS_WEBHOOK_URL, row_payload)
    if not ok:
        return False, str(data.get("error", msg))
    return True, ""


def submission_key(payload: Dict[str, Any]) -> str:
    return payload["idempotency_key"]


def dedupe_success_exists(key: str) -> bool:
    data = load_json(IDEMP_LOG, {})
    return key in data


def mark_success(key: str, telegram_ids: List[int], payload: Dict[str, Any]) -> None:
    data = load_json(IDEMP_LOG, {})
    data[key] = {
        "saved_at": ts_str(),
        "telegram_message_ids": telegram_ids,
        "team_id": payload.get("team_id", ""),
        "work_date": payload.get("work_date", ""),
        "report_type": payload.get("report_type", ""),
    }
    save_json(IDEMP_LOG, data)


def enqueue_pending(payload: Dict[str, Any], err: str) -> None:
    queue = load_json(PENDING_FILE, [])
    found = False
    for item in queue:
        if item.get("idempotency_key") == payload["idempotency_key"]:
            item["payload"] = payload
            item["last_error"] = err
            item["updated_at"] = ts_str()
            found = True
            break
    if not found:
        queue.append(
            {
                "idempotency_key": payload["idempotency_key"],
                "payload": payload,
                "last_error": err,
                "updated_at": ts_str(),
            }
        )
    save_json(PENDING_FILE, queue)


def remove_pending(key: str) -> None:
    queue = load_json(PENDING_FILE, [])
    queue = [x for x in queue if x.get("idempotency_key") != key]
    save_json(PENDING_FILE, queue)


def submit_payload(payload: Dict[str, Any]) -> SubmitResult:
    key = submission_key(payload)
    if dedupe_success_exists(key):
        saved = load_json(IDEMP_LOG, {}).get(key, {})
        return SubmitResult(
            telegram_ok=True,
            sheets_ok=True,
            telegram_message_ids=saved.get("telegram_message_ids", []),
            telegram_error="already_success",
            sheets_error="already_success",
        )

    telegram_ok, ids, tg_err = send_telegram_edit_first(payload)
    sheets_ok, sh_err = append_sheets(payload)

    if telegram_ok and (sheets_ok or not SHEETS_REQUIRED):
        mark_success(key, ids, payload)
        remove_pending(key)
        set_root_message_ids(payload.get("team_id", ""), payload.get("work_date", ""), payload.get("report_type", ""), ids)
    else:
        err = f"telegram={telegram_ok}:{tg_err} | sheets={sheets_ok}:{sh_err}"
        enqueue_pending(payload, err)
    return SubmitResult(
        telegram_ok=telegram_ok,
        sheets_ok=sheets_ok,
        telegram_message_ids=ids,
        telegram_error=tg_err,
        sheets_error=sh_err,
    )


def retry_pending() -> Tuple[int, int]:
    queue = load_json(PENDING_FILE, [])
    if not queue:
        return 0, 0

    success = 0
    total = len(queue)
    for item in list(queue):
        payload = item.get("payload", {})
        if not payload:
            continue
        result = submit_payload(payload)
        if result.telegram_ok and (result.sheets_ok or not SHEETS_REQUIRED):
            success += 1
    return success, total


def validate_common(form: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    if not form["team_id"].strip():
        errs.append("Team wajib diisi.")
    if not form["pelapor"].strip():
        errs.append("Nama pelapor wajib diisi.")
    if form["shift"] not in {"1", "2", "3"}:
        errs.append("Shift tidak valid.")
    return errs


def validate_non_steril(details: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    if not details["produk"].strip():
        errs.append("Produk wajib diisi.")
    if not details.get("alat", "").strip():
        errs.append("Nama alat wajib diisi.")
    if not details.get("nama_petugas_list", []):
        errs.append("1-3 Nama petugas wajib diisi (bisa lebih dari satu nama).")
    if not details.get("petugas_vacum", "").strip():
        errs.append("Petugas vakum wajib diisi. Jika tidak vakum, isi nama PIC yang bertanggung jawab.")
    if details.get("timer_ada", "") not in {"O", "X"}:
        errs.append("Timer ada? wajib pilih O atau X.")
    if details["total_fresh_kg"] < 0 or details["total_buang_kg"] < 0:
        errs.append("Nilai kilogram tidak boleh negatif.")
    if float(details.get("total_beku_kg", 0.0)) < 0:
        errs.append("Total barang beku (kg) tidak boleh negatif.")
    expected = float(details.get("total_beku_kg", 0.0)) + float(details.get("total_fresh_kg", 0.0)) - float(
        details.get("total_buang_kg", 0.0)
    )
    if abs(expected - float(details.get("total_akhir_kg", 0.0))) > 0.001:
        errs.append("Total akhir harus sama dengan (barang beku + fresh - dibuang).")
    if details.get("sudah_dikirim_semua", "") == "X" and not details.get("nama_pic_cek", "").strip():
        errs.append("Jika 'Sudah dikirim semua' = X, petugas cek wajib diisi.")
    if not details.get("total_dikirim_packing", "").strip():
        errs.append("Total barang dikirim ke packing/press wajib diisi untuk handover.")
    return errs


def validate_steril(details: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    if not details["rencana_steril"].strip():
        errs.append("Rencana jam steril wajib diisi.")
    if not details["produk"].strip():
        errs.append("Produk wajib diisi.")
    if not details.get("nama_petugas_list", []):
        errs.append("1-3 Nama petugas wajib diisi (bisa lebih dari satu nama).")
    if not details.get("petugas_steril", "").strip():
        errs.append("Untuk laporan steril, petugas steril wajib diisi.")
    if float(details.get("total_beku_kg", 0.0)) < 0:
        errs.append("Total barang beku (kg) tidak boleh negatif.")
    expected = float(details.get("total_beku_kg", 0.0)) + float(details.get("total_fresh_kg", 0.0)) - float(
        details.get("total_buang_kg", 0.0)
    )
    if abs(expected - float(details.get("total_akhir_kg", 0.0))) > 0.001:
        errs.append("Total akhir harus sama dengan (barang beku + fresh - dibuang).")
    return errs


def save_work_state(team_id: str, work_date: str, values: Dict[str, Any]) -> None:
    data = load_json(STATE_FILE, {})
    key = f"{team_id}::{work_date}"
    data[key] = {
        "updated_at": ts_str(),
        "values": values,
    }
    save_json(STATE_FILE, data)


def load_work_state(team_id: str, work_date: str) -> Dict[str, Any]:
    key = f"{team_id}::{work_date}"
    data = load_json(STATE_FILE, {})
    return data.get(key, {}).get("values", {})


def lock_scope(team_id: str, work_date: str) -> str:
    return f"{team_id.strip()}::{work_date}"


def read_lock(team_id: str, work_date: str) -> Dict[str, Any]:
    scope = lock_scope(team_id, work_date)
    data = load_json(LOCK_FILE, {})
    return data.get(scope, {})


def open_team_lock(team_id: str, work_date: str, owner: str) -> Tuple[bool, str, Dict[str, Any]]:
    if not owner.strip():
        return False, "Owner lock wajib diisi.", {}
    scope = lock_scope(team_id, work_date)
    data = load_json(LOCK_FILE, {})
    cur = data.get(scope)
    if cur and cur.get("owner") != owner:
        return False, "Sudah dipegang tim lain. Gunakan Take Over jika diperlukan.", cur
    if cur and cur.get("owner") == owner:
        return True, "Lock aktif untuk owner yang sama.", cur
    token = str(uuid.uuid4())
    new_lock = {
        "owner": owner,
        "token": token,
        "version": 1,
        "updated_at": ts_str(),
    }
    data[scope] = new_lock
    save_json(LOCK_FILE, data)
    return True, "Lock berhasil dibuka.", new_lock


def takeover_team_lock(team_id: str, work_date: str, owner: str) -> Tuple[bool, str, Dict[str, Any]]:
    if not owner.strip():
        return False, "Owner lock wajib diisi.", {}
    scope = lock_scope(team_id, work_date)
    data = load_json(LOCK_FILE, {})
    cur = data.get(scope, {})
    token = str(uuid.uuid4())
    version = int(cur.get("version", 0)) + 1
    new_lock = {
        "owner": owner,
        "token": token,
        "version": version,
        "updated_at": ts_str(),
    }
    data[scope] = new_lock
    save_json(LOCK_FILE, data)
    return True, "Take Over berhasil.", new_lock


def validate_lock_for_submit(team_id: str, work_date: str, owner: str, token: str, version: int) -> Tuple[bool, str]:
    cur = read_lock(team_id, work_date)
    if not cur:
        return False, "Tim belum dibuka. Tekan Buka Tim dulu."
    if cur.get("owner") == owner:
        return True, ""
    if cur.get("token") == token and int(cur.get("version", -1)) == int(version):
        return True, ""
    return False, "Konflik kunci terdeteksi. Gunakan Ambil Alih Tim lalu kirim ulang."


def latest_success_minutes_ago(team_id: str, work_date: str) -> Optional[int]:
    data = load_json(IDEMP_LOG, {})
    latest = None
    for _, rec in data.items():
        if rec.get("team_id") == team_id and rec.get("work_date") == work_date:
            saved = rec.get("saved_at")
            if not saved:
                continue
            try:
                dt = datetime.strptime(saved, "%Y-%m-%d %H:%M:%S").replace(tzinfo=APP_TZ)
            except Exception:
                continue
            if latest is None or dt > latest:
                latest = dt
    if latest is None:
        return None
    delta = now_local() - latest
    return int(delta.total_seconds() // 60)


def root_scope(team_id: str, work_date: str, report_type: str) -> str:
    return f"{team_id.strip()}::{work_date}::{report_type}"


def get_root_message_ids(team_id: str, work_date: str, report_type: str) -> List[int]:
    data = load_json(ROOT_TRACK_FILE, {})
    ids = data.get(root_scope(team_id, work_date, report_type), [])
    return [int(x) for x in ids if str(x).isdigit()]


def set_root_message_ids(team_id: str, work_date: str, report_type: str, message_ids: List[int]) -> None:
    data = load_json(ROOT_TRACK_FILE, {})
    data[root_scope(team_id, work_date, report_type)] = message_ids
    save_json(ROOT_TRACK_FILE, data)


def main() -> None:
    ensure_storage()
    st.set_page_config(page_title="Laporan Giling Kupas", layout="centered")
    if TEAM_PASSWORDS_ERROR:
        st.warning(TEAM_PASSWORDS_ERROR)
    st.title("B-1-3 Laporan Giling (Kupas)")
    st.caption("Durasi lapor: 1 kali / 30 menit | Telegram utama | Google Sheets backup wajib")

    if "lock_token" not in st.session_state:
        st.session_state["lock_token"] = ""
    if "lock_version" not in st.session_state:
        st.session_state["lock_version"] = 0
    if "lock_owner" not in st.session_state:
        st.session_state["lock_owner"] = ""
    if "active_idempotency_key" not in st.session_state:
        st.session_state["active_idempotency_key"] = str(uuid.uuid4())
    if "authenticated_scope" not in st.session_state:
        st.session_state["authenticated_scope"] = ""
    if "report_type_confirmed" not in st.session_state:
        st.session_state["report_type_confirmed"] = st.session_state.get("report_type", "non_steril")
    if "pending_report_type" not in st.session_state:
        st.session_state["pending_report_type"] = ""
    if "await_report_type_confirm" not in st.session_state:
        st.session_state["await_report_type_confirm"] = False
    if "defrost_rows_non" not in st.session_state:
        st.session_state["defrost_rows_non"] = 1

    st.subheader("Kontrol Tim")
    lc1, lc2, lc3, lc4 = st.columns(4)
    with lc1:
        team_choices = list(TEAM_PASSWORDS.keys())
        default_team = st.session_state.get("team_scope", team_choices[0] if team_choices else "")
        team_index = team_choices.index(default_team) if default_team in team_choices else 0
        team_scope = st.selectbox(
            "Tim laporan",
            options=team_choices,
            index=team_index,
            format_func=lambda x: TEAM_LABELS.get(x, x),
        )
    with lc2:
        work_date_scope = st.date_input("Tanggal kerja", value=now_local().date(), key="work_date_scope")
    with lc3:
        operator_scope = st.text_input("Pelapor", value=st.session_state.get("owner_scope", ""), placeholder="Nama pelapor")
    with lc4:
        team_pin = st.text_input("PIN Tim", type="password")

    st.session_state["team_scope"] = team_scope
    st.session_state["owner_scope"] = operator_scope

    scope = f"{work_date_scope}::{team_scope}"
    lock_now = read_lock(team_scope, str(work_date_scope))
    if lock_now:
        st.caption(f"Kunci aktif: {lock_now.get('owner', '-')} ({lock_now.get('updated_at', '-')})")
    else:
        st.caption("Belum ada kunci aktif untuk tim ini.")
    st.caption("Buka Tim: mulai laporan tim ini hari ini (PIN + kunci).")
    st.caption("Ambil Alih Tim: ambil alih saat tim terkunci operator lain.")

    b1, b2 = st.columns(2)
    with b1:
        if st.button("Buka Tim"):
            if not operator_scope.strip():
                st.error("Nama pelapor wajib diisi.")
            elif secrets.compare_digest(team_pin, TEAM_PASSWORDS.get(team_scope, "")):
                ok, msg, lock = open_team_lock(team_scope, str(work_date_scope), operator_scope.strip())
                if ok:
                    st.session_state["lock_token"] = lock.get("token", "")
                    st.session_state["lock_version"] = int(lock.get("version", 0))
                    st.session_state["lock_owner"] = operator_scope.strip()
                    st.session_state["authenticated_scope"] = scope
                    st.success(f"{team_scope} berhasil dibuka.")
                else:
                    st.error(msg)
            else:
                st.error("PIN Tim tidak valid.")
    with b2:
        if st.button("Ambil Alih Tim"):
            if not operator_scope.strip():
                st.error("Nama pelapor wajib diisi.")
            elif secrets.compare_digest(team_pin, TEAM_PASSWORDS.get(team_scope, "")):
                ok, msg, lock = takeover_team_lock(team_scope, str(work_date_scope), operator_scope.strip())
                if ok:
                    st.session_state["lock_token"] = lock.get("token", "")
                    st.session_state["lock_version"] = int(lock.get("version", 0))
                    st.session_state["lock_owner"] = operator_scope.strip()
                    st.session_state["authenticated_scope"] = scope
                    st.warning(msg)
                else:
                    st.error(msg)
            else:
                st.error("PIN Tim tidak valid untuk ambil alih.")
    if st.session_state.get("authenticated_scope") != scope:
        st.warning("Masukkan PIN lalu tekan 'Buka Tim' untuk mulai isi laporan.")
        st.stop()

    lock_now = read_lock(team_scope, str(work_date_scope))
    if lock_now:
        st.caption(
            f"Lock aktif | owner={lock_now.get('owner')} | version={lock_now.get('version')} | updated={lock_now.get('updated_at')}"
        )
    else:
        st.caption("Belum ada lock aktif untuk scope ini.")

    mins = latest_success_minutes_ago(team_scope, str(work_date_scope))
    if mins is not None and mins > 30:
        st.warning(f"Reminder: belum ada laporan sukses selama {mins} menit pada scope ini.")

    st.subheader("Jenis Laporan Giling")
    confirmed_type = st.session_state.get("report_type_confirmed", "non_steril")
    if confirmed_type == "non_steril":
        st.markdown(
            "Mode aktif: <span style='color:#b91c1c;font-weight:800'>NON-STERIL</span> "
            "(Barang <span style='color:#b91c1c;font-weight:800'>TIDAK</span> butuh steril)",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "Mode aktif: <span style='color:#166534;font-weight:800'>STERIL</span> "
            "(Barang <span style='color:#166534;font-weight:800'>BUTUH</span> steril)",
            unsafe_allow_html=True,
        )

    rt1, rt2 = st.columns(2)
    with rt1:
        if st.button("Pilih NON-STERIL (TIDAK butuh steril)", use_container_width=True):
            if confirmed_type != "non_steril":
                st.session_state["pending_report_type"] = "non_steril"
                st.session_state["await_report_type_confirm"] = True
    with rt2:
        if st.button("Pilih STERIL (BUTUH steril)", use_container_width=True):
            if confirmed_type != "steril_required":
                st.session_state["pending_report_type"] = "steril_required"
                st.session_state["await_report_type_confirm"] = True

    if st.session_state.get("await_report_type_confirm", False):
        pending = st.session_state.get("pending_report_type", "")
        pending_label = "NON-STERIL (TIDAK butuh steril)" if pending == "non_steril" else "STERIL (BUTUH steril)"
        st.warning(f"Konfirmasi ubah jenis laporan ke: {pending_label}")
        cf1, cf2 = st.columns(2)
        with cf1:
            if st.button("Ya, ubah jenis laporan", use_container_width=True):
                st.session_state["report_type_confirmed"] = pending
                st.session_state["report_type"] = pending
                st.session_state["loaded_details"] = {}
                st.session_state["await_report_type_confirm"] = False
                st.session_state["pending_report_type"] = ""
                st.rerun()
        with cf2:
            if st.button("Batal", use_container_width=True):
                st.session_state["await_report_type_confirm"] = False
                st.session_state["pending_report_type"] = ""
                st.rerun()

    # Dynamic controls are placed outside form to allow immediate rerender.
    if st.session_state.get("report_type_confirmed", "non_steril") == "non_steril":
        st.markdown("### Pengaturan Status Defrost")
        mode_defrost = st.radio(
            "Cara isi status defrost",
            options=["List baris", "Tulis manual"],
            horizontal=True,
            key="mode_defrost_non",
        )
        if mode_defrost == "List baris":
            d1, d2, d3 = st.columns([2, 2, 6])
            with d1:
                if st.button("+ Tambah baris", key="btn_add_defrost", use_container_width=True):
                    st.session_state["defrost_rows_non"] = min(20, int(st.session_state.get("defrost_rows_non", 1)) + 1)
                    st.rerun()
            with d2:
                if st.button("- Hapus baris", key="btn_del_defrost", use_container_width=True):
                    st.session_state["defrost_rows_non"] = max(1, int(st.session_state.get("defrost_rows_non", 1)) - 1)
                    st.rerun()
            with d3:
                st.caption(f"Jumlah baris defrost: {int(st.session_state.get('defrost_rows_non', 1))}")

    loaded_details = st.session_state.get("loaded_details", {})
    with st.form("giling_form", clear_on_submit=False):
        top1, top2, top3 = st.columns(3)
        with top1:
            team_id = st.text_input("Tim laporan", value=team_scope, disabled=True)
        with top2:
            pelapor = st.text_input("Pelapor", value=st.session_state.get("pelapor", operator_scope))
        with top3:
            default_shift = st.session_state.get("shift", "1")
            shift_options = ["1", "2", "3"]
            shift_index = shift_options.index(default_shift) if default_shift in shift_options else 0
            shift = st.selectbox("Shift", options=shift_options, index=shift_index)

        jam1, jam2 = st.columns(2)
        with jam1:
            jam_kerja_mulai_t = st.time_input(
                "Jam kerja mulai",
                value=parse_hhmm_time(loaded_details.get("jam_kerja_mulai", ""), "00:00"),
            )
        with jam2:
            jam_kerja_selesai_t = st.time_input(
                "Jam kerja selesai",
                value=parse_hhmm_time(loaded_details.get("jam_kerja_selesai", ""), "23:30"),
            )
        jam_kerja_mulai = f"{jam_kerja_mulai_t.hour:02d}:{jam_kerja_mulai_t.minute:02d}"
        jam_kerja_selesai = f"{jam_kerja_selesai_t.hour:02d}:{jam_kerja_selesai_t.minute:02d}"
        work_date = work_date_scope

        report_type = st.session_state.get("report_type_confirmed", "non_steril")

        st.markdown("### 1. Produk" if report_type == "non_steril" else "### Data Umum")
        produk = st.text_input("1. Produk" if report_type == "non_steril" else "Produk", value=loaded_details.get("produk", ""))
        alat = st.text_input("1-1. Nama alat" if report_type == "non_steril" else "Nama alat", value=loaded_details.get("alat", ""))
        nama_petugas_raw = st.text_area(
            "1-3. Nama Petugas (satu baris satu nama)",
            value=loaded_details.get("nama_petugas_raw", ""),
            placeholder="Linda\nLian",
        )
        st.caption("Tambah manual: 1 baris = 1 nama petugas.")
        nama_petugas_list = parse_name_lines(nama_petugas_raw)
        timer_ada = st.selectbox(
            "1-4. Timer ada ?" if report_type == "non_steril" else "Timer ada?",
            options=["O", "X"],
            index=0 if loaded_details.get("timer_ada", "O") == "O" else 1,
        )

        details: Dict[str, Any] = {}
        if report_type == "non_steril":
            st.markdown("### 1-2. Jumlah isi barang dalam pillow")
            isi_pillow_kg = st.text_input(
                "Jumlah isi barang dalam pillow (kg)",
                value=str(loaded_details.get("isi_pillow_kg", "")),
                placeholder="contoh: 1,635kg atau 1.635",
            )
            petugas_vacum = st.text_input(
                "Petugas vakum (wajib, jika tidak vakum isi nama PIC)",
                value=loaded_details.get("petugas_vacum", ""),
                placeholder="Nama petugas vakum / nama PIC",
            )
            st.markdown("### 2-1. Status defrost")
            mode_defrost = st.session_state.get("mode_defrost_non", "List baris")
            if mode_defrost == "List baris":
                row_count = int(st.session_state.get("defrost_rows_non", 2))
                defrost_lines: List[str] = []
                for idx in range(int(row_count)):
                    dc1, dc2, dc3, dc4 = st.columns([2, 3, 2, 3])
                    jam = dc1.text_input(f"Jam #{idx+1}", placeholder="12:55", key=f"def_jam_non_{idx}")
                    isi = dc2.text_input(f"Status #{idx+1}", placeholder="BB fresh", key=f"def_isi_non_{idx}")
                    kg = dc3.text_input(f"Kg #{idx+1}", placeholder="75", key=f"def_kg_non_{idx}")
                    cat = dc4.text_input(f"Catatan #{idx+1}", placeholder="sudah termasuk campuran", key=f"def_cat_non_{idx}")
                    if jam.strip() or isi.strip() or kg.strip():
                        status_part = isi.strip()
                        if kg.strip():
                            status_part = f"{status_part} = {kg.strip()}kg".strip()
                        defrost_lines.append(f"- {jam.strip()} {status_part}".strip())
                    if cat.strip():
                        defrost_lines.append(f"({cat.strip()})")
                status_defrost = "\n".join(defrost_lines).strip()
                st.caption("Preview status defrost")
                st.code(status_defrost or "-")
                extra_manual = st.text_area(
                    "Tambahan manual (opsional)",
                    value="",
                    key="def_extra_non",
                )
                if extra_manual.strip():
                    status_defrost = (status_defrost + "\n" + extra_manual.strip()).strip()
            else:
                status_defrost = st.text_area(
                    "Status defrost (Kalau sudah habis dipakai, tulis habis)",
                    value=loaded_details.get("status_defrost", ""),
                    placeholder="- 12:55 BB fresh = 75kg\n(sudah termasuk campuran)\n- 13:00 BB fresh = 75kg",
                )
            total_beku = st.text_input(
                "Total barang beku diambil (contoh: sim km 20 pack)",
                value=loaded_details.get("total_beku", ""),
            )
            total_beku_kg = st.number_input(
                "Total barang beku (kg, angka untuk validasi)",
                min_value=0.0,
                step=1.0,
                value=float(loaded_details.get("total_beku_kg", 0.0)),
            )
            total_fresh_kg = st.number_input(
                "Total bb fresh dipakai (kg)",
                min_value=0.0,
                step=1.0,
                value=float(loaded_details.get("total_fresh_kg", 0.0)),
            )
            total_buang_kg = st.number_input(
                "Total bb dibuang (kg)",
                min_value=0.0,
                step=1.0,
                value=float(loaded_details.get("total_buang_kg", 0.0)),
            )
            total_akhir_kg = st.number_input(
                "Total akhir (kg)",
                min_value=0.0,
                step=1.0,
                value=float(loaded_details.get("total_akhir_kg", 0.0)),
            )
            with st.expander("Jika total berubah vs laporan sebelumnya", expanded=False):
                total_change_reason = st.text_input(
                    "Alasan perubahan total",
                    value=loaded_details.get("total_change_reason", ""),
                    key="ns_total_change_reason",
                )
                tl_confirm_phrase = st.text_input(
                    "Konfirmasi TL (isi persis: SUDAH DIKONFIRMASI TL)",
                    value=loaded_details.get("tl_confirm_phrase", ""),
                    key="ns_tl_confirm_phrase",
                )
            st.markdown("### 2-2. Tempat buang pillow defrost")
            tempat_buang_siap = st.selectbox(
                "Tempat buang pillow siap dekat meja/rak?",
                options=["O", "X"],
                index=0 if loaded_details.get("tempat_buang_siap", "O") == "O" else 1,
            )
            st.markdown("### 3-1. Status Giling")
            status_giling = st.text_area(
                "Status giling",
                value=loaded_details.get("status_giling", ""),
                placeholder="- 11:30 mulai giling batch 0\n- 11:50 selesai giling batch 0",
            )
            total_giling = st.number_input(
                "Total Giling (resep)",
                min_value=0,
                step=1,
                value=parse_optional_int(loaded_details.get("total_giling", 0), 0),
            )
            st.markdown("### 3-2. Status vacum")
            status_vacum = st.text_area(
                "Status vacum",
                value=loaded_details.get("status_vacum", ""),
                placeholder="12:00 mulai vacum batch 1\n12:30 selesai vacum batch 1",
            )
            total_hasil_vakum = st.number_input(
                "Total hasil vakum (pack)",
                min_value=0,
                step=1,
                value=parse_optional_int(loaded_details.get("total_hasil_vakum", 0), 0),
            )
            sudah_dikirim_semua = st.selectbox(
                "Sudah dikirim semua?",
                options=["O", "X"],
                index=0 if loaded_details.get("sudah_dikirim_semua", "O") == "O" else 1,
            )
            nama_pic_cek = st.text_input("Petugas cek (jika belum dikirim semua)", value=loaded_details.get("nama_pic_cek", ""))
            st.markdown("### 4. Total barang ada masalah")
            masalah_total_barang = st.text_area(
                "Tulis masalah barang (contoh: basi, kemasan sobek, dll)",
                value=loaded_details.get("masalah_total_barang", ""),
                placeholder="- Basi 2kg\n- Kemasan/pojac sobek 10 pack",
            )
            st.markdown("### 5. Total barang dikirim ke packing (atau press)")
            total_dikirim_packing = st.text_input(
                "Total barang dikirim ke packing/press",
                value=loaded_details.get("total_dikirim_packing", ""),
                placeholder="contoh: 120 pack ke packing",
            )
            catatan = st.text_area("Catatan tambahan", value=loaded_details.get("catatan", ""))

            details = {
                "produk": produk,
                "alat": alat,
                "isi_pillow_kg": isi_pillow_kg,
                "nama_petugas_raw": nama_petugas_raw,
                "nama_petugas_list": nama_petugas_list,
                "petugas_vacum": petugas_vacum,
                "timer_ada": timer_ada,
                "jam_kerja_mulai": jam_kerja_mulai,
                "jam_kerja_selesai": jam_kerja_selesai,
                "status_defrost": status_defrost,
                "total_beku": total_beku,
                "total_beku_kg": total_beku_kg,
                "total_fresh_kg": total_fresh_kg,
                "total_buang_kg": total_buang_kg,
                "total_akhir_kg": total_akhir_kg,
                "tempat_buang_siap": tempat_buang_siap,
                "status_giling": status_giling,
                "total_giling": total_giling,
                "status_vacum": status_vacum,
                "total_hasil_vakum": total_hasil_vakum,
                "sudah_dikirim_semua": sudah_dikirim_semua,
                "nama_pic_cek": nama_pic_cek,
                "masalah_total_barang": masalah_total_barang,
                "total_dikirim_packing": total_dikirim_packing,
                "catatan": catatan,
                "total_change_reason": total_change_reason,
                "tl_confirm_phrase": tl_confirm_phrase,
            }
        else:
            st.markdown("### Form Steril-Required")
            petugas_steril = st.text_input(
                "Petugas steril (wajib)",
                value=loaded_details.get("petugas_steril", ""),
                placeholder="Nama petugas steril",
            )
            rencana_steril = st.text_input("Rencana jam steril berapa lama", value=loaded_details.get("rencana_steril", ""))
            isi_steril = st.text_input("Jumlah isi barang untuk steril", value=loaded_details.get("isi_steril", ""))
            total_beku = st.text_input("Total barang beku diambil", value=loaded_details.get("total_beku", ""))
            total_beku_kg = st.number_input(
                "Total barang beku (kg, angka untuk validasi)",
                min_value=0.0,
                step=1.0,
                value=float(loaded_details.get("total_beku_kg", 0.0)),
            )
            total_fresh_kg = st.number_input(
                "Total bb fresh dipakai (kg)",
                min_value=0.0,
                step=1.0,
                value=float(loaded_details.get("total_fresh_kg", 0.0)),
            )
            total_buang_kg = st.number_input(
                "Total bb dibuang (kg)",
                min_value=0.0,
                step=1.0,
                value=float(loaded_details.get("total_buang_kg", 0.0)),
            )
            total_akhir_kg = st.number_input(
                "Total akhir (kg)",
                min_value=0.0,
                step=1.0,
                value=float(loaded_details.get("total_akhir_kg", 0.0)),
            )
            with st.expander("Jika total berubah vs laporan sebelumnya", expanded=False):
                total_change_reason = st.text_input(
                    "Alasan perubahan total",
                    value=loaded_details.get("total_change_reason", ""),
                    key="st_total_change_reason",
                )
                tl_confirm_phrase = st.text_input(
                    "Konfirmasi TL (isi persis: SUDAH DIKONFIRMASI TL)",
                    value=loaded_details.get("tl_confirm_phrase", ""),
                    key="st_tl_confirm_phrase",
                )
            tempat_buang_siap = st.selectbox(
                "Tempat buang pillow siap dekat meja/rak?",
                options=["O", "X"],
                index=0 if loaded_details.get("tempat_buang_siap", "O") == "O" else 1,
            )
            total_giling = st.text_input("Total giling", value=loaded_details.get("total_giling", ""))
            total_produk_steril = st.text_input("Total produk steril", value=loaded_details.get("total_produk_steril", ""))
            cb_siap = st.selectbox(
                "CB sudah dibersihkan dan isi air?",
                options=["O", "X"],
                index=0 if loaded_details.get("cb_siap", "O") == "O" else 1,
            )
            cb_nyala = st.selectbox(
                "CB sudah dinyalakan?",
                options=["O", "X"],
                index=0 if loaded_details.get("cb_nyala", "O") == "O" else 1,
            )
            ambil_20_menit = st.selectbox(
                "Produk diambil packing <=20 menit?",
                options=["O", "X"],
                index=0 if loaded_details.get("ambil_20_menit", "O") == "O" else 1,
            )
            tidak_ada_sisa_cb = st.selectbox(
                "Tidak ada sisa barang di CB?",
                options=["O", "X"],
                index=0 if loaded_details.get("tidak_ada_sisa_cb", "O") == "O" else 1,
            )
            catatan = st.text_area("Catatan tambahan", value=loaded_details.get("catatan", ""))

            details = {
                "produk": produk,
                "alat": alat,
                "rencana_steril": rencana_steril,
                "nama_petugas_raw": nama_petugas_raw,
                "nama_petugas_list": nama_petugas_list,
                "petugas_steril": petugas_steril,
                "timer_ada": timer_ada,
                "jam_kerja_mulai": jam_kerja_mulai,
                "jam_kerja_selesai": jam_kerja_selesai,
                "isi_steril": isi_steril,
                "total_beku": total_beku,
                "total_beku_kg": total_beku_kg,
                "total_fresh_kg": total_fresh_kg,
                "total_buang_kg": total_buang_kg,
                "total_akhir_kg": total_akhir_kg,
                "tempat_buang_siap": tempat_buang_siap,
                "total_giling": total_giling,
                "total_produk_steril": total_produk_steril,
                "cb_siap": cb_siap,
                "cb_nyala": cb_nyala,
                "ambil_20_menit": ambil_20_menit,
                "tidak_ada_sisa_cb": tidak_ada_sisa_cb,
                "catatan": catatan,
                "total_change_reason": total_change_reason,
                "tl_confirm_phrase": tl_confirm_phrase,
            }

        existing_message_ids_raw = st.text_input(
            "Existing Telegram message IDs (optional, comma separated for edit-first)",
            value="",
        )
        submitted = st.form_submit_button("Kirim Laporan")

    prev_state_snapshot = load_work_state(team_id.strip(), str(work_date))
    # Persist current working context after form render.
    save_work_state(
        team_id.strip() or "unknown",
        str(work_date),
        {
            "team_id": team_id,
            "shift": shift,
            "pelapor": pelapor,
            "report_type": report_type,
            "details": details,
        },
    )

    if submitted:
        common_form = {"team_id": team_id, "pelapor": pelapor, "shift": shift}
        errs = validate_common(common_form)
        lock_ok, lock_err = validate_lock_for_submit(
            team_id.strip(),
            str(work_date),
            st.session_state.get("lock_owner", ""),
            st.session_state.get("lock_token", ""),
            int(st.session_state.get("lock_version", 0)),
        )
        if not lock_ok:
            errs.append(lock_err)
        if report_type == "non_steril":
            errs.extend(validate_non_steril(details))
        else:
            errs.extend(validate_steril(details))

        prev_total = parse_optional_float(prev_state_snapshot.get("details", {}).get("total_akhir_kg")) if prev_state_snapshot else None
        cur_total = parse_optional_float(details.get("total_akhir_kg"))
        if prev_total is not None and cur_total is not None and abs(prev_total - cur_total) > 0.001:
            if not str(details.get("total_change_reason", "")).strip():
                errs.append("Total berubah dari laporan sebelumnya. Isi alasan perubahan.")
            if str(details.get("tl_confirm_phrase", "")).strip().upper() != "SUDAH DIKONFIRMASI TL":
                errs.append("Total berubah. Isi konfirmasi TL persis: SUDAH DIKONFIRMASI TL")

        if errs:
            for e in errs:
                st.error(e)
            return

        existing_ids = []
        if existing_message_ids_raw.strip():
            for token in existing_message_ids_raw.split(","):
                token = token.strip()
                if token.isdigit():
                    existing_ids.append(int(token))
        if not existing_ids:
            existing_ids = get_root_message_ids(team_id.strip(), str(work_date), report_type)

        key = st.session_state["active_idempotency_key"]
        payload = {
            "idempotency_key": key,
            "system_timestamp": ts_str(),
            "timezone": str(APP_TZ),
            "work_date": str(work_date),
            "team_id": team_id.strip(),
            "shift": shift,
            "pelapor": pelapor.strip(),
            "report_type": report_type,
            "report_type_label": "Barang tidak butuh steril" if report_type == "non_steril" else "Barang butuh steril",
            "details": details,
            "existing_message_ids": existing_ids,
            "lock_token": st.session_state.get("lock_token", ""),
            "lock_version": int(st.session_state.get("lock_version", 0)),
        }

        result = submit_payload(payload)
        if result.telegram_ok:
            st.success("Telegram: berhasil terkirim.")
            if result.telegram_message_ids:
                st.caption(f"Message IDs: {', '.join([str(x) for x in result.telegram_message_ids])}")
                set_root_message_ids(team_id.strip(), str(work_date), report_type, result.telegram_message_ids)
        else:
            st.error(f"Telegram gagal: {result.telegram_error}. Tersimpan sebagai pending untuk retry.")

        if result.sheets_ok:
            st.success("Google Sheets backup: berhasil append.")
        else:
            st.warning(f"Google Sheets backup gagal: {result.sheets_error}. Retry tersedia di panel status.")

        if not result.telegram_ok or (not result.sheets_ok and SHEETS_REQUIRED):
            st.info("Submission disimpan di queue pending. Gunakan tombol Retry Pending.")
        else:
            st.session_state["active_idempotency_key"] = str(uuid.uuid4())


if __name__ == "__main__":
    main()
