import json
import os
import secrets
import threading
import uuid
import ast
import mimetypes
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta
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
EVIDENCE_DIR = DATA_DIR / "evidence"
SECTION_CHECKPOINT_FILE = DATA_DIR / "section_checkpoints.json"

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
    EVIDENCE_DIR.mkdir(exist_ok=True)
    for file_path, default in [
        (PENDING_FILE, []),
        (STATE_FILE, {}),
        (IDEMP_LOG, {}),
        (LOCK_FILE, {}),
        (ROOT_TRACK_FILE, {}),
        (SECTION_CHECKPOINT_FILE, {}),
    ]:
        if not file_path.exists():
            file_path.write_text(json.dumps(default), encoding="utf-8")


def load_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


_SAVE_LOCK = threading.RLock()

def save_json(path: Path, payload: Any) -> None:
    with _SAVE_LOCK:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, path)


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


def guess_image_suffix(file_name: str, mime_type: str = "") -> str:
    suffix = Path(str(file_name or "")).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        return suffix
    mime = str(mime_type or "").lower()
    if "png" in mime:
        return ".png"
    if "webp" in mime:
        return ".webp"
    return ".jpg"


def save_uploaded_image_to_evidence(
    uploaded_file: Any,
    *,
    team_id: str,
    work_date: Any,
    prefix: str,
    sig_key: str,
    path_key: str,
    name_key: str,
) -> None:
    if uploaded_file is None:
        return
    upload_sig = (
        f"{getattr(uploaded_file, 'name', 'camera')}:"
        f"{getattr(uploaded_file, 'size', '')}:"
        f"{getattr(uploaded_file, 'type', '')}"
    )
    if st.session_state.get(sig_key, "") == upload_sig:
        return
    suffix = guess_image_suffix(getattr(uploaded_file, "name", ""), getattr(uploaded_file, "type", ""))
    safe_team = "".join(ch for ch in str(team_id) if ch.isalnum() or ch in {"-", "_"}).strip() or "team"
    file_name = f"{prefix}_{safe_team}_{str(work_date).replace('-', '')}_{datetime.now(APP_TZ).strftime('%H%M%S')}{suffix}"
    target_path = EVIDENCE_DIR / file_name
    target_path.write_bytes(uploaded_file.getvalue())
    st.session_state[path_key] = str(target_path)
    readable_name = str(getattr(uploaded_file, "name", "")).strip() or file_name
    st.session_state[name_key] = readable_name
    st.session_state[sig_key] = upload_sig


def eval_simple_math(expression: str) -> Tuple[Optional[float], str]:
    raw = str(expression or "").strip().replace(",", ".")
    if not raw:
        return None, ""
    if len(raw) > 120:
        return None, "Rumus terlalu panjang (maks 120 karakter)."
    try:
        node = ast.parse(raw, mode="eval")
    except Exception:
        return None, "Format rumus tidak valid."

    allowed_bin_ops = (ast.Add, ast.Sub, ast.Mult, ast.Div)
    allowed_unary_ops = (ast.UAdd, ast.USub)

    def _eval(n: ast.AST) -> float:
        if isinstance(n, ast.Expression):
            return _eval(n.body)
        if isinstance(n, ast.Constant) and isinstance(n.value, (int, float)):
            return float(n.value)
        if isinstance(n, ast.Num):  # py<3.8 compatibility
            return float(n.n)
        if isinstance(n, ast.BinOp) and isinstance(n.op, allowed_bin_ops):
            left = _eval(n.left)
            right = _eval(n.right)
            if isinstance(n.op, ast.Add):
                return left + right
            if isinstance(n.op, ast.Sub):
                return left - right
            if isinstance(n.op, ast.Mult):
                return left * right
            if right == 0:
                raise ZeroDivisionError
            return left / right
        if isinstance(n, ast.UnaryOp) and isinstance(n.op, allowed_unary_ops):
            val = _eval(n.operand)
            return val if isinstance(n.op, ast.UAdd) else -val
        raise ValueError("unsupported")

    try:
        value = _eval(node)
    except ZeroDivisionError:
        return None, "Tidak bisa dibagi 0."
    except Exception:
        return None, "Rumus hanya boleh angka dan operator + - * / ( )."
    return value, ""


def format_float_compact(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def parse_hhmm_time(value: Any, default_hhmm: str) -> time:
    raw = str(value or "").strip()
    if raw:
        normalized = normalize_hhmm_loose(raw)
        if normalized:
            raw = normalized
        try:
            return datetime.strptime(raw, "%H:%M").time()
        except Exception:
            pass
    return datetime.strptime(default_hhmm, "%H:%M").time()


def normalize_hhmm_loose(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    raw = raw.replace(".", ":")
    if ":" in raw:
        parts = raw.split(":")
        if len(parts) != 2:
            return ""
        hh_raw, mm_raw = parts[0].strip(), parts[1].strip()
        if not hh_raw.isdigit() or not mm_raw.isdigit():
            return ""
        hh = int(hh_raw)
        mm = int(mm_raw)
    else:
        digits = "".join(ch for ch in raw if ch.isdigit())
        if not digits:
            return ""
        if len(digits) <= 2:
            hh = int(digits)
            mm = 0
        elif len(digits) == 3:
            hh = int(digits[:1])
            mm = int(digits[1:])
        elif len(digits) == 4:
            hh = int(digits[:2])
            mm = int(digits[2:])
        else:
            return ""
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return ""
    return f"{hh:02d}:{mm:02d}"


def is_valid_hhmm(value: Any) -> bool:
    return bool(normalize_hhmm_loose(value))


def hhmm_plus_minutes(value: Any, minutes: int) -> str:
    raw = normalize_hhmm_loose(value)
    if not raw:
        return ""
    try:
        base = datetime.strptime(raw, "%H:%M")
        out = base + timedelta(minutes=int(minutes))
        return out.strftime("%H:%M")
    except Exception:
        return ""


def minutes_diff_hhmm(start_hhmm: Any, end_hhmm: Any) -> Optional[int]:
    start_raw = normalize_hhmm_loose(start_hhmm)
    end_raw = normalize_hhmm_loose(end_hhmm)
    if not start_raw or not end_raw:
        return None
    try:
        start_dt = datetime.strptime(start_raw, "%H:%M")
        end_dt = datetime.strptime(end_raw, "%H:%M")
        if end_dt < start_dt:
            end_dt += timedelta(days=1)
        return int((end_dt - start_dt).total_seconds() // 60)
    except Exception:
        return None


def parse_name_lines(value: str) -> List[str]:
    out: List[str] = []
    for line in str(value or "").splitlines():
        name = normalize_name(line)
        if name:
            out.append(name)
    return out


def ensure_row_count_from_session(
    count_key: str,
    field_prefixes: List[str],
    min_rows: int = 1,
    max_rows: int = 20,
) -> int:
    requested = int(st.session_state.get(count_key, min_rows))
    detected = 0
    for idx in range(max_rows):
        has_value = False
        for prefix in field_prefixes:
            val = str(st.session_state.get(f"{prefix}{idx}", "")).strip()
            if val:
                has_value = True
                break
        if has_value:
            detected = idx + 1
    final_rows = max(min_rows, requested, detected)
    final_rows = min(max_rows, final_rows)
    st.session_state[count_key] = final_rows
    return final_rows


def drop_last_row_from_session(
    count_key: str,
    field_prefixes: List[str],
    min_rows: int = 1,
) -> None:
    current = int(st.session_state.get(count_key, min_rows))
    if current <= min_rows:
        st.session_state[count_key] = min_rows
        return
    last_idx = current - 1
    for prefix in field_prefixes:
        key_name = f"{prefix}{last_idx}"
        if key_name in st.session_state:
            st.session_state[key_name] = ""
    st.session_state[count_key] = current - 1


def normalize_giling_status_input(
    raw_status: str,
    next_batch: int,
    open_batch: Optional[int],
) -> Tuple[str, int, Optional[int]]:
    token = str(raw_status or "").strip()
    if not token:
        return "", next_batch, open_batch
    normalized = " ".join(token.lower().split())
    start_alias = {"1", "mulai", "mulai giling", "mulai giling batch"}
    finish_alias = {"2", "selesai", "selesai giling", "selesai giling batch"}
    next_batch = max(1, int(next_batch or 1))

    if normalized in start_alias:
        if open_batch is None:
            batch_num = next_batch
        else:
            batch_num = max(next_batch, open_batch + 1)
        return f"mulai giling batch {batch_num}", batch_num, batch_num

    if normalized in finish_alias:
        batch_num = open_batch if open_batch is not None else next_batch
        return f"selesai giling batch {batch_num}", batch_num + 1, None

    return token, next_batch, open_batch


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


def inject_compact_ui_theme() -> None:
    st.markdown(
        """
<style>
[data-testid="stAppViewContainer"] {
  background: #eef6fb;
}
[data-testid="stHeader"] {
  background: transparent;
}
[data-testid="block-container"] {
  max-width: 860px;
  padding-top: 1.2rem;
  padding-bottom: 2.5rem;
}
div[data-testid="stVerticalBlockBorderWrapper"] {
  background: #ffffff;
  border: 1px solid #c8dced;
  border-radius: 14px;
}
h1, h2, h3 {
  color: #17324d;
}
div[data-testid="stTextInput"] input,
div[data-testid="stTextArea"] textarea {
  background: #ffffff;
  border: 1px solid #bed5e8;
  border-radius: 10px;
  overflow-wrap: anywhere;
}
div[data-baseweb="select"] > div {
  background: #ffffff;
  border: 1px solid #bed5e8;
  border-radius: 10px;
}
div[data-baseweb="select"] span,
div[data-baseweb="select"] div {
  overflow-wrap: anywhere !important;
  word-break: break-word !important;
}
div.stButton > button {
  background: #1ea7e1;
  color: #ffffff;
  border: none;
  border-radius: 10px;
  font-weight: 700;
  white-space: normal;
  overflow-wrap: anywhere;
  word-break: break-word;
  padding: 0.42rem 0.8rem;
  height: auto;
  min-height: 2.4rem;
}
div.stButton > button:hover {
  background: #0f95d4;
}
[data-testid="stCodeBlock"] pre,
[data-testid="stCode"] pre {
  white-space: pre-wrap !important;
  overflow-wrap: anywhere !important;
  word-break: break-word !important;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def chunk_sections(sections: List[str], safe_limit: int) -> List[str]:
    parts: List[str] = []
    current: List[str] = []
    current_len = 0

    def flush() -> None:
        nonlocal current, current_len
        if current:
            parts.append("\n\n".join(current).strip())
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


def http_post_multipart(
    url: str,
    fields: Dict[str, str],
    file_field: str,
    file_path: Path,
    timeout: int = 30,
) -> Tuple[bool, str, Dict[str, Any]]:
    if not url:
        return False, "Missing URL", {}
    if not file_path.exists():
        return False, "Missing file", {}
    boundary = f"----CodexBoundary{uuid.uuid4().hex}"
    body = bytearray()

    for k, v in fields.items():
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{k}"\r\n\r\n'.encode("utf-8"))
        body.extend(str(v).encode("utf-8"))
        body.extend(b"\r\n")

    mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    body.extend(f"--{boundary}\r\n".encode("utf-8"))
    body.extend(
        (
            f'Content-Disposition: form-data; name="{file_field}"; filename="{file_path.name}"\r\n'
            f"Content-Type: {mime_type}\r\n\r\n"
        ).encode("utf-8")
    )
    body.extend(file_path.read_bytes())
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))

    try:
        req = request.Request(
            url=url,
            data=bytes(body),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
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
        return False, f"HTTPError {exc.code}", {"error": exc.read().decode("utf-8", errors="ignore")}
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


def tg_send_photo(photo_path: str, caption: str = "") -> Tuple[bool, str, Dict[str, Any]]:
    if not TELEGRAM_BOT_TOKEN:
        return False, "Missing TELEGRAM_BOT_TOKEN", {}
    if not TELEGRAM_CHAT_ID:
        return False, "Missing TELEGRAM_CHAT_ID", {}
    path = Path(photo_path)
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    fields = {"chat_id": TELEGRAM_CHAT_ID}
    cap = str(caption or "").strip()
    if cap:
        fields["caption"] = cap[:1000]
    ok, msg, data = http_post_multipart(url, fields=fields, file_field="photo", file_path=path)
    if isinstance(data, dict) and "ok" in data:
        return bool(data.get("ok")), msg, data
    return ok, msg, data


def tg_send_update_reply(message_id: int, text: str = "Laporan sudah diperbarui.") -> Tuple[bool, str, Dict[str, Any]]:
    if not TELEGRAM_CHAT_ID:
        return False, "Missing TELEGRAM_CHAT_ID", {}
    ok, msg, data = tg_api(
        "sendMessage",
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "reply_to_message_id": int(message_id),
            "allow_sending_without_reply": True,
        },
    )
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


def _v(value: Any, default: str = "-") -> str:
    s = str(value or "").strip()
    return s if s else default


def _is_placeholder(value: Any) -> bool:
    s = str(value or "").strip().lower()
    return s in {"", "-", "x", "0", "0.0", "pilih"}


def _fmt_date_short(date_text: Any) -> str:
    raw = str(date_text or "").strip()
    if not raw:
        return "-"
    try:
        return datetime.strptime(raw, "%Y-%m-%d").strftime("%d-%m-%y")
    except Exception:
        return raw


def _fmt_jam(text: Any) -> str:
    raw = str(text or "").strip()
    norm = normalize_hhmm_loose(raw)
    return norm if norm else raw


def _clean_text_lines(text: Any) -> List[str]:
    lines: List[str] = []
    for raw in str(text or "").splitlines():
        line = str(raw).strip()
        if not line:
            continue
        line = re.sub(r"^\s*-\s*\[\d+\]\s*", "- ", line)
        line = re.sub(r"^-\s*(\d{3,4})(?=\s)", lambda m: f"- {_fmt_jam(m.group(1))}", line)
        line = re.sub(r"^(\d{3,4})(?=\s)", lambda m: _fmt_jam(m.group(1)), line)
        if line in {"-", "- -"}:
            continue
        lines.append(line)
    return lines


def _normalize_unit_value(value: Any, unit: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    cleaned = re.sub(rf"\s*{re.escape(unit)}\s*$", "", raw, flags=re.IGNORECASE).strip()
    if not cleaned:
        return "-"
    return f"{cleaned} {unit}"


def _prefer_auto_total(manual: Any, auto: Any) -> str:
    manual_s = str(manual or "").strip()
    auto_s = str(auto or "").strip()
    if _is_placeholder(manual_s) and not _is_placeholder(auto_s):
        return auto_s
    return manual_s or auto_s or "-"


def _defrost_lines(details: Dict[str, Any]) -> List[str]:
    rows = details.get("defrost_rows", [])
    out: List[str] = []
    if isinstance(rows, list):
        for row in rows:
            jam = _fmt_jam(row.get("jam", ""))
            status = str(row.get("status", "")).strip()
            pack = str(row.get("pack", "")).strip()
            cat = str(row.get("catatan", "")).strip()
            if jam or status or pack:
                line = f"- {jam} {status}".strip()
                if pack:
                    line = f"{line} = {pack}pack"
                out.append(line)
            if cat and not _is_placeholder(cat):
                out.append(f"({cat})")
    return out or _clean_text_lines(details.get("status_defrost", "")) or ["-"]


def _tempat_buang_lines(details: Dict[str, Any]) -> List[str]:
    rows = details.get("tempat_buang_rows", [])
    if isinstance(rows, list) and rows:
        latest_status = ""
        latest_jam = ""
        logs: List[str] = []
        for row in rows:
            jam = _fmt_jam(row.get("jam", ""))
            status = str(row.get("status", "")).strip()
            cat = str(row.get("catatan", "")).strip()
            if status in {"O", "X"}:
                latest_status = status
                latest_jam = jam
            if jam or status or (cat and not _is_placeholder(cat)):
                line = f"- {jam} | {status or '-'}".strip()
                if cat and not _is_placeholder(cat):
                    line += f" | {cat}"
                logs.append(line)
        head = f"-> {latest_status or '-'}"
        if latest_jam:
            head += f" ({latest_jam})"
        if len(logs) > 1:
            return [head, "-> Log cek:", *logs]
        return [head]
    status = _v(details.get("tempat_buang_siap"))
    jam = _fmt_jam(details.get("tempat_buang_check_time", ""))
    return [f"-> {status}{f' ({jam})' if jam else ''}"]


def render_header_block(payload: Dict[str, Any]) -> str:
    pelapor = normalize_name(payload.get("pelapor", "")) or "-"
    work_date_short = _fmt_date_short(payload.get("work_date", ""))
    if payload.get("report_type") == "non_steril":
        title = "Laporan Giling - Barang yang tidak butuh steril"
    else:
        title = "Laporan Giling / Steril - Barang yang butuh steril"
    return "\n".join(
        [
            title,
            f"({work_date_short} / {pelapor})",
            "Durasi Laporan : 1 kali / 30 menit",
            f"Team: {payload.get('team_id', '-')} | Shift: {payload.get('shift', '-')}",
            f"Waktu sistem: {payload.get('system_timestamp', '-')}",
        ]
    )


def render_non_steril_blocks(payload: Dict[str, Any]) -> List[str]:
    d = payload["details"]
    nama_petugas = d.get("nama_petugas_list", [])
    nama_petugas_txt = ", ".join([str(x).strip() for x in nama_petugas if str(x).strip()]) or "-"

    giling_lines = _clean_text_lines(d.get("status_giling", ""))
    vacum_lines = _clean_text_lines(d.get("status_vacum", ""))
    defrost_lines = _defrost_lines(d)
    tempat_buang = _tempat_buang_lines(d)

    delay_rows = d.get("giling_delay_rows", [])
    delay_lines: List[str] = []
    if isinstance(delay_rows, list):
        for row in delay_rows:
            jam = _fmt_jam(row.get("jam", ""))
            status = str(row.get("status", "")).strip()
            detail = str(row.get("detail", "")).strip()
            if status == "O":
                delay_lines.append(f"- {jam or '-'} | Delay ada | {detail or '-'}")
    if not delay_lines and str(d.get("giling_delay_lama", "")).strip() == "O":
        detail = str(d.get("giling_delay_detail", "")).strip()
        if detail:
            delay_lines = _clean_text_lines(detail)

    vac_ops_rows = d.get("vacum_ops_rows", [])
    vac_ops_lines: List[str] = []
    if isinstance(vac_ops_rows, list):
        for row in vac_ops_rows:
            start = _fmt_jam(row.get("stop_start", row.get("jam", "")))
            end = _fmt_jam(row.get("stop_end", ""))
            mesin = str(row.get("mesin_status", "")).strip()
            pic = str(row.get("pic_cek", "")).strip()
            if mesin == "O":
                span = f"{start or '-'}-{end or '-'}"
                line = f"- Mesin stop/istirahat {span}"
                if pic and not _is_placeholder(pic):
                    line += f" | PIC cek: {pic}"
                vac_ops_lines.append(line)

    handover_rows = d.get("handover_rows", [])
    handover_lines: List[str] = []
    if isinstance(handover_rows, list):
        for row in handover_rows:
            jam = _fmt_jam(row.get("jam", ""))
            kirim = _v(row.get("kirim_pack"))
            terima = _v(row.get("terima_pack"))
            tl_kupas = _v(row.get("tl_kupas", row.get("pic_packing", "")))
            tl_packing = _v(row.get("tl_packing"))
            selisih = str(row.get("selisih_pack", "")).strip()
            alasan = str(row.get("alasan_selisih", "")).strip()
            line = f"- {jam or '-'} | kirim {kirim} | terima {terima} | TL Kupas {tl_kupas} | TL Packing {tl_packing}"
            if selisih and not _is_placeholder(selisih) and abs(parse_optional_float(selisih) or 0.0) > 0.001:
                line += f" | selisih {selisih}"
            if alasan and not _is_placeholder(alasan):
                line += f" | alasan: {alasan}"
            handover_lines.append(line)

    total_giling = _prefer_auto_total(d.get("total_giling", ""), d.get("giling_total_resep_auto", ""))
    total_giling = re.sub(r"\s*resep\s*$", "", total_giling, flags=re.IGNORECASE).strip() or "-"
    total_vacum = _prefer_auto_total(d.get("total_hasil_vakum", ""), d.get("vacum_total_pack_auto", ""))
    total_vacum = re.sub(r"\s*pack\s*$", "", total_vacum, flags=re.IGNORECASE).strip() or "-"

    return [
        "\n".join(
            [
                f"1. Produk : {_v(d.get('produk'))}",
                f"Jam kerja : {_v(d.get('jam_kerja_mulai'))} - {_v(d.get('jam_kerja_selesai'))}",
                f"1-2. Jumlah isi barang dalam pillow : {_v(d.get('isi_pillow_kg'))}",
                f"1-3. Nama petugas : {nama_petugas_txt}",
                f"1-4. Timer ada ? : {_v(d.get('timer_ada'))}",
                f"-> Petugas vakum / PIC : {_v(d.get('petugas_vacum'))}",
            ]
        ),
        "\n".join(
            [
                "2-1. Status defrost",
                "(Kalau sudah habis dipakai, tulis habis)",
                *defrost_lines,
                f"-> Total barang beku di ambil : {_v(d.get('total_beku'))}",
                f"-> Total bb fresh dipakai : {_normalize_unit_value(d.get('total_fresh_kg', '0'), 'kg')}",
                f"-> Total bb dibuang : {_normalize_unit_value(d.get('total_buang_kg', '0'), 'kg')}",
                f"-> Total : {_normalize_unit_value(d.get('total_akhir_kg', '0'), 'kg')} (Barang beku + bb fresh - bb dibuang)",
            ]
        ),
        "\n".join(
            [
                "2-2. Tempat untuk buang pillow barang defrost sudah siap dekat meja atau rak defrost?",
                *tempat_buang,
            ]
        ),
        "\n".join(
            [
                "3-1. Status Giling",
                *(giling_lines or ["-"]),
                f"--> Total Giling : {total_giling} resep",
                *(["-> Log delay giling:", *delay_lines] if delay_lines else []),
            ]
        ),
        "\n".join(
            [
                "3-2. Status vacum",
                *(vacum_lines or ["-"]),
                f"-> Total Hasil vakum : {total_vacum} pack",
                f"-> Nama : {_v(d.get('nama_pic_cek'))}",
                *(["-> Log operasional vacum:", *vac_ops_lines] if vac_ops_lines else []),
            ]
        ),
        "\n".join(["4. Total barang ada masalah", _v(d.get("masalah_total_barang"), "-")]),
        "\n".join(
            [
                "5. Total barang dikirim ke packing (atau press)",
                f"-> Dikirim kupas : {_v(d.get('total_dikirim_packing'))} pack",
                f"-> Diterima packing : {_v(d.get('total_diterima_packing'))} pack",
                f"-> Selisih : {_v(d.get('selisih_handover_packing'))} pack",
                f"-> Status cocok : {_v(d.get('status_handover_packing'))}",
                *(["-> Log serah-terima:", *handover_lines] if handover_lines else []),
            ]
        ),
        "\n".join(["CATATAN", _v(d.get("catatan"), "-")]),
    ]


def render_steril_blocks(payload: Dict[str, Any]) -> List[str]:
    d = payload["details"]
    nama_petugas = d.get("nama_petugas_list", [])
    nama_petugas_txt = ", ".join([str(x).strip() for x in nama_petugas if str(x).strip()]) or "-"

    defrost_lines = _defrost_lines(d)
    tempat_buang = _tempat_buang_lines(d)
    giling_lines = _clean_text_lines(d.get("status_giling", ""))

    steril_lines: List[str] = []
    for row in d.get("steril_rows", []) if isinstance(d.get("steril_rows", []), list) else []:
        jam = _fmt_jam(row.get("jam", ""))
        batch = _v(row.get("batch"))
        panci = _v(row.get("panci"))
        cat = str(row.get("catatan", "")).strip()
        line = f"- {jam or '-'} steril batch {batch} ({panci} panci)"
        if cat and not _is_placeholder(cat):
            line += f" | {cat}"
        steril_lines.append(line)
    if not steril_lines:
        steril_lines = _clean_text_lines(d.get("status_steril", "")) or ["-"]

    total_breakdown_lines: List[str] = []
    for row in d.get("total_steril_breakdown_rows", []) if isinstance(d.get("total_steril_breakdown_rows", []), list) else []:
        qty = str(row.get("qty_panci", "")).strip()
        berat = str(row.get("berat_kg", "")).strip()
        if qty and berat:
            total_breakdown_lines.append(f"- {qty} panci @{berat}kg")
    if not total_breakdown_lines:
        total_breakdown_lines = [_v(d.get("total_produk_steril"), "-")]

    check_lines: List[str] = []
    for row in d.get("steril_check_rows", []) if isinstance(d.get("steril_check_rows", []), list) else []:
        batch = _v(row.get("batch"))
        actual = _fmt_jam(row.get("jam_actual", "")) or "-"
        status = _v(row.get("status"), "-")
        check_lines.append(f"- {actual} steril batch {batch} | {status}")
    if not check_lines:
        check_lines = ["-"]

    cb_lines: List[str] = []
    for row in d.get("cb_rows", []) if isinstance(d.get("cb_rows", []), list) else []:
        jam = _fmt_jam(row.get("jam", ""))
        batch = _v(row.get("batch"))
        panci = _v(row.get("panci"))
        cb_lines.append(f"- {jam or '-'} cb batch {batch} ({panci} panci)")
    if not cb_lines:
        cb_lines = ["-"]

    total_giling = str(d.get("total_giling", "")).strip() or str(d.get("giling_total_resep_auto", "")).strip() or "-"

    return [
        "\n".join(
            [
                f"1. Produk : {_v(d.get('produk'))}",
                f"- Jam kerja : {_v(d.get('jam_kerja_mulai'))} - {_v(d.get('jam_kerja_selesai'))}",
                f"1-2. Jumlah isi barang untuk steril : {_v(d.get('isi_steril'))}",
                f"1-3. Nama petugas : {nama_petugas_txt}",
                f"1-4. Timer ada ? : {_v(d.get('timer_ada'))}",
                f"-> Petugas steril : {_v(d.get('petugas_steril'))}",
                f"-> Rencana jam steril : {_v(d.get('rencana_steril'))}",
            ]
        ),
        "\n".join(
            [
                "2-1. Status defrost",
                "(Kalau sudah habis dipakai, tulis habis)",
                *defrost_lines,
                f"-> Total barang beku di ambil : {_v(d.get('total_beku'))}",
                f"-> Total bb fresh dipakai : {_v(d.get('total_fresh_kg', '0'))}kg",
                f"-> Total bb dibuang : {_v(d.get('total_buang_kg', '0'))}kg",
                f"-> Total : {_v(d.get('total_akhir_kg', '0'))}kg",
            ]
        ),
        "\n".join(
            [
                "2-2. Tempat untuk buang pillow barang defrost sudah siap dekat meja atau rak defrost?",
                *tempat_buang,
            ]
        ),
        "\n".join(["3-1. Status Giling", *(giling_lines or ["-"]), f"--> Total Giling : {total_giling} resep"]),
        "\n".join(["3-2. Status Steril / Status Gas", *steril_lines, "-> Total Steril :", *total_breakdown_lines]),
        "\n".join(
            [f"3-2-1. Jam steril sudah sesuai? (Target : {_v(d.get('steril_target_minutes', '75'))} menit)", *check_lines]
        ),
        "\n".join(
            [
                "3-3. Status Coolbath (CB)",
                f"-> CB sudah dibersihkan dan isi air : {_v(d.get('cb_siap'))}",
                f"-> CB sudah dinyalakan : {_v(d.get('cb_nyala'))}",
                "-> Jam produk masuk ke CB",
                *cb_lines,
                "-> Total Produk Steril :",
                *total_breakdown_lines,
                f"-> Produk harus diambil packing dalam 20 menit : {_v(d.get('ambil_20_menit'))}",
                f"-> Tidak ada sisa barang di CB : {_v(d.get('tidak_ada_sisa_cb'))}",
            ]
        ),
        "\n".join(["CATATAN", _v(d.get("catatan"), "-")]),
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
    edited_ids: List[int] = []

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
                edited_ids.append(existing_ids[idx])
            else:
                desc = ""
                if isinstance(data, dict):
                    desc = str(data.get("description", "") or data.get("error", ""))
                desc_l = desc.lower()
                if "message is not modified" in desc_l:
                    result_ids.append(existing_ids[idx])
                    edited = True
                elif ("message to edit not found" in desc_l) or ("can't be edited" in desc_l) or ("message can't be edited" in desc_l):
                    err_msgs.append(f"edit part {idx+1} failed (fallback send new)")
                else:
                    err_msgs.append(f"edit part {idx+1} failed: {desc or 'unknown'}")

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

    if edited_ids:
        root_reply_id = edited_ids[0]
        ok_upd, _, data_upd = tg_send_update_reply(root_reply_id, "Laporan sudah diperbarui.")
        if not ok_upd:
            desc = ""
            if isinstance(data_upd, dict):
                desc = str(data_upd.get("description", "") or data_upd.get("error", ""))
            err_msgs.append(f"send update reply failed: {desc or 'unknown'}")

    photo_specs = [("handover_photo_path", "Bukti handover packing")]
    sent_photo_paths: set[str] = set()
    for photo_key, caption_label in photo_specs:
        photo_path = str(payload.get("details", {}).get(photo_key, "")).strip()
        if not photo_path or photo_path in sent_photo_paths:
            continue
        sent_photo_paths.add(photo_path)
        cap = f"{caption_label} | {payload.get('team_id', '-')} | {payload.get('work_date', '-')} {hhmm_now()}"
        ok_photo, _, data_photo = tg_send_photo(photo_path, cap)
        if not ok_photo:
            desc = ""
            if isinstance(data_photo, dict):
                desc = str(data_photo.get("description", "") or data_photo.get("error", ""))
            err_msgs.append(f"send photo failed ({caption_label}): {desc or 'unknown'}")
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
        if not sheets_ok:
            print(f"[WARN] Sheets backup gagal: {sh_err}")
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
    MAX_RETRY = 5
    queue = load_json(PENDING_FILE, [])
    queue = [x for x in queue if x.get("retry_count", 0) < MAX_RETRY]
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
    if not details.get("nama_petugas_list", []):
        errs.append("1-3 Nama petugas wajib diisi (bisa lebih dari satu nama).")
    if not details.get("petugas_vacum", "").strip():
        errs.append("Petugas vakum wajib diisi. Jika tidak vakum, isi nama PIC yang bertanggung jawab.")
    if details.get("timer_ada", "") not in {"O", "X"}:
        errs.append("Timer ada? wajib pilih O atau X.")
    total_beku_kg = parse_optional_float(details.get("total_beku_kg"))
    total_fresh_kg = parse_optional_float(details.get("total_fresh_kg"))
    total_buang_kg = parse_optional_float(details.get("total_buang_kg"))
    total_akhir_kg = parse_optional_float(details.get("total_akhir_kg"))
    if total_beku_kg is None:
        errs.append("Total barang beku (kg) wajib diisi angka.")
    if total_fresh_kg is None:
        errs.append("Total bb fresh dipakai (kg) wajib diisi angka.")
    if total_buang_kg is None:
        errs.append("Total bb dibuang (kg) wajib diisi angka.")
    if total_akhir_kg is None:
        errs.append("Total akhir (kg) wajib diisi angka.")
    if None not in (total_beku_kg, total_fresh_kg, total_buang_kg, total_akhir_kg):
        if total_beku_kg < 0 or total_fresh_kg < 0 or total_buang_kg < 0 or total_akhir_kg < 0:
            errs.append("Nilai kilogram tidak boleh negatif.")
        expected = total_beku_kg + total_fresh_kg - total_buang_kg
        if abs(expected - total_akhir_kg) > 0.001:
            errs.append("Total akhir harus sama dengan (barang beku + fresh - dibuang).")
    else:
        errs.append("Total akhir harus sama dengan (barang beku + fresh - dibuang).")
    handover_rows = details.get("handover_rows", [])
    if not handover_rows:
        errs.append("Isi minimal 1 baris serah-terima ke packing/press.")
    else:
        for idx, row in enumerate(handover_rows, start=1):
            kirim = parse_optional_float(row.get("kirim_pack"))
            terima = parse_optional_float(row.get("terima_pack"))
            jam = str(row.get("jam", "")).strip()
            tl_packing = str(row.get("tl_packing", "")).strip()
            tl_kupas = str(row.get("tl_kupas", row.get("pic_packing", ""))).strip()
            alasan = str(row.get("alasan_selisih", "")).strip()
            if not jam:
                errs.append(f"Baris handover {idx}: jam wajib diisi.")
            if kirim is None:
                errs.append(f"Baris handover {idx}: total dikirim wajib angka.")
            if terima is None:
                errs.append(f"Baris handover {idx}: total diterima wajib angka.")
            if not tl_packing:
                errs.append(f"Baris handover {idx}: nama TL packing wajib diisi.")
            if not tl_kupas:
                errs.append(f"Baris handover {idx}: nama TL kupas wajib diisi.")
            if kirim is not None and terima is not None:
                if kirim < 0 or terima < 0:
                    errs.append(f"Baris handover {idx}: nilai tidak boleh negatif.")
                selisih = kirim - terima
                if abs(selisih) > 0.001 and not alasan:
                    errs.append(f"Baris handover {idx}: ada selisih, alasan wajib diisi.")
    tempat_rows = details.get("tempat_buang_rows", [])
    active_tempat_rows: List[Dict[str, Any]] = []
    if isinstance(tempat_rows, list):
        for row in tempat_rows:
            jam = str(row.get("jam", "")).strip()
            status = str(row.get("status", "")).strip()
            catatan = str(row.get("catatan", "")).strip()
            if jam or status or catatan:
                active_tempat_rows.append(row)
    if active_tempat_rows:
        for idx, row in enumerate(active_tempat_rows, start=1):
            jam = str(row.get("jam", "")).strip()
            status = str(row.get("status", "")).strip()
            if status not in {"O", "X"}:
                errs.append(f"2-2 log {idx}: status wajib O atau X.")
            if not is_valid_hhmm(jam):
                errs.append(f"2-2 log {idx}: jam wajib diisi (contoh 1000 atau 10:00).")
    else:
        if details.get("tempat_buang_siap", "") not in {"O", "X"}:
            errs.append("2-2 wajib dipilih O atau X pada setiap laporan.")
        if not is_valid_hhmm(details.get("tempat_buang_check_time", "")):
            errs.append("2-2 jam cek wajib diisi (contoh 1000 atau 10:00).")
    giling_delay_rows = details.get("giling_delay_rows", [])
    active_delay_rows: List[Dict[str, Any]] = []
    if isinstance(giling_delay_rows, list):
        for row in giling_delay_rows:
            jam = str(row.get("jam", "")).strip()
            status = str(row.get("status", "")).strip()
            detail = str(row.get("detail", "")).strip()
            if jam or detail or status == "O":
                active_delay_rows.append(row)
    if not active_delay_rows:
        errs.append("Isi minimal 1 log delay giling (jam + status O/X).")
    else:
        for idx, row in enumerate(active_delay_rows, start=1):
            jam = str(row.get("jam", "")).strip()
            status = str(row.get("status", "")).strip()
            detail = str(row.get("detail", "")).strip()
            if not jam:
                errs.append(f"Log delay giling {idx}: jam wajib diisi.")
            if status not in {"O", "X"}:
                errs.append(f"Log delay giling {idx}: status wajib O atau X.")
            if status == "O" and not detail:
                errs.append(f"Log delay giling {idx}: detail wajib diisi jika status O.")

    total_vacum = parse_optional_float(details.get("total_hasil_vakum"))
    total_vacum_defect = parse_optional_float(details.get("total_vacum_defect_pack"))
    if total_vacum is None:
        errs.append("Total vakum diproses (pack) wajib diisi angka.")
    if total_vacum_defect is None:
        errs.append("Total vakum bermasalah (pack) wajib diisi angka.")

    vacum_defect_rows = details.get("vacum_defect_rows", [])
    defect_sum_from_rows = 0.0
    has_defect_row = False
    if isinstance(vacum_defect_rows, list):
        for idx, row in enumerate(vacum_defect_rows, start=1):
            jenis = str(row.get("jenis", "")).strip()
            jumlah_raw = str(row.get("jumlah_pack", "")).strip()
            if not jenis and not jumlah_raw:
                continue
            has_defect_row = True
            jumlah_val = parse_optional_float(jumlah_raw)
            if not jenis:
                errs.append(f"Barang ada masalah vacum {idx} wajib diisi.")
            if jumlah_val is None:
                errs.append(f"Jumlah pack barang bermasalah vacum {idx} wajib angka.")
                continue
            if jumlah_val < 0:
                errs.append(f"Jumlah pack barang bermasalah vacum {idx} tidak boleh negatif.")
                continue
            defect_sum_from_rows += jumlah_val

    if total_vacum is not None and total_vacum_defect is not None:
        if total_vacum < 0 or total_vacum_defect < 0:
            errs.append("Nilai total vacum tidak boleh negatif.")
        if total_vacum_defect > total_vacum:
            errs.append("Total vacum bermasalah tidak boleh lebih besar dari total vacum diproses.")
        if has_defect_row and abs(defect_sum_from_rows - total_vacum_defect) > 0.001:
            errs.append("Jumlah pack dari barang ada masalah vacum harus sama dengan total vacum bermasalah.")
        if total_vacum_defect > 0 and not has_defect_row:
            errs.append("Jika ada vacum bermasalah, barang ada masalah vacum wajib diisi.")

    vacum_ops_rows = details.get("vacum_ops_rows", [])
    active_ops_rows: List[Dict[str, Any]] = []
    if isinstance(vacum_ops_rows, list):
        for row in vacum_ops_rows:
            stop_start = str(row.get("stop_start", row.get("jam", ""))).strip()
            stop_end = str(row.get("stop_end", "")).strip()
            mesin_status = str(row.get("mesin_status", "")).strip()
            pic_cek = str(row.get("pic_cek", "")).strip()
            if stop_start or stop_end or mesin_status or pic_cek:
                active_ops_rows.append(row)
    if not active_ops_rows:
        errs.append("Isi minimal 1 log operasional vacum.")
    else:
        for idx, row in enumerate(active_ops_rows, start=1):
            stop_start = str(row.get("stop_start", row.get("jam", ""))).strip()
            stop_end = str(row.get("stop_end", "")).strip()
            mesin_status = str(row.get("mesin_status", "")).strip()
            pic_cek = str(row.get("pic_cek", "")).strip()
            if mesin_status not in {"O", "X"}:
                errs.append(f"Log operasional vacum {idx}: mesin stop/istirahat wajib O/X.")
            if mesin_status == "O":
                if not is_valid_hhmm(stop_start):
                    errs.append(f"Log operasional vacum {idx}: jam mulai stop wajib diisi (contoh 1000 atau 10:00).")
                if not is_valid_hhmm(stop_end):
                    errs.append(f"Log operasional vacum {idx}: jam selesai stop wajib diisi (contoh 1030 atau 10:30).")
            if not pic_cek:
                errs.append(f"Log operasional vacum {idx}: PIC cek wajib diisi.")
    return errs


def validate_steril(details: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    if not str(details.get("rencana_steril", "")).strip():
        errs.append("Rencana jam steril wajib diisi.")
    if not str(details.get("produk", "")).strip():
        errs.append("Produk wajib diisi.")
    if not details.get("nama_petugas_list", []):
        errs.append("1-3 Nama petugas wajib diisi (bisa lebih dari satu nama).")
    if not details.get("petugas_steril", "").strip():
        errs.append("Untuk laporan steril, petugas steril wajib diisi.")
    if details.get("timer_ada", "") not in {"O", "X"}:
        errs.append("Timer ada? wajib pilih O atau X.")

    total_beku_kg = parse_optional_float(details.get("total_beku_kg"))
    total_fresh_kg = parse_optional_float(details.get("total_fresh_kg"))
    total_buang_kg = parse_optional_float(details.get("total_buang_kg"))
    total_akhir_kg = parse_optional_float(details.get("total_akhir_kg"))
    if total_beku_kg is None:
        errs.append("Total barang beku (kg) wajib diisi angka.")
    if total_fresh_kg is None:
        errs.append("Total bb fresh dipakai (kg) wajib diisi angka.")
    if total_buang_kg is None:
        errs.append("Total bb dibuang (kg) wajib diisi angka.")
    if total_akhir_kg is None:
        errs.append("Total akhir (kg) wajib diisi angka.")
    if None not in (total_beku_kg, total_fresh_kg, total_buang_kg, total_akhir_kg):
        if total_beku_kg < 0 or total_fresh_kg < 0 or total_buang_kg < 0 or total_akhir_kg < 0:
            errs.append("Nilai kilogram tidak boleh negatif.")
        expected = total_beku_kg + total_fresh_kg - total_buang_kg
        if abs(expected - total_akhir_kg) > 0.001:
            errs.append("Total akhir harus sama dengan (barang beku + fresh - dibuang).")
    else:
        errs.append("Total akhir harus sama dengan (barang beku + fresh - dibuang).")

    tempat_rows = details.get("tempat_buang_rows", [])
    active_tempat_rows: List[Dict[str, Any]] = []
    if isinstance(tempat_rows, list):
        for row in tempat_rows:
            jam = str(row.get("jam", "")).strip()
            status = str(row.get("status", "")).strip()
            catatan = str(row.get("catatan", "")).strip()
            if jam or status or catatan:
                active_tempat_rows.append(row)
    if active_tempat_rows:
        for idx, row in enumerate(active_tempat_rows, start=1):
            jam = str(row.get("jam", "")).strip()
            status = str(row.get("status", "")).strip()
            if status not in {"O", "X"}:
                errs.append(f"2-2 log {idx}: status wajib O atau X.")
            if not is_valid_hhmm(jam):
                errs.append(f"2-2 log {idx}: jam wajib diisi (contoh 1000 atau 10:00).")
    else:
        if details.get("tempat_buang_siap", "") not in {"O", "X"}:
            errs.append("2-2 wajib dipilih O atau X pada setiap laporan.")
        if not is_valid_hhmm(details.get("tempat_buang_check_time", "")):
            errs.append("2-2 jam cek wajib diisi (contoh 1000 atau 10:00).")
    if not str(details.get("status_defrost", "")).strip():
        errs.append("2-1 Status defrost wajib diisi.")
    if not str(details.get("status_giling", "")).strip():
        errs.append("3-1 Status giling wajib diisi.")

    steril_target_minutes = parse_optional_int(details.get("steril_target_minutes"), 75)
    if steril_target_minutes <= 0:
        errs.append("Target menit steril harus lebih dari 0.")

    steril_rows = details.get("steril_rows", [])
    active_steril_rows: List[Dict[str, Any]] = []
    steril_start_by_batch: Dict[str, str] = {}
    if isinstance(steril_rows, list):
        for row in steril_rows:
            jam = str(row.get("jam", "")).strip()
            batch = str(row.get("batch", "")).strip()
            panci = str(row.get("panci", "")).strip()
            catatan = str(row.get("catatan", "")).strip()
            if jam or batch or panci or catatan:
                active_steril_rows.append(row)
    if not active_steril_rows:
        errs.append("3-2 Status steril/gas: isi minimal 1 log steril batch.")
    else:
        for idx, row in enumerate(active_steril_rows, start=1):
            jam = str(row.get("jam", "")).strip()
            batch = str(row.get("batch", "")).strip()
            panci_raw = str(row.get("panci", "")).strip()
            if not is_valid_hhmm(jam):
                errs.append(f"3-2 log steril {idx}: jam wajib diisi (contoh 1000 atau 10:00).")
            if not batch:
                errs.append(f"3-2 log steril {idx}: batch wajib diisi.")
            panci_val = parse_optional_float(panci_raw)
            if panci_val is None:
                errs.append(f"3-2 log steril {idx}: jumlah panci wajib angka.")
            elif panci_val <= 0:
                errs.append(f"3-2 log steril {idx}: jumlah panci harus lebih dari 0.")
            if batch and jam and is_valid_hhmm(jam) and batch not in steril_start_by_batch:
                steril_start_by_batch[batch] = jam

    steril_check_rows = details.get("steril_check_rows", [])
    active_steril_check_rows: List[Dict[str, Any]] = []
    if isinstance(steril_check_rows, list):
        for row in steril_check_rows:
            batch = str(row.get("batch", "")).strip()
            jam_actual = str(row.get("jam_actual", "")).strip()
            if batch or jam_actual:
                active_steril_check_rows.append(row)
    if active_steril_rows and not active_steril_check_rows:
        errs.append("3-2-1 Jam steril sesuai: isi minimal 1 log cek jam steril.")
    else:
        for idx, row in enumerate(active_steril_check_rows, start=1):
            batch = str(row.get("batch", "")).strip()
            jam_actual = str(row.get("jam_actual", "")).strip()
            if not batch:
                errs.append(f"3-2-1 log cek {idx}: batch wajib diisi.")
            if not is_valid_hhmm(jam_actual):
                errs.append(f"3-2-1 log cek {idx}: jam aktual wajib diisi (contoh 1000 atau 10:00).")
            start_jam = steril_start_by_batch.get(batch, "")
            if start_jam and is_valid_hhmm(jam_actual):
                diff = minutes_diff_hhmm(start_jam, jam_actual)
                if diff is not None and diff < steril_target_minutes:
                    errs.append(
                        f"3-2-1 log cek {idx}: durasi batch {batch} kurang dari target {steril_target_minutes} menit."
                    )

    total_rows = details.get("total_steril_breakdown_rows", [])
    active_total_rows: List[Dict[str, Any]] = []
    if isinstance(total_rows, list):
        for row in total_rows:
            qty = str(row.get("qty_panci", "")).strip()
            berat = str(row.get("berat_kg", "")).strip()
            if qty or berat:
                active_total_rows.append(row)
    if not active_total_rows:
        errs.append("Isi minimal 1 rincian total steril (contoh: 72 panci @5kg).")
    else:
        for idx, row in enumerate(active_total_rows, start=1):
            qty_val = parse_optional_float(row.get("qty_panci", ""))
            berat_raw = str(row.get("berat_kg", "")).strip()
            if qty_val is None or qty_val <= 0:
                errs.append(f"Rincian total steril {idx}: jumlah panci wajib angka > 0.")
            if not berat_raw:
                errs.append(f"Rincian total steril {idx}: berat per panci wajib diisi.")

    if details.get("cb_siap", "") not in {"O", "X"}:
        errs.append("3-3 CB bersih + isi air wajib pilih O/X.")
    if details.get("cb_nyala", "") not in {"O", "X"}:
        errs.append("3-3 CB dinyalakan wajib pilih O/X.")
    if details.get("ambil_20_menit", "") not in {"O", "X"}:
        errs.append("3-3 Produk diambil <=20 menit wajib pilih O/X.")
    if details.get("tidak_ada_sisa_cb", "") not in {"O", "X"}:
        errs.append("3-3 Tidak ada sisa di CB wajib pilih O/X.")

    cb_rows = details.get("cb_rows", [])
    active_cb_rows: List[Dict[str, Any]] = []
    if isinstance(cb_rows, list):
        for row in cb_rows:
            jam = str(row.get("jam", "")).strip()
            batch = str(row.get("batch", "")).strip()
            panci = str(row.get("panci", "")).strip()
            catatan = str(row.get("catatan", "")).strip()
            if jam or batch or panci or catatan:
                active_cb_rows.append(row)
    if not active_cb_rows:
        errs.append("3-3 Status Coolbath: isi minimal 1 log jam produk masuk CB.")
    else:
        for idx, row in enumerate(active_cb_rows, start=1):
            jam = str(row.get("jam", "")).strip()
            batch = str(row.get("batch", "")).strip()
            panci_raw = str(row.get("panci", "")).strip()
            if not is_valid_hhmm(jam):
                errs.append(f"3-3 log CB {idx}: jam wajib diisi (contoh 1000 atau 10:00).")
            if not batch:
                errs.append(f"3-3 log CB {idx}: batch wajib diisi.")
            panci_val = parse_optional_float(panci_raw)
            if panci_val is None:
                errs.append(f"3-3 log CB {idx}: jumlah panci wajib angka.")
            elif panci_val <= 0:
                errs.append(f"3-3 log CB {idx}: jumlah panci harus lebih dari 0.")
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


def section_checkpoint_scope(team_id: str, work_date: str, report_type: str, section_id: str) -> str:
    return f"{team_id.strip()}::{work_date}::{report_type}::{section_id}"


def get_section_checkpoint(team_id: str, work_date: str, report_type: str, section_id: str) -> str:
    data = load_json(SECTION_CHECKPOINT_FILE, {})
    key = section_checkpoint_scope(team_id, work_date, report_type, section_id)
    rec = data.get(key, {})
    if isinstance(rec, dict):
        return str(rec.get("saved_at", "")).strip()
    return ""


def save_section_checkpoint(team_id: str, work_date: str, report_type: str, section_id: str) -> str:
    data = load_json(SECTION_CHECKPOINT_FILE, {})
    key = section_checkpoint_scope(team_id, work_date, report_type, section_id)
    saved_at = ts_str()
    data[key] = {"saved_at": saved_at}
    save_json(SECTION_CHECKPOINT_FILE, data)
    return saved_at


def render_section_checkpoint_ui(team_id: str, work_date: str, report_type: str, section_id: str, label: str) -> None:
    c1, c2 = st.columns([2, 6])
    with c1:
        if st.button(f"Simpan {label}", key=f"btn_ckpt::{team_id}::{work_date}::{report_type}::{section_id}"):
            saved_at = save_section_checkpoint(team_id, work_date, report_type, section_id)
            st.session_state[f"ckpt_last::{team_id}::{work_date}::{report_type}::{section_id}"] = saved_at
    with c2:
        saved_at = str(st.session_state.get(f"ckpt_last::{team_id}::{work_date}::{report_type}::{section_id}", "")).strip()
        if not saved_at:
            saved_at = get_section_checkpoint(team_id, work_date, report_type, section_id)
        if saved_at:
            st.caption(f"Status: tersimpan sementara pada {saved_at}")
        else:
            st.caption("Status: belum disimpan sementara.")


def main() -> None:
    ensure_storage()
    st.set_page_config(page_title="Laporan Giling Kupas", layout="centered")
    inject_compact_ui_theme()
    if TEAM_PASSWORDS_ERROR:
        st.warning(TEAM_PASSWORDS_ERROR)
    st.title("Laporan Giling")
    st.caption("Mobile-first report app (30 menit / 1 kali) | Telegram utama | Google Sheets backup wajib")

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
    if "defrost_rows_st" not in st.session_state:
        st.session_state["defrost_rows_st"] = 1
    if "tempat_buang_rows_non" not in st.session_state:
        st.session_state["tempat_buang_rows_non"] = 1
    if "tempat_buang_rows_st" not in st.session_state:
        st.session_state["tempat_buang_rows_st"] = 1
    if "giling_rows_non" not in st.session_state:
        st.session_state["giling_rows_non"] = 1
    if "giling_rows_st" not in st.session_state:
        st.session_state["giling_rows_st"] = 1
    if "giling_delay_rows_non" not in st.session_state:
        st.session_state["giling_delay_rows_non"] = 1
    if "vacum_rows_non" not in st.session_state:
        st.session_state["vacum_rows_non"] = 1
    if "vacum_ops_rows_non" not in st.session_state:
        st.session_state["vacum_ops_rows_non"] = 1
    if "vacum_defect_rows_non" not in st.session_state:
        st.session_state["vacum_defect_rows_non"] = 1
    if "handover_rows_non" not in st.session_state:
        st.session_state["handover_rows_non"] = 1
    if "loaded_scope_key" not in st.session_state:
        st.session_state["loaded_scope_key"] = ""
    if "sticky_validation_active" not in st.session_state:
        st.session_state["sticky_validation_active"] = False
    if "sticky_validation_errors" not in st.session_state:
        st.session_state["sticky_validation_errors"] = []

    with st.container(border=True):
        st.markdown("**Header**")
        lc1, lc2, lc3, lc4 = st.columns(4)
        with lc1:
            team_choices = list(TEAM_PASSWORDS.keys())
            default_team = st.session_state.get("team_scope", team_choices[0] if team_choices else "")
            team_index = team_choices.index(default_team) if default_team in team_choices else 0
            team_scope = st.selectbox(
                "Team ID",
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
            if st.button("Open Team", use_container_width=True):
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
            if st.button("Take Over Team", use_container_width=True):
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

    if st.session_state.get("loaded_scope_key", "") != scope:
        loaded_scope_state = load_work_state(team_scope, str(work_date_scope))
        loaded_scope_report_type = ""
        loaded_scope_details: Dict[str, Any] = {}
        if isinstance(loaded_scope_state, dict):
            loaded_scope_report_type = str(loaded_scope_state.get("report_type", "")).strip()
            loaded_scope_details_raw = loaded_scope_state.get("details", {})
            if isinstance(loaded_scope_details_raw, dict):
                loaded_scope_details = loaded_scope_details_raw
            loaded_shift = str(loaded_scope_state.get("shift", "")).strip()
            loaded_pelapor = str(loaded_scope_state.get("pelapor", "")).strip()
            if loaded_shift in {"1", "2", "3"}:
                st.session_state["shift"] = loaded_shift
            if loaded_pelapor:
                st.session_state["pelapor"] = loaded_pelapor
        if loaded_scope_report_type in {"non_steril", "steril_required"}:
            st.session_state["report_type_confirmed"] = loaded_scope_report_type
            st.session_state["report_type"] = loaded_scope_report_type
        st.session_state["loaded_details"] = loaded_scope_details
        st.session_state["loaded_scope_key"] = scope
        st.session_state["sticky_validation_active"] = False
        st.session_state["sticky_validation_errors"] = []

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

    with st.container(border=True):
        st.markdown("**Jenis Laporan Giling**")
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
                    st.session_state["sticky_validation_active"] = False
                    st.session_state["sticky_validation_errors"] = []
                    st.session_state["await_report_type_confirm"] = False
                    st.session_state["pending_report_type"] = ""
                    st.rerun()
            with cf2:
                if st.button("Batal", use_container_width=True):
                    st.session_state["await_report_type_confirm"] = False
                    st.session_state["pending_report_type"] = ""
                    st.rerun()

    loaded_details = st.session_state.get("loaded_details", {})
    with st.container(border=True):
        st.markdown("**Petugas + Detail kerja**")
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

        details: Dict[str, Any] = {}
        if report_type == "non_steril":
            st.markdown("### 1. Produk")
            produk = st.text_input("1. Produk", value=loaded_details.get("produk", ""))
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
            nama_petugas_raw = st.text_area(
                "1-3. Nama Petugas (satu baris satu nama)",
                value=loaded_details.get("nama_petugas_raw", ""),
                placeholder="Linda\nLian",
            )
            st.caption("Tambah manual: 1 baris = 1 nama petugas.")
            nama_petugas_list = parse_name_lines(nama_petugas_raw)
            timer_ada = st.selectbox(
                "1-4. Timer ada ?",
                options=["O", "X"],
                index=0 if loaded_details.get("timer_ada", "O") == "O" else 1,
            )
            alat = ""
            st.markdown("### 2-1. Status defrost")
            mode_defrost = st.radio(
                "Cara isi status defrost",
                options=["List baris", "Tulis manual"],
                horizontal=True,
                key="mode_defrost_non",
            )
            defrost_total_pack_auto = str(loaded_details.get("defrost_total_pack_auto", ""))
            if mode_defrost == "List baris":
                seed_defrost_non_key = f"seed_defrost_non::{team_id}::{work_date}"
                if not st.session_state.get(seed_defrost_non_key, False):
                    loaded_defrost_rows_non = loaded_details.get("defrost_rows", [])
                    if isinstance(loaded_defrost_rows_non, list) and loaded_defrost_rows_non:
                        existing_defrost_local = False
                        for i in range(20):
                            if str(st.session_state.get(f"def_jam_non_{i}", "")).strip() or str(
                                st.session_state.get(f"def_isi_non_{i}", "")
                            ).strip() or str(st.session_state.get(f"def_kg_non_{i}", "")).strip() or str(
                                st.session_state.get(f"def_cat_non_{i}", "")
                            ).strip():
                                existing_defrost_local = True
                                break
                        if not existing_defrost_local:
                            st.session_state["defrost_rows_non"] = min(20, max(1, len(loaded_defrost_rows_non)))
                            for idx, row in enumerate(loaded_defrost_rows_non[:20]):
                                st.session_state[f"def_no_non_{idx}"] = str(row.get("no", idx + 1))
                                st.session_state[f"def_jam_non_{idx}"] = str(row.get("jam", ""))
                                st.session_state[f"def_isi_non_{idx}"] = str(row.get("status", ""))
                                st.session_state[f"def_kg_non_{idx}"] = str(row.get("pack", ""))
                                st.session_state[f"def_cat_non_{idx}"] = str(row.get("catatan", ""))
                    st.session_state[seed_defrost_non_key] = True
                d1, d2, d3 = st.columns([2, 2, 6])
                with d1:
                    if st.button("+ Tambah", key="btn_add_defrost"):
                        st.session_state["defrost_rows_non"] = min(20, int(st.session_state.get("defrost_rows_non", 1)) + 1)
                with d2:
                    if st.button("- Hapus", key="btn_del_defrost"):
                        drop_last_row_from_session(
                            "defrost_rows_non",
                            ["def_jam_non_", "def_isi_non_", "def_kg_non_", "def_cat_non_"],
                            min_rows=1,
                        )
                with d3:
                    pass

                row_count = ensure_row_count_from_session(
                    "defrost_rows_non",
                    ["def_jam_non_", "def_isi_non_", "def_kg_non_", "def_cat_non_"],
                    min_rows=1,
                    max_rows=20,
                )
                defrost_lines: List[str] = []
                defrost_rows: List[Dict[str, Any]] = []
                defrost_pack_sum = 0.0
                defrost_pack_invalid = 0
                for idx in range(int(row_count)):
                    dc0, dc1, dc2, dc3, dc4 = st.columns([1, 2, 3, 2, 3])
                    no_key = f"def_no_non_{idx}"
                    if not str(st.session_state.get(no_key, "")).strip():
                        st.session_state[no_key] = str(idx + 1)
                    no = dc0.text_input("No", key=no_key, max_chars=3)
                    jam = dc1.text_input("Jam", placeholder="12:55", key=f"def_jam_non_{idx}")
                    isi = dc2.text_input("Status", placeholder="BB fresh", key=f"def_isi_non_{idx}")
                    pack = dc3.text_input("Pack", placeholder="75", key=f"def_kg_non_{idx}")
                    cat = dc4.text_input("Catatan", placeholder="sudah termasuk campuran", key=f"def_cat_non_{idx}")
                    pack_val = parse_optional_float(pack)
                    if pack.strip():
                        if pack_val is None:
                            defrost_pack_invalid += 1
                        elif pack_val >= 0:
                            defrost_pack_sum += pack_val
                    if jam.strip() or isi.strip() or pack.strip():
                        defrost_rows.append({"no": no, "jam": jam, "status": isi, "pack": pack, "catatan": cat})
                        status_part = isi.strip()
                        if pack.strip():
                            status_part = f"{status_part} = {pack.strip()}pack".strip()
                        prefix = f"[{no.strip()}] " if no.strip() else ""
                        defrost_lines.append(f"- {prefix}{jam.strip()} {status_part}".strip())
                    if cat.strip():
                        defrost_lines.append(f"({cat.strip()})")
                status_defrost = "\n".join(defrost_lines).strip()
                st.caption("Preview status defrost")
                st.code(status_defrost or "-")
                defrost_total_pack_auto = format_float_compact(defrost_pack_sum)
                st.text_input("Total pack defrost (otomatis)", value=defrost_total_pack_auto, disabled=True)
                if defrost_pack_invalid > 0:
                    st.warning(f"Ada {defrost_pack_invalid} nilai pack yang bukan angka, tidak dihitung.")
                extra_manual = st.text_area(
                    "Tambahan manual (opsional)",
                    value="",
                    key="def_extra_non",
                )
                if extra_manual.strip():
                    status_defrost = (status_defrost + "\n" + extra_manual.strip()).strip()
            else:
                defrost_rows = []
                status_defrost = st.text_area(
                    "Status defrost (Kalau sudah habis dipakai, tulis habis)",
                    value=loaded_details.get("status_defrost", ""),
                    placeholder="- 12:55 BB fresh = 75kg\n(sudah termasuk campuran)\n- 13:00 BB fresh = 75kg",
                )
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "defrost", "2-1 Defrost")
            total_beku = st.text_input(
                "Total barang beku diambil (contoh: sim km 20 pack)",
                value=loaded_details.get("total_beku", ""),
            )
            total_beku_kg_key = f"total_beku_kg_non::{team_id}::{work_date}"
            total_fresh_kg_key = f"total_fresh_kg_non::{team_id}::{work_date}"
            total_buang_kg_key = f"total_buang_kg_non::{team_id}::{work_date}"
            total_akhir_kg_key = f"total_akhir_kg_non::{team_id}::{work_date}"
            for key_name, seed in [
                (total_beku_kg_key, str(loaded_details.get("total_beku_kg", ""))),
                (total_fresh_kg_key, str(loaded_details.get("total_fresh_kg", ""))),
                (total_buang_kg_key, str(loaded_details.get("total_buang_kg", ""))),
                (total_akhir_kg_key, str(loaded_details.get("total_akhir_kg", ""))),
            ]:
                if key_name not in st.session_state:
                    st.session_state[key_name] = seed

            total_beku_kg = st.text_input(
                "Total barang beku (kg, angka untuk validasi)",
                key=total_beku_kg_key,
                placeholder="contoh: 75",
            )
            total_fresh_kg = st.text_input(
                "Total bb fresh dipakai (kg)",
                key=total_fresh_kg_key,
                placeholder="contoh: 225",
            )
            total_buang_kg = st.text_input(
                "Total bb dibuang (kg)",
                key=total_buang_kg_key,
                placeholder="contoh: 0",
            )
            beku_val_non = parse_optional_float(total_beku_kg)
            fresh_val_non = parse_optional_float(total_fresh_kg)
            buang_val_non = parse_optional_float(total_buang_kg)
            total_akhir_auto_non = ""
            if None not in (beku_val_non, fresh_val_non, buang_val_non):
                total_akhir_auto_non = format_float_compact((beku_val_non or 0.0) + (fresh_val_non or 0.0) - (buang_val_non or 0.0))
            st.session_state[total_akhir_kg_key] = total_akhir_auto_non
            st.text_input("Total akhir (kg, otomatis)", value=total_akhir_auto_non, disabled=True)
            total_akhir_kg = total_akhir_auto_non

            calc_scope = f"{team_id}::{work_date}"
            calc_expr_key = f"kg_calc_expr_non::{calc_scope}"
            calc_history_key = f"kg_calc_history_non::{calc_scope}"
            calc_open_key = f"kg_calc_sidebar_open_non::{calc_scope}"
            if calc_history_key not in st.session_state:
                hist_seed: List[Dict[str, str]] = []
                loaded_hist = loaded_details.get("kg_calc_history", [])
                if isinstance(loaded_hist, list):
                    hist_seed = [
                        {"at": str(x.get("at", "")), "expr": str(x.get("expr", "")), "result": str(x.get("result", ""))}
                        for x in loaded_hist
                        if isinstance(x, dict)
                    ]
                if not hist_seed:
                    saved_scope = load_work_state(team_id.strip(), str(work_date))
                    saved_details = saved_scope.get("details", {}) if isinstance(saved_scope, dict) else {}
                    saved_hist = saved_details.get("kg_calc_history", []) if isinstance(saved_details, dict) else []
                    if isinstance(saved_hist, list):
                        hist_seed = [
                            {"at": str(x.get("at", "")), "expr": str(x.get("expr", "")), "result": str(x.get("result", ""))}
                            for x in saved_hist
                            if isinstance(x, dict)
                        ]
                st.session_state[calc_history_key] = hist_seed[:25]
            if calc_expr_key not in st.session_state:
                st.session_state[calc_expr_key] = ""
            if calc_open_key not in st.session_state:
                st.session_state[calc_open_key] = False

            with st.sidebar:
                st.markdown("### Kalkulator")
                calc_button_label = "Buka kalkulator kg" if not st.session_state.get(calc_open_key, False) else "Tutup kalkulator kg"
                if st.button(calc_button_label, key=f"btn_toggle_kg_calc_non::{calc_scope}", use_container_width=True):
                    st.session_state[calc_open_key] = not st.session_state.get(calc_open_key, False)
                    st.rerun()

                if st.session_state.get(calc_open_key, False):
                    with st.container(border=True):
                        st.caption("Kalkulator cepat (angka +, -, *, /)")
                        st.text_input(
                            "Rumus kg",
                            key=calc_expr_key,
                            placeholder="contoh: (75 + 75 + 90) - 10",
                        )
                        calc_value, calc_err = eval_simple_math(st.session_state.get(calc_expr_key, ""))
                        if calc_err:
                            st.warning(calc_err)
                        elif calc_value is not None:
                            st.success(f"Hasil: {format_float_compact(calc_value)} kg")
                        else:
                            st.caption("Isi rumus untuk hitung.")

                        apply_target_map = {
                            "Total barang beku (kg)": total_beku_kg_key,
                            "Total bb fresh dipakai (kg)": total_fresh_kg_key,
                            "Total bb dibuang (kg)": total_buang_kg_key,
                        }
                        target_label = st.selectbox(
                            "Pakai hasil ke",
                            options=list(apply_target_map.keys()),
                            key=f"kg_calc_target_non::{calc_scope}",
                        )

                        sb1, sb2, sb3 = st.columns(3)
                        with sb1:
                            if st.button("Pakai", key=f"btn_apply_calc_non::{calc_scope}", use_container_width=True):
                                if calc_value is None:
                                    st.warning("Rumus belum valid.")
                                else:
                                    st.session_state[apply_target_map[target_label]] = format_float_compact(calc_value)
                                    st.rerun()
                        with sb2:
                            if st.button("Simpan", key=f"btn_save_kg_calc_non::{calc_scope}", use_container_width=True):
                                if calc_value is None:
                                    st.warning("Rumus belum valid.")
                                else:
                                    history = list(st.session_state.get(calc_history_key, []))
                                    history.insert(
                                        0,
                                        {
                                            "at": ts_str(),
                                            "expr": str(st.session_state.get(calc_expr_key, "")).strip(),
                                            "result": format_float_compact(calc_value),
                                        },
                                    )
                                    st.session_state[calc_history_key] = history[:25]
                                    st.rerun()
                        with sb3:
                            if st.button("Hapus", key=f"btn_clear_kg_calc_non::{calc_scope}", use_container_width=True):
                                st.session_state[calc_history_key] = []
                                st.rerun()

                        history = st.session_state.get(calc_history_key, [])
                        if history:
                            st.caption("Riwayat (5 terbaru)")
                            for item in history[:5]:
                                expr_hist = str(item.get("expr", "")).strip() or "-"
                                res = str(item.get("result", "")).strip() or "-"
                                st.caption(f"{expr_hist} = {res} kg")
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
            st.markdown("### 2-2. Tempat buang pillow")
            st.caption("Tiap laporan: tambah 1 log (jam + O/X). Gunakan catatan hanya jika perlu.")
            seed_tempat_non_key = f"seed_tempat_non::{team_id}::{work_date}"
            if not st.session_state.get(seed_tempat_non_key, False):
                loaded_tempat_rows_non = loaded_details.get("tempat_buang_rows", [])
                if isinstance(loaded_tempat_rows_non, list) and loaded_tempat_rows_non:
                    existing_tempat_local = False
                    for i in range(20):
                        if str(st.session_state.get(f"tb_jam_non_{i}", "")).strip() or str(
                            st.session_state.get(f"tb_status_non_{i}", "")
                        ).strip() or str(st.session_state.get(f"tb_cat_non_{i}", "")).strip():
                            existing_tempat_local = True
                            break
                    if not existing_tempat_local:
                        st.session_state["tempat_buang_rows_non"] = min(20, max(1, len(loaded_tempat_rows_non)))
                        for idx, row in enumerate(loaded_tempat_rows_non[:20]):
                            st.session_state[f"tb_jam_non_{idx}"] = str(row.get("jam", ""))
                            st.session_state[f"tb_status_non_{idx}"] = str(row.get("status", ""))
                            st.session_state[f"tb_cat_non_{idx}"] = str(row.get("catatan", ""))
                elif str(loaded_details.get("tempat_buang_siap", "")).strip() or str(loaded_details.get("tempat_buang_check_time", "")).strip():
                    if not str(st.session_state.get("tb_jam_non_0", "")).strip() and not str(st.session_state.get("tb_status_non_0", "")).strip():
                        st.session_state["tempat_buang_rows_non"] = 1
                        st.session_state["tb_jam_non_0"] = str(loaded_details.get("tempat_buang_check_time", ""))
                        st.session_state["tb_status_non_0"] = str(loaded_details.get("tempat_buang_siap", ""))
                        st.session_state["tb_cat_non_0"] = ""
                st.session_state[seed_tempat_non_key] = True

            tb1, tb2, tb3 = st.columns([2, 2, 6])
            with tb1:
                if st.button("+ Tambah", key="btn_add_tempat_non"):
                    st.session_state["tempat_buang_rows_non"] = min(20, int(st.session_state.get("tempat_buang_rows_non", 1)) + 1)
            with tb2:
                if st.button("- Hapus", key="btn_del_tempat_non"):
                    drop_last_row_from_session(
                        "tempat_buang_rows_non",
                        ["tb_jam_non_", "tb_status_non_", "tb_cat_non_"],
                        min_rows=1,
                    )
            with tb3:
                pass

            row_count_tempat_non = ensure_row_count_from_session(
                "tempat_buang_rows_non",
                ["tb_jam_non_", "tb_status_non_", "tb_cat_non_"],
                min_rows=1,
                max_rows=20,
            )
            tempat_buang_rows: List[Dict[str, Any]] = []
            tempat_preview_non: List[str] = []
            last_tempat_status_non = ""
            last_tempat_jam_non = ""
            for idx in range(int(row_count_tempat_non)):
                tbc0, tbc1, tbc2, tbc3 = st.columns([1, 2, 2, 4])
                no_key = f"tb_no_non_{idx}"
                if not str(st.session_state.get(no_key, "")).strip():
                    st.session_state[no_key] = str(idx + 1)
                no_tb = tbc0.text_input("No", key=no_key, max_chars=3)
                jam_tb = tbc1.text_input("Jam cek", placeholder="12:55", key=f"tb_jam_non_{idx}")
                opts_tb = ["O", "X"]
                status_raw = str(st.session_state.get(f"tb_status_non_{idx}", "") or "")
                status_idx = opts_tb.index(status_raw) if status_raw in opts_tb else 0
                status_tb = tbc2.radio(
                    "Status",
                    options=opts_tb,
                    index=status_idx,
                    horizontal=True,
                    key=f"tb_status_non_{idx}",
                )
                cat_tb = tbc3.text_input("Catatan (opsional)", placeholder="opsional", key=f"tb_cat_non_{idx}")
                if jam_tb.strip() or status_tb.strip() or cat_tb.strip():
                    tempat_buang_rows.append({"jam": jam_tb, "status": status_tb, "catatan": cat_tb})
                    no_prefix = f"[{no_tb.strip()}] " if no_tb.strip() else ""
                    line = f"- {no_prefix}{jam_tb.strip() or 'Jam belum diisi'} | {status_tb or '-'}"
                    if cat_tb.strip():
                        line += f" | {cat_tb.strip()}"
                    tempat_preview_non.append(line)
                    if status_tb in {"O", "X"}:
                        last_tempat_status_non = status_tb
                    if jam_tb.strip():
                        last_tempat_jam_non = jam_tb.strip()
            st.caption("Ringkasan 2-2 tempat buang")
            st.code("\n".join(tempat_preview_non) if tempat_preview_non else "- Belum ada log 2-2")
            tempat_buang_siap = last_tempat_status_non or str(loaded_details.get("tempat_buang_siap", "")).strip()
            tempat_buang_check_time = last_tempat_jam_non or str(loaded_details.get("tempat_buang_check_time", "")).strip()
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "tempat_buang", "2-2 Tempat buang")
            st.markdown("### 3-1. Status Giling")
            st.caption("Isi `1` untuk mulai, `2` untuk selesai. Selain itu bisa tulis manual.")
            mode_giling = st.radio(
                "Cara isi status giling",
                options=["List baris", "Tulis manual"],
                horizontal=True,
                key="mode_giling_non",
            )
            giling_total_resep_auto = str(
                loaded_details.get("giling_total_resep_auto", loaded_details.get("giling_total_pack_auto", ""))
            )
            if mode_giling == "List baris":
                g1, g2, g3 = st.columns([2, 2, 6])
                with g1:
                    if st.button("+ Tambah", key="btn_add_giling"):
                        st.session_state["giling_rows_non"] = min(20, int(st.session_state.get("giling_rows_non", 1)) + 1)
                with g2:
                    if st.button("- Hapus", key="btn_del_giling"):
                        drop_last_row_from_session(
                            "giling_rows_non",
                            ["gil_jam_non_", "gil_isi_non_", "gil_kg_non_", "gil_cat_non_"],
                            min_rows=1,
                        )
                with g3:
                    pass

                row_count_giling = ensure_row_count_from_session(
                    "giling_rows_non",
                    ["gil_jam_non_", "gil_isi_non_", "gil_kg_non_", "gil_cat_non_"],
                    min_rows=1,
                    max_rows=20,
                )
                giling_lines: List[str] = []
                giling_resep_sum = 0.0
                giling_resep_invalid = 0
                giling_next_batch = 1
                giling_open_batch: Optional[int] = None
                for idx in range(int(row_count_giling)):
                    gc0, gc1, gc2, gc3, gc4 = st.columns([1, 2, 3, 2, 3])
                    no_key = f"gil_no_non_{idx}"
                    if not str(st.session_state.get(no_key, "")).strip():
                        st.session_state[no_key] = str(idx + 1)
                    no_gil = gc0.text_input("No", key=no_key, max_chars=3)
                    jam = gc1.text_input("Jam giling", placeholder="11:30", key=f"gil_jam_non_{idx}")
                    giling_status_key = f"gil_isi_non_{idx}"
                    isi = gc2.text_input(
                        "Status giling",
                        placeholder="1 / 2 / manual",
                        key=giling_status_key,
                    )
                    resep = gc3.text_input("Resep giling", placeholder="75", key=f"gil_kg_non_{idx}")
                    cat = gc4.text_input("Catatan giling", placeholder="opsional", key=f"gil_cat_non_{idx}")
                    status_text, giling_next_batch, giling_open_batch = normalize_giling_status_input(
                        isi,
                        giling_next_batch,
                        giling_open_batch,
                    )
                    if isi.strip() and status_text and status_text != isi.strip():
                        gc2.caption(f"Otomatis: {status_text}")
                    resep_val = parse_optional_float(resep)
                    if resep.strip():
                        if resep_val is None:
                            giling_resep_invalid += 1
                        elif resep_val >= 0:
                            giling_resep_sum += resep_val
                    if jam.strip() or status_text.strip() or resep.strip():
                        status_part = status_text.strip()
                        if resep.strip():
                            status_part = f"{status_part} = {resep.strip()} resep".strip() if status_part else f"{resep.strip()} resep"
                        prefix = f"[{no_gil.strip()}] " if no_gil.strip() else ""
                        giling_lines.append(f"- {prefix}{jam.strip()} {status_part}".strip())
                    if cat.strip():
                        giling_lines.append(f"({cat.strip()})")
                status_giling = "\n".join(giling_lines).strip()
                st.caption("Preview status giling")
                st.code(status_giling or "-")
                giling_total_resep_auto = format_float_compact(giling_resep_sum)
                st.text_input("Total resep giling (otomatis)", value=giling_total_resep_auto, disabled=True)
                if giling_resep_invalid > 0:
                    st.warning(f"Ada {giling_resep_invalid} nilai resep giling yang bukan angka, tidak dihitung.")
                extra_giling = st.text_area(
                    "Tambahan manual status giling (opsional)",
                    value="",
                    key="giling_extra_non",
                )
                if extra_giling.strip():
                    status_giling = (status_giling + "\n" + extra_giling.strip()).strip()
            else:
                status_giling = st.text_area(
                    "Status giling",
                    value=loaded_details.get("status_giling", ""),
                    placeholder="- 11:30 mulai giling batch 1\n- 11:50 selesai giling batch 1",
                )
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "giling", "3-1 Giling")
            total_giling = st.text_input(
                "Total Giling (berapa resep)",
                value=str(loaded_details.get("total_giling", "")),
                placeholder="contoh: 15",
            )
            st.markdown("#### Log delay giling (tiap laporan)")
            seed_delay_non_key = f"seed_delay_non::{team_id}::{work_date}"
            if not st.session_state.get(seed_delay_non_key, False):
                loaded_delay_rows = loaded_details.get("giling_delay_rows", [])
                if isinstance(loaded_delay_rows, list) and loaded_delay_rows:
                    existing_delay_local = False
                    for i in range(20):
                        if str(st.session_state.get(f"delay_jam_non_{i}", "")).strip() or str(
                            st.session_state.get(f"delay_status_non_{i}", "")
                        ).strip() or str(st.session_state.get(f"delay_detail_non_{i}", "")).strip():
                            existing_delay_local = True
                            break
                    if not existing_delay_local:
                        st.session_state["giling_delay_rows_non"] = min(20, max(1, len(loaded_delay_rows)))
                        for idx, row in enumerate(loaded_delay_rows[:20]):
                            st.session_state[f"delay_jam_non_{idx}"] = str(row.get("jam", ""))
                            st.session_state[f"delay_status_non_{idx}"] = str(row.get("status", ""))
                            st.session_state[f"delay_detail_non_{idx}"] = str(row.get("detail", ""))
                elif str(loaded_details.get("giling_delay_lama", "")).strip() or str(loaded_details.get("giling_delay_detail", "")).strip():
                    if not str(st.session_state.get("delay_jam_non_0", "")).strip() and not str(
                        st.session_state.get("delay_status_non_0", "")
                    ).strip() and not str(st.session_state.get("delay_detail_non_0", "")).strip():
                        st.session_state["giling_delay_rows_non"] = 1
                        st.session_state["delay_jam_non_0"] = ""
                        st.session_state["delay_status_non_0"] = str(loaded_details.get("giling_delay_lama", ""))
                        st.session_state["delay_detail_non_0"] = str(loaded_details.get("giling_delay_detail", ""))
                st.session_state[seed_delay_non_key] = True

            gd1, gd2, gd3 = st.columns([2, 2, 6])
            with gd1:
                if st.button("+ Tambah", key="btn_add_delay_giling"):
                    st.session_state["giling_delay_rows_non"] = min(20, int(st.session_state.get("giling_delay_rows_non", 1)) + 1)
            with gd2:
                if st.button("- Hapus", key="btn_del_delay_giling"):
                    drop_last_row_from_session(
                        "giling_delay_rows_non",
                        ["delay_jam_non_", "delay_status_non_", "delay_detail_non_"],
                        min_rows=1,
                    )
            with gd3:
                pass

            row_count_delay = ensure_row_count_from_session(
                "giling_delay_rows_non",
                ["delay_jam_non_", "delay_status_non_", "delay_detail_non_"],
                min_rows=1,
                max_rows=20,
            )
            giling_delay_rows: List[Dict[str, Any]] = []
            giling_delay_lines_preview: List[str] = []
            has_delay_o = False
            has_delay_x = False
            for idx in range(int(row_count_delay)):
                dgc0, dgc1, dgc2, dgc3 = st.columns([1, 2, 1, 5])
                no_key = f"delay_no_non_{idx}"
                if not str(st.session_state.get(no_key, "")).strip():
                    st.session_state[no_key] = str(idx + 1)
                no_delay = dgc0.text_input("No", key=no_key, max_chars=3)
                jam_delay = dgc1.text_input(
                    "Jam delay",
                    placeholder="14:10",
                    key=f"delay_jam_non_{idx}",
                )
                delay_opts = ["X", "O"]
                delay_raw = str(st.session_state.get(f"delay_status_non_{idx}", "") or "")
                delay_idx = delay_opts.index(delay_raw) if delay_raw in delay_opts else delay_opts.index("X")
                status_delay = dgc2.selectbox(
                    "Ada delay?",
                    options=delay_opts,
                    index=delay_idx,
                    format_func=lambda x: "Ya" if x == "O" else "Tidak",
                    key=f"delay_status_non_{idx}",
                    label_visibility="visible",
                )
                if status_delay == "O":
                    detail_delay = dgc3.text_input(
                        "Penyebab delay",
                        placeholder="contoh: antrian packing 40 menit",
                        key=f"delay_detail_non_{idx}",
                    )
                else:
                    detail_delay = dgc3.text_input(
                        "Penyebab delay (opsional)",
                        placeholder="kosongkan jika tidak ada delay",
                        key=f"delay_detail_non_{idx}",
                    )
                if jam_delay.strip() or status_delay.strip() or detail_delay.strip():
                    giling_delay_rows.append(
                        {
                            "jam": jam_delay,
                            "status": status_delay,
                            "detail": detail_delay,
                        }
                    )
                    if status_delay == "O":
                        giling_delay_lines_preview.append(
                            f"- {(f'[{no_delay.strip()}] ' if no_delay.strip() else '')}{jam_delay.strip() or 'Jam belum diisi'} | Delay ada | {detail_delay.strip() or 'Penyebab belum diisi'}"
                        )
                    else:
                        giling_delay_lines_preview.append(
                            f"- {(f'[{no_delay.strip()}] ' if no_delay.strip() else '')}{jam_delay.strip() or 'Jam belum diisi'} | Tidak ada delay"
                        )
                    if status_delay == "O":
                        has_delay_o = True
                    if status_delay == "X":
                        has_delay_x = True
            giling_delay_lama = "O" if has_delay_o else ("X" if has_delay_x else "")
            giling_delay_detail = "\n".join(giling_delay_lines_preview).strip()
            st.caption("Ringkasan log delay giling")
            st.code(giling_delay_detail or "- Belum ada log delay")
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "giling_delay", "log delay giling")
            st.markdown("### 3-2. Status vacum")
            mode_vacum = st.radio(
                "Cara isi status vacum",
                options=["List baris", "Tulis manual"],
                horizontal=True,
                key="mode_vacum_non",
            )
            vacum_total_pack_auto = str(loaded_details.get("vacum_total_pack_auto", ""))
            if mode_vacum == "List baris":
                v1, v2, v3 = st.columns([2, 2, 6])
                with v1:
                    if st.button("+ Tambah", key="btn_add_vacum"):
                        st.session_state["vacum_rows_non"] = min(20, int(st.session_state.get("vacum_rows_non", 1)) + 1)
                with v2:
                    if st.button("- Hapus", key="btn_del_vacum"):
                        drop_last_row_from_session(
                            "vacum_rows_non",
                            ["vac_jam_non_", "vac_isi_non_", "vac_kg_non_", "vac_cat_non_"],
                            min_rows=1,
                        )
                with v3:
                    pass

                row_count_vacum = ensure_row_count_from_session(
                    "vacum_rows_non",
                    ["vac_jam_non_", "vac_isi_non_", "vac_kg_non_", "vac_cat_non_"],
                    min_rows=1,
                    max_rows=20,
                )
                vacum_lines: List[str] = []
                vacum_pack_sum = 0.0
                vacum_pack_invalid = 0
                for idx in range(int(row_count_vacum)):
                    vc0, vc1, vc2, vc3, vc4 = st.columns([1, 2, 3, 2, 3])
                    no_key = f"vac_no_non_{idx}"
                    if not str(st.session_state.get(no_key, "")).strip():
                        st.session_state[no_key] = str(idx + 1)
                    no_vac = vc0.text_input("No", key=no_key, max_chars=3)
                    jam = vc1.text_input("Jam vacum", placeholder="12:00", key=f"vac_jam_non_{idx}")
                    isi = vc2.text_input("Status vacum", placeholder="mulai vacum batch 1", key=f"vac_isi_non_{idx}")
                    pack = vc3.text_input("Pack vacum", placeholder="75", key=f"vac_kg_non_{idx}")
                    cat = vc4.text_input("Catatan vacum", placeholder="opsional", key=f"vac_cat_non_{idx}")
                    pack_val = parse_optional_float(pack)
                    if pack.strip():
                        if pack_val is None:
                            vacum_pack_invalid += 1
                        elif pack_val >= 0:
                            vacum_pack_sum += pack_val
                    if jam.strip() or isi.strip() or pack.strip():
                        status_part = isi.strip()
                        if pack.strip():
                            status_part = f"{status_part} = {pack.strip()}pack".strip()
                        prefix = f"[{no_vac.strip()}] " if no_vac.strip() else ""
                        vacum_lines.append(f"- {prefix}{jam.strip()} {status_part}".strip())
                    if cat.strip():
                        vacum_lines.append(f"({cat.strip()})")
                status_vacum = "\n".join(vacum_lines).strip()
                st.caption("Preview status vacum")
                st.code(status_vacum or "-")
                vacum_total_pack_auto = format_float_compact(vacum_pack_sum)
                st.text_input("Total pack vacum (otomatis)", value=vacum_total_pack_auto, disabled=True)
                if vacum_pack_invalid > 0:
                    st.warning(f"Ada {vacum_pack_invalid} nilai pack vacum yang bukan angka, tidak dihitung.")
                extra_vacum = st.text_area(
                    "Tambahan manual status vacum (opsional)",
                    value="",
                    key="vacum_extra_non",
                )
                if extra_vacum.strip():
                    status_vacum = (status_vacum + "\n" + extra_vacum.strip()).strip()
            else:
                status_vacum = st.text_area(
                    "Status vacum",
                    value=loaded_details.get("status_vacum", ""),
                    placeholder="12:00 mulai vacum batch 1\n12:30 selesai vacum batch 1",
                )
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "vacum_status", "3-2 Vacum")

            total_hasil_vakum = st.text_input(
                "Total vakum diproses (pack)",
                value=str(loaded_details.get("total_hasil_vakum", "")),
                placeholder="contoh: 88",
            )

            st.markdown("#### Barang ada masalah vacum/pillow")
            loaded_vacum_defect_rows = loaded_details.get("vacum_defect_rows", [])
            defect_type_manual = "Lainnya (isi manual)"
            defect_type_options = ["", "Seal bocor", "Pack pecah", "Basi", "Vakum kurang rapat", defect_type_manual]
            if isinstance(loaded_vacum_defect_rows, list) and loaded_vacum_defect_rows:
                existing_defect_local = False
                for i in range(20):
                    if str(st.session_state.get(f"vac_defect_type_non_{i}", "")).strip() or str(
                        st.session_state.get(f"vac_defect_jenis_non_{i}", "")
                    ).strip() or str(
                        st.session_state.get(f"vac_defect_qty_non_{i}", "")
                    ).strip() or str(
                        st.session_state.get(f"vac_defect_note_non_{i}", "")
                    ).strip():
                        existing_defect_local = True
                        break
                if not existing_defect_local:
                    st.session_state["vacum_defect_rows_non"] = min(20, max(1, len(loaded_vacum_defect_rows)))
                    for idx, row in enumerate(loaded_vacum_defect_rows[:20]):
                        seeded_jenis = str(row.get("jenis", "")).strip()
                        if seeded_jenis in defect_type_options:
                            st.session_state[f"vac_defect_type_non_{idx}"] = seeded_jenis
                            st.session_state[f"vac_defect_jenis_non_{idx}"] = ""
                        elif seeded_jenis:
                            st.session_state[f"vac_defect_type_non_{idx}"] = defect_type_manual
                            st.session_state[f"vac_defect_jenis_non_{idx}"] = seeded_jenis
                        else:
                            st.session_state[f"vac_defect_type_non_{idx}"] = ""
                            st.session_state[f"vac_defect_jenis_non_{idx}"] = ""
                        st.session_state[f"vac_defect_qty_non_{idx}"] = str(row.get("jumlah_pack", ""))
                        st.session_state[f"vac_defect_note_non_{idx}"] = str(row.get("catatan", ""))
            elif str(loaded_details.get("jenis_defect_vacum", "")).strip() or str(
                loaded_details.get("total_vacum_defect_pack", "")
            ).strip():
                # Backward compatibility for old single text fields.
                if not str(st.session_state.get("vac_defect_jenis_non_0", "")).strip() and not str(
                    st.session_state.get("vac_defect_qty_non_0", "")
                ).strip():
                    st.session_state["vacum_defect_rows_non"] = 1
                    seeded_old = str(loaded_details.get("jenis_defect_vacum", "")).strip()
                    st.session_state["vac_defect_type_non_0"] = defect_type_manual if seeded_old else ""
                    st.session_state["vac_defect_jenis_non_0"] = seeded_old
                    st.session_state["vac_defect_qty_non_0"] = str(loaded_details.get("total_vacum_defect_pack", ""))
                    st.session_state["vac_defect_note_non_0"] = ""

            vd1, vd2, vd3 = st.columns([2, 2, 6])
            with vd1:
                if st.button("+ Tambah", key="btn_add_vac_defect"):
                    st.session_state["vacum_defect_rows_non"] = min(20, int(st.session_state.get("vacum_defect_rows_non", 1)) + 1)
            with vd2:
                if st.button("- Hapus", key="btn_del_vac_defect"):
                    drop_last_row_from_session(
                        "vacum_defect_rows_non",
                        ["vac_defect_type_non_", "vac_defect_jenis_non_", "vac_defect_qty_non_", "vac_defect_note_non_"],
                        min_rows=1,
                    )
            with vd3:
                pass

            row_count_vac_defect = ensure_row_count_from_session(
                "vacum_defect_rows_non",
                ["vac_defect_type_non_", "vac_defect_jenis_non_", "vac_defect_qty_non_", "vac_defect_note_non_"],
                min_rows=1,
                max_rows=20,
            )
            vacum_defect_rows: List[Dict[str, Any]] = []
            vacum_defect_lines: List[str] = []
            sum_vacum_defect = 0.0
            for idx in range(int(row_count_vac_defect)):
                vdc1, vdc2, vdc3 = st.columns([3, 2, 3])
                raw_type = str(st.session_state.get(f"vac_defect_type_non_{idx}", "") or "")
                type_idx = defect_type_options.index(raw_type) if raw_type in defect_type_options else 0
                jenis_type = vdc1.selectbox(
                    "Jenis masalah",
                    options=defect_type_options,
                    index=type_idx,
                    format_func=lambda x: "Pilih barang bermasalah" if x == "" else x,
                    key=f"vac_defect_type_non_{idx}",
                )
                jenis_manual = ""
                if jenis_type == defect_type_manual:
                    jenis_manual = vdc1.text_input(
                        "Jenis manual",
                        placeholder="contoh: seal miring / kontaminasi",
                        key=f"vac_defect_jenis_non_{idx}",
                    )
                jumlah_pack = vdc2.text_input(
                    "Jumlah (pack)",
                    placeholder="contoh: 2",
                    key=f"vac_defect_qty_non_{idx}",
                )
                catatan_defect = vdc3.text_input(
                    "Catatan (opsional)",
                    placeholder="opsional",
                    key=f"vac_defect_note_non_{idx}",
                )
                jenis = jenis_manual.strip() if jenis_type == defect_type_manual else jenis_type.strip()
                qty_val = parse_optional_float(jumlah_pack)
                if jenis or jumlah_pack.strip() or catatan_defect.strip():
                    vacum_defect_rows.append({"jenis": jenis, "jumlah_pack": jumlah_pack, "catatan": catatan_defect})
                if jenis.strip() and qty_val is not None and qty_val >= 0:
                    line = f"{jenis.strip()} {format_float_compact(qty_val)} pack"
                    if catatan_defect.strip():
                        line = f"{line} ({catatan_defect.strip()})"
                    vacum_defect_lines.append(line)
                    sum_vacum_defect += qty_val

            jenis_defect_vacum = ", ".join(vacum_defect_lines)
            total_vacum_defect_pack = format_float_compact(sum_vacum_defect)
            total_vacum_num = parse_optional_float(total_hasil_vakum)
            total_vacum_ok_pack = ""
            if total_vacum_num is not None:
                total_vacum_ok_pack = format_float_compact(total_vacum_num - sum_vacum_defect)
            st.caption("Ringkasan barang ada masalah vacum")
            st.code("\n".join([f"- {x}" for x in vacum_defect_lines]) if vacum_defect_lines else "- Belum ada masalah tercatat")
            st.text_input("Total vacum bermasalah (otomatis, pack)", value=total_vacum_defect_pack, disabled=True)
            st.text_input("Total vakum normal (otomatis, pack)", value=total_vacum_ok_pack, disabled=True)
            st.caption("Rumus: total normal = total diproses - total bermasalah")
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "vacum_defect", "barang ada masalah vacum")

            st.markdown("#### Log operasional vacum (tiap laporan)")
            st.caption("Tujuan: catat kapan mesin vacuum stop/istirahat (jam mulai + jam selesai) per laporan.")
            loaded_vacum_ops_rows = loaded_details.get("vacum_ops_rows", [])
            if isinstance(loaded_vacum_ops_rows, list) and loaded_vacum_ops_rows:
                existing_ops_local = False
                for i in range(20):
                    if str(st.session_state.get(f"vac_ops_stop_start_non_{i}", "")).strip() or str(
                        st.session_state.get(f"vac_ops_stop_end_non_{i}", "")
                    ).strip() or str(st.session_state.get(f"vac_ops_mesin_non_{i}", "")).strip() or str(
                        st.session_state.get(f"vac_ops_pic_non_{i}", "")
                    ).strip():
                        existing_ops_local = True
                        break
                if not existing_ops_local:
                    st.session_state["vacum_ops_rows_non"] = min(20, max(1, len(loaded_vacum_ops_rows)))
                    for idx, row in enumerate(loaded_vacum_ops_rows[:20]):
                        stop_start = str(row.get("stop_start", row.get("jam", "")))
                        stop_end = str(row.get("stop_end", ""))
                        st.session_state[f"vac_ops_stop_start_non_{idx}"] = stop_start
                        st.session_state[f"vac_ops_stop_end_non_{idx}"] = stop_end
                        st.session_state[f"vac_ops_mesin_non_{idx}"] = str(row.get("mesin_status", ""))
                        st.session_state[f"vac_ops_pic_non_{idx}"] = str(row.get("pic_cek", ""))
            elif str(loaded_details.get("mesin_vacum_istirahat", "")).strip() or str(loaded_details.get("nama_pic_cek", "")).strip():
                if not str(st.session_state.get("vac_ops_stop_start_non_0", "")).strip() and not str(
                    st.session_state.get("vac_ops_mesin_non_0", "")
                ).strip():
                    st.session_state["vacum_ops_rows_non"] = 1
                    st.session_state["vac_ops_stop_start_non_0"] = ""
                    st.session_state["vac_ops_stop_end_non_0"] = ""
                    st.session_state["vac_ops_mesin_non_0"] = str(loaded_details.get("mesin_vacum_istirahat", ""))
                    st.session_state["vac_ops_pic_non_0"] = str(loaded_details.get("nama_pic_cek", ""))

            vo1, vo2, vo3 = st.columns([2, 2, 6])
            with vo1:
                if st.button("+ Tambah", key="btn_add_vac_ops"):
                    st.session_state["vacum_ops_rows_non"] = min(20, int(st.session_state.get("vacum_ops_rows_non", 1)) + 1)
            with vo2:
                if st.button("- Hapus", key="btn_del_vac_ops"):
                    drop_last_row_from_session(
                        "vacum_ops_rows_non",
                        [
                            "vac_ops_stop_start_non_",
                            "vac_ops_stop_end_non_",
                            "vac_ops_mesin_non_",
                            "vac_ops_pic_non_",
                        ],
                        min_rows=1,
                    )
            with vo3:
                pass

            row_count_vac_ops = ensure_row_count_from_session(
                "vacum_ops_rows_non",
                [
                    "vac_ops_stop_start_non_",
                    "vac_ops_stop_end_non_",
                    "vac_ops_mesin_non_",
                    "vac_ops_pic_non_",
                ],
                min_rows=1,
                max_rows=20,
            )
            vacum_ops_rows: List[Dict[str, Any]] = []
            has_mesin_o = False
            has_mesin_x = False
            pic_candidates: List[str] = []
            for idx in range(int(row_count_vac_ops)):
                voc1, voc2, voc3, voc4 = st.columns([2, 2, 2, 2])
                stop_start = voc1.text_input("Jam mulai stop", placeholder="1000", key=f"vac_ops_stop_start_non_{idx}")
                stop_end = voc2.text_input("Jam selesai stop", placeholder="1030", key=f"vac_ops_stop_end_non_{idx}")
                ops_opts = ["", "O", "X"]
                mesin_raw = str(st.session_state.get(f"vac_ops_mesin_non_{idx}", "") or "")
                mesin_idx = ops_opts.index(mesin_raw) if mesin_raw in ops_opts else 0
                mesin_status = voc3.selectbox(
                    "Mesin stop / istirahat",
                    options=ops_opts,
                    index=mesin_idx,
                    format_func=lambda x: "Pilih" if x == "" else x,
                    key=f"vac_ops_mesin_non_{idx}",
                )
                pic_cek = voc4.text_input("PIC cek", placeholder="nama PIC", key=f"vac_ops_pic_non_{idx}")
                if (
                    stop_start.strip()
                    or stop_end.strip()
                    or mesin_status.strip()
                    or pic_cek.strip()
                ):
                    vacum_ops_rows.append(
                        {
                            "jam": stop_start,
                            "stop_start": stop_start,
                            "stop_end": stop_end,
                            "mesin_status": mesin_status,
                            "pic_cek": pic_cek,
                        }
                    )
                if mesin_status == "O":
                    has_mesin_o = True
                if mesin_status == "X":
                    has_mesin_x = True
                if pic_cek.strip():
                    pic_candidates.append(pic_cek.strip())

            vacum_antrian_lama = ""
            vacum_antrian_detail = ""
            mesin_vacum_istirahat = "O" if has_mesin_o else ("X" if has_mesin_x else "")
            mesin_vacum_istirahat_detail = ""
            sudah_dikirim_semua = ""
            nama_pic_cek = ", ".join(dict.fromkeys(pic_candidates))
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "vacum_ops", "log operasional vacum")
            st.markdown("### 4. Total barang ada masalah")
            masalah_total_barang = st.text_area(
                "Tulis masalah barang (contoh: basi, kemasan sobek, dll)",
                value=loaded_details.get("masalah_total_barang", ""),
                placeholder="- Basi 2kg\n- Kemasan/pojac sobek 10 pack",
            )
            st.markdown("### 5. Total barang dikirim ke packing (atau press)")
            seed_handover_non_key = f"seed_handover_non::{team_id}::{work_date}"
            if not st.session_state.get(seed_handover_non_key, False):
                loaded_handover_rows = loaded_details.get("handover_rows", [])
                if isinstance(loaded_handover_rows, list) and loaded_handover_rows:
                    existing_handover_local = False
                    for i in range(30):
                        for prefix in [
                            "handover_jam_non_",
                            "handover_kirim_non_",
                            "handover_terima_non_",
                            "handover_tl_non_",
                            "handover_pic_non_",
                            "handover_alasan_non_",
                        ]:
                            if str(st.session_state.get(f"{prefix}{i}", "")).strip():
                                existing_handover_local = True
                                break
                        if existing_handover_local:
                            break
                    if not existing_handover_local:
                        st.session_state["handover_rows_non"] = min(30, max(1, len(loaded_handover_rows)))
                        for idx, row in enumerate(loaded_handover_rows[:30]):
                            st.session_state[f"handover_jam_non_{idx}"] = str(row.get("jam", ""))
                            st.session_state[f"handover_kirim_non_{idx}"] = str(row.get("kirim_pack", ""))
                            st.session_state[f"handover_terima_non_{idx}"] = str(row.get("terima_pack", ""))
                            st.session_state[f"handover_tl_non_{idx}"] = str(row.get("tl_packing", ""))
                            st.session_state[f"handover_pic_non_{idx}"] = str(row.get("tl_kupas", row.get("pic_packing", "")))
                            st.session_state[f"handover_alasan_non_{idx}"] = str(row.get("alasan_selisih", ""))
                st.session_state[seed_handover_non_key] = True

            h1, h2, h3 = st.columns([2, 2, 6])
            with h1:
                if st.button("+ Tambah", key="btn_add_handover"):
                    st.session_state["handover_rows_non"] = min(30, int(st.session_state.get("handover_rows_non", 1)) + 1)
            with h2:
                if st.button("- Hapus", key="btn_del_handover"):
                    drop_last_row_from_session(
                        "handover_rows_non",
                        [
                            "handover_jam_non_",
                            "handover_kirim_non_",
                            "handover_terima_non_",
                            "handover_tl_non_",
                            "handover_pic_non_",
                            "handover_alasan_non_",
                        ],
                        min_rows=1,
                    )
            with h3:
                pass

            row_count_handover = ensure_row_count_from_session(
                "handover_rows_non",
                [
                    "handover_jam_non_",
                    "handover_kirim_non_",
                    "handover_terima_non_",
                    "handover_tl_non_",
                    "handover_pic_non_",
                    "handover_alasan_non_",
                ],
                min_rows=1,
                max_rows=30,
            )

            handover_rows: List[Dict[str, Any]] = []
            sum_kirim = 0.0
            sum_terima = 0.0
            for idx in range(int(row_count_handover)):
                hc1, hc2, hc3, hc4, hc5, hc6 = st.columns([2, 2, 2, 2, 2, 3])
                jam = hc1.text_input("Jam handover", placeholder="14:20", key=f"handover_jam_non_{idx}")
                kirim = hc2.text_input("Kirim", placeholder="120", key=f"handover_kirim_non_{idx}")
                terima = hc3.text_input("Terima", placeholder="116", key=f"handover_terima_non_{idx}")
                tl = hc4.text_input("TL Packing", placeholder="Ibu Rina", key=f"handover_tl_non_{idx}")
                pic = hc5.text_input("TL Kupas", placeholder="TL kupas", key=f"handover_pic_non_{idx}")
                alasan = hc6.text_input(
                    "Alasan selisih",
                    placeholder="isi jika ada selisih",
                    key=f"handover_alasan_non_{idx}",
                )

                kirim_val = parse_optional_float(kirim)
                terima_val = parse_optional_float(terima)
                selisih_text = ""
                if kirim_val is not None and terima_val is not None:
                    sum_kirim += kirim_val
                    sum_terima += terima_val
                    selisih_text = format_float_compact(kirim_val - terima_val)
                handover_rows.append(
                    {
                        "jam": jam,
                        "kirim_pack": kirim,
                        "terima_pack": terima,
                        "selisih_pack": selisih_text,
                        "tl_packing": tl,
                        "tl_kupas": pic,
                        "pic_packing": pic,
                        "alasan_selisih": alasan,
                    }
                )

            total_dikirim_packing = format_float_compact(sum_kirim)
            total_diterima_packing = format_float_compact(sum_terima)
            selisih_total = sum_kirim - sum_terima
            selisih_display = format_float_compact(selisih_total)
            status_handover_packing = "O" if abs(selisih_total) <= 0.001 else "X"
            st.caption(
                f"Ringkasan handover: kirim {total_dikirim_packing} | terima {total_diterima_packing} | selisih {selisih_display} | status cocok {status_handover_packing}"
            )
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "handover", "5. Handover packing")
            photo_scope = f"{team_id}::{work_date}"
            handover_photo_path_key = f"handover_photo_path_non::{photo_scope}"
            handover_photo_name_key = f"handover_photo_name_non::{photo_scope}"
            handover_photo_sig_key = f"handover_photo_sig_non::{photo_scope}"
            if handover_photo_path_key not in st.session_state:
                seeded_path = str(loaded_details.get("handover_photo_path", "")).strip()
                seeded_name = str(loaded_details.get("handover_photo_name", "")).strip()
                if seeded_path and Path(seeded_path).exists():
                    st.session_state[handover_photo_path_key] = seeded_path
                    st.session_state[handover_photo_name_key] = seeded_name or Path(seeded_path).name
                else:
                    st.session_state[handover_photo_path_key] = ""
                    st.session_state[handover_photo_name_key] = ""
            if handover_photo_sig_key not in st.session_state:
                st.session_state[handover_photo_sig_key] = ""

            st.markdown("#### Bukti handover packing (foto)")
            photo_mode_handover = st.radio(
                "Cara ambil foto handover",
                options=["Kamera langsung", "Upload file"],
                horizontal=True,
                key=f"handover_photo_mode_non::{photo_scope}",
            )
            photo_input_handover = None
            if photo_mode_handover == "Kamera langsung":
                photo_input_handover = st.camera_input(
                    "Ambil foto handover sekarang",
                    key=f"handover_photo_camera_non::{photo_scope}::{st.session_state.get('active_idempotency_key', '')}",
                )
            else:
                photo_input_handover = st.file_uploader(
                    "Upload foto bukti serah-terima (jpg/png/webp)",
                    type=["jpg", "jpeg", "png", "webp"],
                    key=f"handover_photo_upload_non::{photo_scope}::{st.session_state.get('active_idempotency_key', '')}",
                )
            save_uploaded_image_to_evidence(
                photo_input_handover,
                team_id=team_id,
                work_date=work_date,
                prefix="handover",
                sig_key=handover_photo_sig_key,
                path_key=handover_photo_path_key,
                name_key=handover_photo_name_key,
            )

            saved_photo_path = str(st.session_state.get(handover_photo_path_key, "")).strip()
            saved_photo_name = str(st.session_state.get(handover_photo_name_key, "")).strip()
            if saved_photo_path and Path(saved_photo_path).exists():
                st.caption(f"Foto tersimpan: {saved_photo_name or Path(saved_photo_path).name}")
                st.image(saved_photo_path, caption="Preview bukti handover", use_container_width=True)
                if st.button("Hapus foto bukti", key=f"btn_del_handover_photo_non::{photo_scope}"):
                    st.session_state[handover_photo_path_key] = ""
                    st.session_state[handover_photo_name_key] = ""
                    st.session_state[handover_photo_sig_key] = ""
                    st.rerun()
            else:
                st.caption("Belum ada foto bukti handover.")

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
                "defrost_rows": defrost_rows,
                "defrost_total_pack_auto": defrost_total_pack_auto,
                "total_beku": total_beku,
                "total_beku_kg": total_beku_kg,
                "total_fresh_kg": total_fresh_kg,
                "total_buang_kg": total_buang_kg,
                "total_akhir_kg": total_akhir_kg,
                "kg_calc_history": st.session_state.get(calc_history_key, []),
                "tempat_buang_siap": tempat_buang_siap,
                "tempat_buang_check_time": tempat_buang_check_time,
                "tempat_buang_rows": tempat_buang_rows,
                "status_giling": status_giling,
                "giling_total_resep_auto": giling_total_resep_auto,
                "giling_total_pack_auto": giling_total_resep_auto,
                "total_giling": total_giling,
                "giling_delay_lama": giling_delay_lama,
                "giling_delay_detail": giling_delay_detail,
                "giling_delay_rows": giling_delay_rows,
                "status_vacum": status_vacum,
                "vacum_total_pack_auto": vacum_total_pack_auto,
                "total_hasil_vakum": total_hasil_vakum,
                "total_vacum_defect_pack": total_vacum_defect_pack,
                "total_vacum_ok_pack": total_vacum_ok_pack,
                "jenis_defect_vacum": jenis_defect_vacum,
                "vacum_defect_rows": vacum_defect_rows,
                "vacum_antrian_lama": vacum_antrian_lama,
                "vacum_antrian_detail": vacum_antrian_detail,
                "mesin_vacum_istirahat": mesin_vacum_istirahat,
                "mesin_vacum_istirahat_detail": mesin_vacum_istirahat_detail,
                "sudah_dikirim_semua": sudah_dikirim_semua,
                "nama_pic_cek": nama_pic_cek,
                "vacum_ops_rows": vacum_ops_rows,
                "masalah_total_barang": masalah_total_barang,
                "total_dikirim_packing": total_dikirim_packing,
                "total_diterima_packing": total_diterima_packing,
                "selisih_handover_packing": selisih_display,
                "status_handover_packing": status_handover_packing,
                "handover_rows": handover_rows,
                "handover_photo_path": saved_photo_path,
                "handover_photo_name": saved_photo_name,
                "catatan": catatan,
                "total_change_reason": total_change_reason,
                "tl_confirm_phrase": tl_confirm_phrase,
            }
        else:
            st.markdown("### 1. Produk")
            produk = st.text_input("1. Produk", value=loaded_details.get("produk", ""))
            isi_steril = st.text_input("1-2. Jumlah isi barang untuk steril", value=loaded_details.get("isi_steril", ""))
            nama_petugas_raw = st.text_area(
                "1-3. Nama Petugas (satu baris satu nama)",
                value=loaded_details.get("nama_petugas_raw", ""),
                placeholder="Linda\nLian",
            )
            st.caption("Tambah manual: 1 baris = 1 nama petugas.")
            nama_petugas_list = parse_name_lines(nama_petugas_raw)
            timer_ada = st.selectbox(
                "1-4. Timer ada ?",
                options=["O", "X"],
                index=0 if loaded_details.get("timer_ada", "O") == "O" else 1,
            )
            st.markdown("#### Info Steril (khusus)")
            alat = ""
            petugas_steril = st.text_input(
                "Petugas steril (wajib)",
                value=loaded_details.get("petugas_steril", ""),
                placeholder="Nama petugas steril",
            )
            rencana_steril = st.text_input("Rencana jam steril berapa lama", value=loaded_details.get("rencana_steril", ""))

            st.markdown("### 2-1. Status defrost")
            mode_defrost_st = st.radio(
                "Cara isi status defrost",
                options=["List baris", "Tulis manual"],
                horizontal=True,
                key="mode_defrost_st",
            )
            defrost_total_pack_auto = str(loaded_details.get("defrost_total_pack_auto", ""))
            if mode_defrost_st == "List baris":
                seed_defrost_st_key = f"seed_defrost_st::{team_id}::{work_date}"
                if not st.session_state.get(seed_defrost_st_key, False):
                    loaded_defrost_rows_st = loaded_details.get("defrost_rows", [])
                    if isinstance(loaded_defrost_rows_st, list) and loaded_defrost_rows_st:
                        existing_defrost_local_st = False
                        for i in range(20):
                            if str(st.session_state.get(f"def_jam_st_{i}", "")).strip() or str(
                                st.session_state.get(f"def_isi_st_{i}", "")
                            ).strip() or str(st.session_state.get(f"def_kg_st_{i}", "")).strip() or str(
                                st.session_state.get(f"def_cat_st_{i}", "")
                            ).strip():
                                existing_defrost_local_st = True
                                break
                        if not existing_defrost_local_st:
                            st.session_state["defrost_rows_st"] = min(20, max(1, len(loaded_defrost_rows_st)))
                            for idx, row in enumerate(loaded_defrost_rows_st[:20]):
                                st.session_state[f"def_no_st_{idx}"] = str(row.get("no", idx + 1))
                                st.session_state[f"def_jam_st_{idx}"] = str(row.get("jam", ""))
                                st.session_state[f"def_isi_st_{idx}"] = str(row.get("status", ""))
                                st.session_state[f"def_kg_st_{idx}"] = str(row.get("pack", ""))
                                st.session_state[f"def_cat_st_{idx}"] = str(row.get("catatan", ""))
                    st.session_state[seed_defrost_st_key] = True
                d1, d2, d3 = st.columns([2, 2, 6])
                with d1:
                    if st.button("+ Tambah", key="btn_add_defrost_st"):
                        st.session_state["defrost_rows_st"] = min(20, int(st.session_state.get("defrost_rows_st", 1)) + 1)
                with d2:
                    if st.button("- Hapus", key="btn_del_defrost_st"):
                        drop_last_row_from_session(
                            "defrost_rows_st",
                            ["def_jam_st_", "def_isi_st_", "def_kg_st_", "def_cat_st_"],
                            min_rows=1,
                        )
                with d3:
                    pass

                row_count = ensure_row_count_from_session(
                    "defrost_rows_st",
                    ["def_jam_st_", "def_isi_st_", "def_kg_st_", "def_cat_st_"],
                    min_rows=1,
                    max_rows=20,
                )
                defrost_lines: List[str] = []
                defrost_rows: List[Dict[str, Any]] = []
                defrost_pack_sum = 0.0
                defrost_pack_invalid = 0
                for idx in range(int(row_count)):
                    dc0, dc1, dc2, dc3, dc4 = st.columns([1, 2, 3, 2, 3])
                    no_key = f"def_no_st_{idx}"
                    if not str(st.session_state.get(no_key, "")).strip():
                        st.session_state[no_key] = str(idx + 1)
                    no = dc0.text_input("No", key=no_key, max_chars=3)
                    jam = dc1.text_input("Jam", placeholder="12:55", key=f"def_jam_st_{idx}")
                    isi = dc2.text_input("Status", placeholder="BB fresh", key=f"def_isi_st_{idx}")
                    pack = dc3.text_input("Pack", placeholder="75", key=f"def_kg_st_{idx}")
                    cat = dc4.text_input("Catatan", placeholder="sudah termasuk campuran", key=f"def_cat_st_{idx}")
                    pack_val = parse_optional_float(pack)
                    if pack.strip():
                        if pack_val is None:
                            defrost_pack_invalid += 1
                        elif pack_val >= 0:
                            defrost_pack_sum += pack_val
                    if jam.strip() or isi.strip() or pack.strip():
                        defrost_rows.append({"no": no, "jam": jam, "status": isi, "pack": pack, "catatan": cat})
                        status_part = isi.strip()
                        if pack.strip():
                            status_part = f"{status_part} = {pack.strip()}pack".strip()
                        prefix = f"[{no.strip()}] " if no.strip() else ""
                        defrost_lines.append(f"- {prefix}{jam.strip()} {status_part}".strip())
                    if cat.strip():
                        defrost_lines.append(f"({cat.strip()})")
                status_defrost = "\n".join(defrost_lines).strip()
                st.caption("Preview status defrost")
                st.code(status_defrost or "-")
                defrost_total_pack_auto = format_float_compact(defrost_pack_sum)
                st.text_input("Total pack defrost (otomatis)", value=defrost_total_pack_auto, disabled=True)
                if defrost_pack_invalid > 0:
                    st.warning(f"Ada {defrost_pack_invalid} nilai pack yang bukan angka, tidak dihitung.")
                extra_manual_st = st.text_area("Tambahan manual (opsional)", value="", key="def_extra_st")
                if extra_manual_st.strip():
                    status_defrost = (status_defrost + "\n" + extra_manual_st.strip()).strip()
            else:
                defrost_rows = []
                status_defrost = st.text_area(
                    "Status defrost (Kalau sudah habis dipakai, tulis habis)",
                    value=loaded_details.get("status_defrost", ""),
                    placeholder="- 20:00 BB fresh = 90kg\n- 20:20 BB fresh = 40kg",
                )
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "defrost", "2-1 Defrost")

            total_beku = st.text_input("Total barang beku diambil", value=loaded_details.get("total_beku", ""))
            total_beku_kg_key_st = f"total_beku_kg_st::{team_id}::{work_date}"
            total_fresh_kg_key_st = f"total_fresh_kg_st::{team_id}::{work_date}"
            total_buang_kg_key_st = f"total_buang_kg_st::{team_id}::{work_date}"
            total_akhir_kg_key_st = f"total_akhir_kg_st::{team_id}::{work_date}"
            for key_name, seed in [
                (total_beku_kg_key_st, str(loaded_details.get("total_beku_kg", ""))),
                (total_fresh_kg_key_st, str(loaded_details.get("total_fresh_kg", ""))),
                (total_buang_kg_key_st, str(loaded_details.get("total_buang_kg", ""))),
                (total_akhir_kg_key_st, str(loaded_details.get("total_akhir_kg", ""))),
            ]:
                if key_name not in st.session_state:
                    st.session_state[key_name] = seed

            total_beku_kg = st.text_input("Total barang beku (kg, angka untuk validasi)", key=total_beku_kg_key_st, placeholder="contoh: 75")
            total_fresh_kg = st.text_input("Total bb fresh dipakai (kg)", key=total_fresh_kg_key_st, placeholder="contoh: 225")
            total_buang_kg = st.text_input("Total bb dibuang (kg)", key=total_buang_kg_key_st, placeholder="contoh: 0")
            beku_val_st = parse_optional_float(total_beku_kg)
            fresh_val_st = parse_optional_float(total_fresh_kg)
            buang_val_st = parse_optional_float(total_buang_kg)
            total_akhir_auto_st = ""
            if None not in (beku_val_st, fresh_val_st, buang_val_st):
                total_akhir_auto_st = format_float_compact((beku_val_st or 0.0) + (fresh_val_st or 0.0) - (buang_val_st or 0.0))
            st.session_state[total_akhir_kg_key_st] = total_akhir_auto_st
            st.text_input("Total akhir (kg, otomatis)", value=total_akhir_auto_st, disabled=True)
            total_akhir_kg = total_akhir_auto_st

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

            st.markdown("### 2-2. Tempat buang pillow")
            st.caption("Tiap laporan: tambah 1 log (jam + O/X). Gunakan catatan hanya jika perlu.")
            seed_tempat_st_key = f"seed_tempat_st::{team_id}::{work_date}"
            if not st.session_state.get(seed_tempat_st_key, False):
                loaded_tempat_rows_st = loaded_details.get("tempat_buang_rows", [])
                if isinstance(loaded_tempat_rows_st, list) and loaded_tempat_rows_st:
                    existing_tempat_local_st = False
                    for i in range(20):
                        if str(st.session_state.get(f"tb_jam_st_{i}", "")).strip() or str(
                            st.session_state.get(f"tb_status_st_{i}", "")
                        ).strip() or str(st.session_state.get(f"tb_cat_st_{i}", "")).strip():
                            existing_tempat_local_st = True
                            break
                    if not existing_tempat_local_st:
                        st.session_state["tempat_buang_rows_st"] = min(20, max(1, len(loaded_tempat_rows_st)))
                        for idx, row in enumerate(loaded_tempat_rows_st[:20]):
                            st.session_state[f"tb_jam_st_{idx}"] = str(row.get("jam", ""))
                            st.session_state[f"tb_status_st_{idx}"] = str(row.get("status", ""))
                            st.session_state[f"tb_cat_st_{idx}"] = str(row.get("catatan", ""))
                elif str(loaded_details.get("tempat_buang_siap", "")).strip() or str(loaded_details.get("tempat_buang_check_time", "")).strip():
                    if not str(st.session_state.get("tb_jam_st_0", "")).strip() and not str(st.session_state.get("tb_status_st_0", "")).strip():
                        st.session_state["tempat_buang_rows_st"] = 1
                        st.session_state["tb_jam_st_0"] = str(loaded_details.get("tempat_buang_check_time", ""))
                        st.session_state["tb_status_st_0"] = str(loaded_details.get("tempat_buang_siap", ""))
                        st.session_state["tb_cat_st_0"] = ""
                st.session_state[seed_tempat_st_key] = True

            tb1, tb2, tb3 = st.columns([2, 2, 6])
            with tb1:
                if st.button("+ Tambah", key="btn_add_tempat_st"):
                    st.session_state["tempat_buang_rows_st"] = min(20, int(st.session_state.get("tempat_buang_rows_st", 1)) + 1)
            with tb2:
                if st.button("- Hapus", key="btn_del_tempat_st"):
                    drop_last_row_from_session(
                        "tempat_buang_rows_st",
                        ["tb_jam_st_", "tb_status_st_", "tb_cat_st_"],
                        min_rows=1,
                    )
            with tb3:
                pass

            row_count_tempat_st = ensure_row_count_from_session(
                "tempat_buang_rows_st",
                ["tb_jam_st_", "tb_status_st_", "tb_cat_st_"],
                min_rows=1,
                max_rows=20,
            )
            tempat_buang_rows: List[Dict[str, Any]] = []
            tempat_preview_st: List[str] = []
            last_tempat_status_st = ""
            last_tempat_jam_st = ""
            for idx in range(int(row_count_tempat_st)):
                tbc0, tbc1, tbc2, tbc3 = st.columns([1, 2, 2, 4])
                no_key = f"tb_no_st_{idx}"
                if not str(st.session_state.get(no_key, "")).strip():
                    st.session_state[no_key] = str(idx + 1)
                no_tb = tbc0.text_input("No", key=no_key, max_chars=3)
                jam_tb = tbc1.text_input("Jam cek", placeholder="12:55", key=f"tb_jam_st_{idx}")
                opts_tb = ["O", "X"]
                status_raw = str(st.session_state.get(f"tb_status_st_{idx}", "") or "")
                status_idx = opts_tb.index(status_raw) if status_raw in opts_tb else 0
                status_tb = tbc2.radio(
                    "Status",
                    options=opts_tb,
                    index=status_idx,
                    horizontal=True,
                    key=f"tb_status_st_{idx}",
                )
                cat_tb = tbc3.text_input("Catatan (opsional)", placeholder="opsional", key=f"tb_cat_st_{idx}")
                if jam_tb.strip() or status_tb.strip() or cat_tb.strip():
                    tempat_buang_rows.append({"jam": jam_tb, "status": status_tb, "catatan": cat_tb})
                    no_prefix = f"[{no_tb.strip()}] " if no_tb.strip() else ""
                    line = f"- {no_prefix}{jam_tb.strip() or 'Jam belum diisi'} | {status_tb or '-'}"
                    if cat_tb.strip():
                        line += f" | {cat_tb.strip()}"
                    tempat_preview_st.append(line)
                    if status_tb in {"O", "X"}:
                        last_tempat_status_st = status_tb
                    if jam_tb.strip():
                        last_tempat_jam_st = jam_tb.strip()
            st.caption("Ringkasan 2-2 tempat buang")
            st.code("\n".join(tempat_preview_st) if tempat_preview_st else "- Belum ada log 2-2")
            tempat_buang_siap = last_tempat_status_st or str(loaded_details.get("tempat_buang_siap", "")).strip()
            tempat_buang_check_time = last_tempat_jam_st or str(loaded_details.get("tempat_buang_check_time", "")).strip()
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "tempat_buang", "2-2 Tempat buang")

            st.markdown("### 3-1. Status Giling")
            st.caption("Isi `1` untuk mulai, `2` untuk selesai. Selain itu bisa tulis manual.")
            mode_giling_st = st.radio(
                "Cara isi status giling",
                options=["List baris", "Tulis manual"],
                horizontal=True,
                key="mode_giling_st",
            )
            giling_total_resep_auto = str(
                loaded_details.get("giling_total_resep_auto", loaded_details.get("giling_total_pack_auto", ""))
            )
            if mode_giling_st == "List baris":
                g1, g2, g3 = st.columns([2, 2, 6])
                with g1:
                    if st.button("+ Tambah", key="btn_add_giling_st"):
                        st.session_state["giling_rows_st"] = min(20, int(st.session_state.get("giling_rows_st", 1)) + 1)
                with g2:
                    if st.button("- Hapus", key="btn_del_giling_st"):
                        drop_last_row_from_session(
                            "giling_rows_st",
                            ["gil_jam_st_", "gil_isi_st_", "gil_kg_st_", "gil_cat_st_"],
                            min_rows=1,
                        )
                with g3:
                    pass

                row_count_giling_st = ensure_row_count_from_session(
                    "giling_rows_st",
                    ["gil_jam_st_", "gil_isi_st_", "gil_kg_st_", "gil_cat_st_"],
                    min_rows=1,
                    max_rows=20,
                )
                giling_lines_st: List[str] = []
                giling_resep_sum = 0.0
                giling_resep_invalid = 0
                giling_next_batch_st = 1
                giling_open_batch_st: Optional[int] = None
                for idx in range(int(row_count_giling_st)):
                    gc1, gc2, gc3, gc4 = st.columns([2, 3, 2, 3])
                    jam = gc1.text_input("Jam giling", placeholder="20:30", key=f"gil_jam_st_{idx}")
                    giling_status_key_st = f"gil_isi_st_{idx}"
                    isi = gc2.text_input("Status giling", placeholder="1 / 2 / manual", key=giling_status_key_st)
                    resep = gc3.text_input("Resep giling", placeholder="18", key=f"gil_kg_st_{idx}")
                    cat = gc4.text_input("Catatan giling", placeholder="opsional", key=f"gil_cat_st_{idx}")
                    status_text, giling_next_batch_st, giling_open_batch_st = normalize_giling_status_input(
                        isi,
                        giling_next_batch_st,
                        giling_open_batch_st,
                    )
                    if isi.strip() and status_text and status_text != isi.strip():
                        gc2.caption(f"Otomatis: {status_text}")
                    resep_val = parse_optional_float(resep)
                    if resep.strip():
                        if resep_val is None:
                            giling_resep_invalid += 1
                        elif resep_val >= 0:
                            giling_resep_sum += resep_val
                    if jam.strip() or status_text.strip() or resep.strip():
                        status_part = status_text.strip()
                        if resep.strip():
                            status_part = f"{status_part} = {resep.strip()} resep".strip() if status_part else f"{resep.strip()} resep"
                        giling_lines_st.append(f"- {jam.strip()} {status_part}".strip())
                    if cat.strip():
                        giling_lines_st.append(f"({cat.strip()})")
                status_giling = "\n".join(giling_lines_st).strip()
                st.caption("Preview status giling")
                st.code(status_giling or "-")
                giling_total_resep_auto = format_float_compact(giling_resep_sum)
                st.text_input("Total resep giling (otomatis)", value=giling_total_resep_auto, disabled=True)
                if giling_resep_invalid > 0:
                    st.warning(f"Ada {giling_resep_invalid} nilai resep giling yang bukan angka, tidak dihitung.")
                extra_giling_st = st.text_area("Tambahan manual status giling (opsional)", value="", key="giling_extra_st")
                if extra_giling_st.strip():
                    status_giling = (status_giling + "\n" + extra_giling_st.strip()).strip()
            else:
                status_giling = st.text_area(
                    "Status giling",
                    value=loaded_details.get("status_giling", ""),
                    placeholder="- 20:30 mulai giling batch 1\n- 21:35 giling batch 3",
                )
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "giling", "3-1 Giling")
            total_giling = st.text_input("Total Giling (berapa resep)", value=str(loaded_details.get("total_giling", "")), placeholder="contoh: 15")

            st.markdown("### 3-2. Status Steril / Status Gas")
            st.caption("Isi log per batch: jam steril + batch + jumlah panci.")
            seed_steril_rows_key = f"seed_steril_rows::{team_id}::{work_date}"
            if not st.session_state.get(seed_steril_rows_key, False):
                loaded_steril_rows = loaded_details.get("steril_rows", [])
                if isinstance(loaded_steril_rows, list) and loaded_steril_rows:
                    existing_steril_local = False
                    for i in range(30):
                        if str(st.session_state.get(f"steril_jam_st_{i}", "")).strip() or str(
                            st.session_state.get(f"steril_batch_st_{i}", "")
                        ).strip() or str(st.session_state.get(f"steril_panci_st_{i}", "")).strip():
                            existing_steril_local = True
                            break
                    if not existing_steril_local:
                        st.session_state["steril_rows_st"] = min(30, max(1, len(loaded_steril_rows)))
                        for idx, row in enumerate(loaded_steril_rows[:30]):
                            st.session_state[f"steril_no_st_{idx}"] = str(row.get("no", idx + 1))
                            st.session_state[f"steril_jam_st_{idx}"] = str(row.get("jam", ""))
                            st.session_state[f"steril_batch_st_{idx}"] = str(row.get("batch", ""))
                            st.session_state[f"steril_panci_st_{idx}"] = str(row.get("panci", ""))
                            st.session_state[f"steril_cat_st_{idx}"] = str(row.get("catatan", ""))
                st.session_state[seed_steril_rows_key] = True

            s1, s2, s3 = st.columns([2, 2, 6])
            with s1:
                if st.button("+ Tambah", key="btn_add_steril_row_st"):
                    st.session_state["steril_rows_st"] = min(30, int(st.session_state.get("steril_rows_st", 1)) + 1)
            with s2:
                if st.button("- Hapus", key="btn_del_steril_row_st"):
                    drop_last_row_from_session(
                        "steril_rows_st",
                        ["steril_no_st_", "steril_jam_st_", "steril_batch_st_", "steril_panci_st_", "steril_cat_st_"],
                        min_rows=1,
                    )
            with s3:
                pass

            row_count_steril = ensure_row_count_from_session(
                "steril_rows_st",
                ["steril_no_st_", "steril_jam_st_", "steril_batch_st_", "steril_panci_st_", "steril_cat_st_"],
                min_rows=1,
                max_rows=30,
            )
            steril_rows: List[Dict[str, Any]] = []
            steril_lines: List[str] = []
            steril_start_map: Dict[str, str] = {}
            sum_steril_panci = 0.0
            invalid_steril_panci = 0
            for idx in range(int(row_count_steril)):
                sc0, sc1, sc2, sc3, sc4 = st.columns([1, 2, 2, 2, 3])
                no_key = f"steril_no_st_{idx}"
                if not str(st.session_state.get(no_key, "")).strip():
                    st.session_state[no_key] = str(idx + 1)
                no = sc0.text_input("No", key=no_key, max_chars=3)
                jam = sc1.text_input("Jam steril", placeholder="08:20", key=f"steril_jam_st_{idx}")
                batch = sc2.text_input("Batch", placeholder="1", key=f"steril_batch_st_{idx}")
                panci = sc3.text_input("Panci", placeholder="17", key=f"steril_panci_st_{idx}")
                cat = sc4.text_input("Catatan", placeholder="opsional", key=f"steril_cat_st_{idx}")
                panci_val = parse_optional_float(panci)
                if panci.strip():
                    if panci_val is None:
                        invalid_steril_panci += 1
                    elif panci_val >= 0:
                        sum_steril_panci += panci_val
                if jam.strip() or batch.strip() or panci.strip() or cat.strip():
                    steril_rows.append({"no": no, "jam": jam, "batch": batch, "panci": panci, "catatan": cat})
                    line = f"- {(f'[{no.strip()}] ' if no.strip() else '')}{jam.strip() or '-'} steril batch {batch.strip() or '-'} ({panci.strip() or '-'} panci)"
                    if cat.strip():
                        line += f" | {cat.strip()}"
                    steril_lines.append(line)
                    if batch.strip() and jam.strip() and is_valid_hhmm(jam):
                        steril_start_map.setdefault(batch.strip(), jam.strip())
            status_steril = "\n".join(steril_lines).strip()
            st.caption("Preview status steril/gas")
            st.code(status_steril or "- Belum ada log steril")
            steril_total_panci_auto = format_float_compact(sum_steril_panci)
            st.text_input("Total panci steril (otomatis)", value=steril_total_panci_auto, disabled=True)
            if invalid_steril_panci > 0:
                st.warning(f"Ada {invalid_steril_panci} nilai panci steril yang bukan angka, tidak dihitung.")
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "steril_status", "3-2 Steril/Gas")

            st.markdown("#### Total Steril (rincian)")
            seed_total_breakdown_key = f"seed_steril_total::{team_id}::{work_date}"
            if not st.session_state.get(seed_total_breakdown_key, False):
                loaded_total_breakdown = loaded_details.get("total_steril_breakdown_rows", [])
                if isinstance(loaded_total_breakdown, list) and loaded_total_breakdown:
                    existing_total_local = False
                    for i in range(15):
                        if str(st.session_state.get(f"steril_total_qty_st_{i}", "")).strip() or str(
                            st.session_state.get(f"steril_total_kg_st_{i}", "")
                        ).strip():
                            existing_total_local = True
                            break
                    if not existing_total_local:
                        st.session_state["steril_total_rows_st"] = min(15, max(1, len(loaded_total_breakdown)))
                        for idx, row in enumerate(loaded_total_breakdown[:15]):
                            st.session_state[f"steril_total_no_st_{idx}"] = str(row.get("no", idx + 1))
                            st.session_state[f"steril_total_qty_st_{idx}"] = str(row.get("qty_panci", ""))
                            st.session_state[f"steril_total_kg_st_{idx}"] = str(row.get("berat_kg", ""))
                st.session_state[seed_total_breakdown_key] = True
            tb1, tb2, tb3 = st.columns([2, 2, 6])
            with tb1:
                if st.button("+ Tambah", key="btn_add_steril_total_st"):
                    st.session_state["steril_total_rows_st"] = min(15, int(st.session_state.get("steril_total_rows_st", 1)) + 1)
            with tb2:
                if st.button("- Hapus", key="btn_del_steril_total_st"):
                    drop_last_row_from_session(
                        "steril_total_rows_st",
                        ["steril_total_no_st_", "steril_total_qty_st_", "steril_total_kg_st_"],
                        min_rows=1,
                    )
            with tb3:
                pass

            row_count_total = ensure_row_count_from_session(
                "steril_total_rows_st",
                ["steril_total_no_st_", "steril_total_qty_st_", "steril_total_kg_st_"],
                min_rows=1,
                max_rows=15,
            )
            total_steril_breakdown_rows: List[Dict[str, Any]] = []
            total_steril_breakdown_lines: List[str] = []
            for idx in range(int(row_count_total)):
                tc0, tc1, tc2 = st.columns([1, 2, 2])
                no_key = f"steril_total_no_st_{idx}"
                if not str(st.session_state.get(no_key, "")).strip():
                    st.session_state[no_key] = str(idx + 1)
                no = tc0.text_input("No", key=no_key, max_chars=3)
                qty = tc1.text_input("Jumlah panci", placeholder="72", key=f"steril_total_qty_st_{idx}")
                berat = tc2.text_input("Berat/panci (kg)", placeholder="5", key=f"steril_total_kg_st_{idx}")
                if qty.strip() or berat.strip():
                    total_steril_breakdown_rows.append({"no": no, "qty_panci": qty, "berat_kg": berat})
                    if qty.strip() and berat.strip():
                        total_steril_breakdown_lines.append(f"- {qty.strip()} panci @{berat.strip()}kg")
                    elif qty.strip():
                        total_steril_breakdown_lines.append(f"- {qty.strip()} panci")
            total_steril_breakdown_text = "\n".join(total_steril_breakdown_lines).strip()
            st.code(total_steril_breakdown_text or "- Belum ada rincian total steril")

            st.markdown("### 3-2-1. Jam steril sudah sesuai?")
            target_minutes_key_st = f"steril_target_minutes_st::{team_id}::{work_date}"
            if target_minutes_key_st not in st.session_state:
                st.session_state[target_minutes_key_st] = int(parse_optional_int(loaded_details.get("steril_target_minutes"), 75))
            steril_target_minutes = int(
                st.number_input(
                    "Target steril (menit)",
                    min_value=1,
                    max_value=300,
                    step=5,
                    key=target_minutes_key_st,
                )
            )
            seed_steril_check_key = f"seed_steril_check::{team_id}::{work_date}"
            if not st.session_state.get(seed_steril_check_key, False):
                loaded_steril_check_rows = loaded_details.get("steril_check_rows", [])
                if isinstance(loaded_steril_check_rows, list) and loaded_steril_check_rows:
                    existing_check_local = False
                    for i in range(30):
                        if str(st.session_state.get(f"steril_check_batch_st_{i}", "")).strip() or str(
                            st.session_state.get(f"steril_check_actual_st_{i}", "")
                        ).strip():
                            existing_check_local = True
                            break
                    if not existing_check_local:
                        st.session_state["steril_check_rows_st"] = min(30, max(1, len(loaded_steril_check_rows)))
                        for idx, row in enumerate(loaded_steril_check_rows[:30]):
                            st.session_state[f"steril_check_no_st_{idx}"] = str(row.get("no", idx + 1))
                            st.session_state[f"steril_check_batch_st_{idx}"] = str(row.get("batch", ""))
                            st.session_state[f"steril_check_actual_st_{idx}"] = str(row.get("jam_actual", ""))
                st.session_state[seed_steril_check_key] = True
            c1, c2, c3 = st.columns([2, 2, 6])
            with c1:
                if st.button("+ Tambah", key="btn_add_steril_check_st"):
                    st.session_state["steril_check_rows_st"] = min(30, int(st.session_state.get("steril_check_rows_st", 1)) + 1)
            with c2:
                if st.button("- Hapus", key="btn_del_steril_check_st"):
                    drop_last_row_from_session(
                        "steril_check_rows_st",
                        ["steril_check_no_st_", "steril_check_batch_st_", "steril_check_actual_st_"],
                        min_rows=1,
                    )
            with c3:
                pass

            row_count_check = ensure_row_count_from_session(
                "steril_check_rows_st",
                ["steril_check_no_st_", "steril_check_batch_st_", "steril_check_actual_st_"],
                min_rows=1,
                max_rows=30,
            )
            steril_check_rows: List[Dict[str, Any]] = []
            steril_check_lines: List[str] = []
            for idx in range(int(row_count_check)):
                cc0, cc1, cc2, cc3, cc4 = st.columns([1, 2, 2, 2, 1])
                no_key = f"steril_check_no_st_{idx}"
                if not str(st.session_state.get(no_key, "")).strip():
                    st.session_state[no_key] = str(idx + 1)
                no = cc0.text_input("No", key=no_key, max_chars=3)
                batch = cc1.text_input("Batch", placeholder=str(idx + 1), key=f"steril_check_batch_st_{idx}")
                jam_target = hhmm_plus_minutes(steril_start_map.get(batch.strip(), ""), steril_target_minutes)
                target_view_key = f"steril_check_target_view_st_{idx}"
                st.session_state[target_view_key] = jam_target
                cc2.text_input("Jam target", key=target_view_key, disabled=True)
                jam_actual = cc3.text_input("Jam aktual", placeholder="09:35", key=f"steril_check_actual_st_{idx}")
                status_check = "-"
                diff_min = minutes_diff_hhmm(steril_start_map.get(batch.strip(), ""), jam_actual)
                if diff_min is not None:
                    status_check = "O" if diff_min >= steril_target_minutes else "X"
                status_view_key = f"steril_check_status_view_st_{idx}"
                st.session_state[status_view_key] = status_check
                cc4.text_input("Status", key=status_view_key, disabled=True)
                if batch.strip() or jam_actual.strip():
                    steril_check_rows.append(
                        {
                            "no": no,
                            "batch": batch,
                            "jam_target": jam_target,
                            "jam_actual": jam_actual,
                            "status": status_check,
                        }
                    )
                    if diff_min is not None:
                        steril_check_lines.append(
                            f"- {(f'[{no.strip()}] ' if no.strip() else '')}batch {batch.strip() or '-'} | target {jam_target or '-'} | aktual {jam_actual.strip() or '-'} | {status_check} ({diff_min} menit)"
                        )
                    else:
                        steril_check_lines.append(
                            f"- {(f'[{no.strip()}] ' if no.strip() else '')}batch {batch.strip() or '-'} | target {jam_target or '-'} | aktual {jam_actual.strip() or '-'} | -"
                        )
            st.caption("Ringkasan cek jam steril")
            st.code("\n".join(steril_check_lines) if steril_check_lines else "- Belum ada cek jam steril")
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "steril_check", "3-2-1 Cek jam steril")

            st.markdown("### 3-3. Status Coolbath (CB)")
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
            st.markdown("#### Jam produk masuk ke CB")
            seed_cb_rows_key = f"seed_cb_rows::{team_id}::{work_date}"
            if not st.session_state.get(seed_cb_rows_key, False):
                loaded_cb_rows = loaded_details.get("cb_rows", [])
                if isinstance(loaded_cb_rows, list) and loaded_cb_rows:
                    existing_cb_local = False
                    for i in range(30):
                        if str(st.session_state.get(f"cb_jam_st_{i}", "")).strip() or str(
                            st.session_state.get(f"cb_batch_st_{i}", "")
                        ).strip() or str(st.session_state.get(f"cb_panci_st_{i}", "")).strip():
                            existing_cb_local = True
                            break
                    if not existing_cb_local:
                        st.session_state["cb_rows_st"] = min(30, max(1, len(loaded_cb_rows)))
                        for idx, row in enumerate(loaded_cb_rows[:30]):
                            st.session_state[f"cb_no_st_{idx}"] = str(row.get("no", idx + 1))
                            st.session_state[f"cb_jam_st_{idx}"] = str(row.get("jam", ""))
                            st.session_state[f"cb_batch_st_{idx}"] = str(row.get("batch", ""))
                            st.session_state[f"cb_panci_st_{idx}"] = str(row.get("panci", ""))
                            st.session_state[f"cb_cat_st_{idx}"] = str(row.get("catatan", ""))
                st.session_state[seed_cb_rows_key] = True
            cb1, cb2, cb3 = st.columns([2, 2, 6])
            with cb1:
                if st.button("+ Tambah", key="btn_add_cb_row_st"):
                    st.session_state["cb_rows_st"] = min(30, int(st.session_state.get("cb_rows_st", 1)) + 1)
            with cb2:
                if st.button("- Hapus", key="btn_del_cb_row_st"):
                    drop_last_row_from_session(
                        "cb_rows_st",
                        ["cb_no_st_", "cb_jam_st_", "cb_batch_st_", "cb_panci_st_", "cb_cat_st_"],
                        min_rows=1,
                    )
            with cb3:
                pass

            row_count_cb = ensure_row_count_from_session(
                "cb_rows_st",
                ["cb_no_st_", "cb_jam_st_", "cb_batch_st_", "cb_panci_st_", "cb_cat_st_"],
                min_rows=1,
                max_rows=30,
            )
            cb_rows: List[Dict[str, Any]] = []
            cb_lines: List[str] = []
            for idx in range(int(row_count_cb)):
                cbc0, cbc1, cbc2, cbc3, cbc4 = st.columns([1, 2, 2, 2, 3])
                no_key = f"cb_no_st_{idx}"
                if not str(st.session_state.get(no_key, "")).strip():
                    st.session_state[no_key] = str(idx + 1)
                no = cbc0.text_input("No", key=no_key, max_chars=3)
                jam = cbc1.text_input("Jam masuk CB", placeholder="09:55", key=f"cb_jam_st_{idx}")
                batch = cbc2.text_input("Batch", placeholder="1", key=f"cb_batch_st_{idx}")
                panci = cbc3.text_input("Panci", placeholder="17", key=f"cb_panci_st_{idx}")
                cat = cbc4.text_input("Catatan", placeholder="opsional", key=f"cb_cat_st_{idx}")
                if jam.strip() or batch.strip() or panci.strip() or cat.strip():
                    cb_rows.append({"no": no, "jam": jam, "batch": batch, "panci": panci, "catatan": cat})
                    line = f"- {(f'[{no.strip()}] ' if no.strip() else '')}{jam.strip() or '-'} cb batch {batch.strip() or '-'} ({panci.strip() or '-'} panci)"
                    if cat.strip():
                        line += f" | {cat.strip()}"
                    cb_lines.append(line)
            st.caption("Ringkasan log CB")
            st.code("\n".join(cb_lines) if cb_lines else "- Belum ada log CB")
            st.markdown("#### Total Produk Steril")
            total_produk_steril = total_steril_breakdown_text
            st.code(total_produk_steril or "- Belum ada rincian total produk steril")
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
            render_section_checkpoint_ui(team_id, str(work_date), report_type, "coolbath", "3-3 Coolbath")
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
                "status_defrost": status_defrost,
                "defrost_rows": defrost_rows,
                "defrost_total_pack_auto": defrost_total_pack_auto,
                "total_beku": total_beku,
                "total_beku_kg": total_beku_kg,
                "total_fresh_kg": total_fresh_kg,
                "total_buang_kg": total_buang_kg,
                "total_akhir_kg": total_akhir_kg,
                "tempat_buang_siap": tempat_buang_siap,
                "tempat_buang_check_time": tempat_buang_check_time,
                "tempat_buang_rows": tempat_buang_rows,
                "status_giling": status_giling,
                "giling_total_resep_auto": giling_total_resep_auto,
                "giling_total_pack_auto": giling_total_resep_auto,
                "total_giling": total_giling,
                "status_steril": status_steril,
                "steril_rows": steril_rows,
                "total_panci_steril_auto": steril_total_panci_auto,
                "total_steril_breakdown_rows": total_steril_breakdown_rows,
                "total_steril_breakdown_text": total_steril_breakdown_text,
                "steril_target_minutes": str(steril_target_minutes),
                "steril_check_rows": steril_check_rows,
                "cb_rows": cb_rows,
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
        submitted = st.button("Kirim Laporan", type="primary")

    prev_state_snapshot = load_work_state(team_id.strip(), str(work_date))
    # Persist current working context after form render (for refresh recovery).
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

    def collect_validation_errors(prev_snapshot: Dict[str, Any]) -> List[str]:
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

        prev_total = parse_optional_float(prev_snapshot.get("details", {}).get("total_akhir_kg")) if prev_snapshot else None
        cur_total = parse_optional_float(details.get("total_akhir_kg"))
        if prev_total is not None and cur_total is not None and abs(prev_total - cur_total) > 0.001:
            if not str(details.get("total_change_reason", "")).strip():
                errs.append("Total berubah dari laporan sebelumnya. Isi alasan perubahan.")
            if str(details.get("tl_confirm_phrase", "")).strip().upper() != "SUDAH DIKONFIRMASI TL":
                errs.append("Total berubah. Isi konfirmasi TL persis: SUDAH DIKONFIRMASI TL")
        return errs

    if submitted:
        st.session_state["sticky_validation_active"] = True

    validation_errors: List[str] = []
    if st.session_state.get("sticky_validation_active", False):
        validation_errors = collect_validation_errors(prev_state_snapshot)
        st.session_state["sticky_validation_errors"] = validation_errors
    else:
        st.session_state["sticky_validation_errors"] = []

    for e in st.session_state.get("sticky_validation_errors", []):
        st.error(e)

    if submitted:
        if validation_errors:
            return
        st.session_state["sticky_validation_active"] = False
        st.session_state["sticky_validation_errors"] = []

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
            if result.telegram_error.strip():
                st.warning(f"Catatan Telegram: {result.telegram_error}")
            if result.telegram_message_ids:
                st.caption(f"Message IDs: {', '.join([str(x) for x in result.telegram_message_ids])}")
                set_root_message_ids(team_id.strip(), str(work_date), report_type, result.telegram_message_ids)
                edited_parts = 0
                for i, mid in enumerate(result.telegram_message_ids):
                    if i < len(existing_ids) and mid == existing_ids[i]:
                        edited_parts += 1
                if edited_parts > 0:
                    st.info("Update laporan mengedit pesan Telegram sebelumnya (ID sama), jadi tidak muncul bubble pesan baru.")
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
            for k in ["tempat_buang_siap_non", "tempat_buang_siap_st"]:
                if k in st.session_state:
                    st.session_state[k] = ""
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


if __name__ == "__main__":
    main()





