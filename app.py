import json
import os
import secrets
import uuid
import ast
import mimetypes
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
EVIDENCE_DIR = DATA_DIR / "evidence"

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
  background: #f2f5f1;
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
  border: 1px solid #d5dfd3;
  border-radius: 14px;
}
h1, h2, h3 {
  color: #1b2c20;
}
div[data-testid="stTextInput"] input,
div[data-testid="stTextArea"] textarea {
  background: #ffffff;
  border: 1px solid #c8d5c6;
  border-radius: 10px;
}
div[data-baseweb="select"] > div {
  background: #ffffff;
  border: 1px solid #c8d5c6;
  border-radius: 10px;
}
div.stButton > button {
  background: #2d8f5b;
  color: #ffffff;
  border: none;
  border-radius: 10px;
  font-weight: 700;
}
div.stButton > button:hover {
  background: #26784d;
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

    handover_rows = d.get("handover_rows", [])
    handover_lines: List[str] = []
    for idx, row in enumerate(handover_rows, start=1):
        jam = str(row.get("jam", "")).strip() or "-"
        kirim = str(row.get("kirim_pack", "")).strip() or "-"
        terima = str(row.get("terima_pack", "")).strip() or "-"
        selisih = str(row.get("selisih_pack", "")).strip() or "-"
        tl = str(row.get("tl_packing", "")).strip() or "-"
        pic = str(row.get("pic_packing", "")).strip() or "-"
        alasan = str(row.get("alasan_selisih", "")).strip()
        line = f"- [{idx}] {jam} | kirim {kirim} | terima {terima} | selisih {selisih} | TL {tl} | PIC {pic}"
        if alasan:
            line += f" | alasan: {alasan}"
        handover_lines.append(line)
    if not handover_lines:
        handover_lines = ["- Tidak ada baris handover"]

    giling_delay_rows = d.get("giling_delay_rows", [])
    giling_delay_lines: List[str] = []
    for idx, row in enumerate(giling_delay_rows, start=1):
        jam = str(row.get("jam", "")).strip() or "-"
        status = str(row.get("status", "")).strip() or "-"
        detail = str(row.get("detail", "")).strip() or "-"
        status_label = "Delay ada" if status == "O" else ("Tidak ada delay" if status == "X" else "-")
        giling_delay_lines.append(f"- [{idx}] {jam} | {status_label} | {detail}")
    if not giling_delay_lines:
        fallback_delay = str(d.get("giling_delay_detail", "")).strip()
        giling_delay_lines = [fallback_delay] if fallback_delay else ["- Tidak ada log delay"]

    vacum_ops_rows = d.get("vacum_ops_rows", [])
    vacum_ops_lines: List[str] = []
    for idx, row in enumerate(vacum_ops_rows, start=1):
        jam = str(row.get("jam", "")).strip() or "-"
        antrian = str(row.get("antrian_status", "")).strip() or "-"
        antrian_detail = str(row.get("antrian_detail", "")).strip() or "-"
        mesin = str(row.get("mesin_status", "")).strip() or "-"
        mesin_detail = str(row.get("mesin_detail", "")).strip() or "-"
        kirim = str(row.get("kirim_status", "")).strip() or "-"
        pic = str(row.get("pic_cek", "")).strip() or "-"
        vacum_ops_lines.append(
            f"- [{idx}] {jam} | antrian:{antrian} ({antrian_detail}) | mesin:{mesin} ({mesin_detail}) | kirim:{kirim} | PIC:{pic}"
        )
    if not vacum_ops_lines:
        vacum_ops_lines = ["- Tidak ada log operasional vacum"]

    return [
        "\n".join(
            [
                "1. PRODUK",
                f"- Jam kerja: {d.get('jam_kerja_mulai', '-')} - {d.get('jam_kerja_selesai', '-')}",
                f"- Produk: {d.get('produk', '-') or '-'}",
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
                f"-> Total pack defrost (otomatis): {d.get('defrost_total_pack_auto', '-') or '-'} pack",
                f"-> Total barang beku diambil: {d.get('total_beku', '-') or '-'}",
                f"-> Total bb fresh dipakai: {d.get('total_fresh_kg', 0)} kg",
                f"-> Total bb dibuang: {d.get('total_buang_kg', 0)} kg",
                f"-> Total: {d.get('total_akhir_kg', 0)} kg (jenis barang 1 + jenis barang 2)",
            ]
        ),
        "\n".join(
            [
                "2-2. Tempat buang pillow sudah siap dekat meja/rak, dan sudah dikosongkan kalau sudah penuh?",
                f"-> {d.get('tempat_buang_siap', '-') or '-'}",
            ]
        ),
        "\n".join(
            [
                "3-1. STATUS GILING",
                status_giling,
                f"-> Total pack giling (otomatis): {d.get('giling_total_pack_auto', '-') or '-'} pack",
                f"--> Total Giling: {d.get('total_giling', '-') or '-'} resep",
                f"-> Status delay giling (ringkas): {d.get('giling_delay_lama', '-') or '-'}",
                "-> Log delay giling:",
                *giling_delay_lines,
            ]
        ),
        "\n".join(
            [
                "3-2. STATUS VACUM",
                status_vacum,
                f"-> Total pack vacum (otomatis): {d.get('vacum_total_pack_auto', '-') or '-'} pack",
                f"-> Total vakum diproses: {d.get('total_hasil_vakum', '-') or '-'} pack",
                f"-> Total vakum bermasalah: {d.get('total_vacum_defect_pack', '-') or '-'} pack",
                f"-> Total vakum normal (setelah masalah): {d.get('total_vacum_ok_pack', '-') or '-'} pack",
                f"-> Jenis defect vacum/pillow: {d.get('jenis_defect_vacum', '-') or '-'}",
                f"-> Antrian vacum terlalu lama?: {d.get('vacum_antrian_lama', '-') or '-'}",
                f"-> Detail antrian vacum: {d.get('vacum_antrian_detail', '-') or '-'}",
                f"-> Mesin vacum sudah cukup istirahat?: {d.get('mesin_vacum_istirahat', '-') or '-'}",
                f"-> Detail kondisi mesin vacum: {d.get('mesin_vacum_istirahat_detail', '-') or '-'}",
                f"-> Sudah dikirim semua?: {d.get('sudah_dikirim_semua', '-') or '-'}",
                f"-> Petugas cek: {d.get('nama_pic_cek', '-') or '-'}",
                "-> Log operasional vacum:",
                *vacum_ops_lines,
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
                f"-> Dikirim kupas: {d.get('total_dikirim_packing', '-') or '-'} pack",
                f"-> Diterima packing: {d.get('total_diterima_packing', '-') or '-'} pack",
                f"-> Selisih: {d.get('selisih_handover_packing', '-') or '-'} pack",
                f"-> Status cocok: {d.get('status_handover_packing', '-') or '-'}",
                f"-> Bukti foto handover: {d.get('handover_photo_name', '-') or '-'}",
                "-> Log serah-terima:",
                *handover_lines,
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
            ("Tempat buang pillow siap & dikosongkan saat penuh", d.get("tempat_buang_siap", "")),
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

    photo_path = str(payload.get("details", {}).get("handover_photo_path", "")).strip()
    if photo_path:
        cap = f"Bukti handover packing | {payload.get('team_id', '-')} | {payload.get('work_date', '-')} {hhmm_now()}"
        ok_photo, _, data_photo = tg_send_photo(photo_path, cap)
        if not ok_photo:
            desc = ""
            if isinstance(data_photo, dict):
                desc = str(data_photo.get("description", "") or data_photo.get("error", ""))
            err_msgs.append(f"send photo failed: {desc or 'unknown'}")
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
    if details.get("sudah_dikirim_semua", "") == "X" and not details.get("nama_pic_cek", "").strip():
        errs.append("Jika 'Sudah dikirim semua' = X, petugas cek wajib diisi.")
    handover_rows = details.get("handover_rows", [])
    if not handover_rows:
        errs.append("Isi minimal 1 baris serah-terima ke packing/press.")
    else:
        for idx, row in enumerate(handover_rows, start=1):
            kirim = parse_optional_float(row.get("kirim_pack"))
            terima = parse_optional_float(row.get("terima_pack"))
            jam = str(row.get("jam", "")).strip()
            tl = str(row.get("tl_packing", "")).strip()
            pic = str(row.get("pic_packing", "")).strip()
            alasan = str(row.get("alasan_selisih", "")).strip()
            if not jam:
                errs.append(f"Baris handover #{idx}: jam wajib diisi.")
            if kirim is None:
                errs.append(f"Baris handover #{idx}: total dikirim wajib angka.")
            if terima is None:
                errs.append(f"Baris handover #{idx}: total diterima wajib angka.")
            if not tl:
                errs.append(f"Baris handover #{idx}: nama TL packing wajib diisi.")
            if not pic:
                errs.append(f"Baris handover #{idx}: nama PIC packing wajib diisi.")
            if kirim is not None and terima is not None:
                if kirim < 0 or terima < 0:
                    errs.append(f"Baris handover #{idx}: nilai tidak boleh negatif.")
                selisih = kirim - terima
                if abs(selisih) > 0.001 and not alasan:
                    errs.append(f"Baris handover #{idx}: ada selisih, alasan wajib diisi.")
    if details.get("tempat_buang_siap", "") not in {"O", "X"}:
        errs.append("2-2 wajib dipilih O atau X pada setiap laporan.")
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
                errs.append(f"Log delay giling #{idx}: jam wajib diisi.")
            if status not in {"O", "X"}:
                errs.append(f"Log delay giling #{idx}: status wajib O atau X.")
            if status == "O" and not detail:
                errs.append(f"Log delay giling #{idx}: detail wajib diisi jika status O.")

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
                errs.append(f"Jenis masalah vacum #{idx} wajib diisi.")
            if jumlah_val is None:
                errs.append(f"Jumlah pack masalah vacum #{idx} wajib angka.")
                continue
            if jumlah_val < 0:
                errs.append(f"Jumlah pack masalah vacum #{idx} tidak boleh negatif.")
                continue
            defect_sum_from_rows += jumlah_val

    if total_vacum is not None and total_vacum_defect is not None:
        if total_vacum < 0 or total_vacum_defect < 0:
            errs.append("Nilai total vacum tidak boleh negatif.")
        if total_vacum_defect > total_vacum:
            errs.append("Total vacum bermasalah tidak boleh lebih besar dari total vacum diproses.")
        if has_defect_row and abs(defect_sum_from_rows - total_vacum_defect) > 0.001:
            errs.append("Jumlah pack dari jenis masalah vacum harus sama dengan total vacum bermasalah.")
        if total_vacum_defect > 0 and not has_defect_row:
            errs.append("Jika ada vacum bermasalah, jenis masalah vacum wajib diisi.")

    vacum_ops_rows = details.get("vacum_ops_rows", [])
    active_ops_rows: List[Dict[str, Any]] = []
    if isinstance(vacum_ops_rows, list):
        for row in vacum_ops_rows:
            jam = str(row.get("jam", "")).strip()
            antrian_status = str(row.get("antrian_status", "")).strip()
            antrian_detail = str(row.get("antrian_detail", "")).strip()
            mesin_status = str(row.get("mesin_status", "")).strip()
            mesin_detail = str(row.get("mesin_detail", "")).strip()
            kirim_status = str(row.get("kirim_status", "")).strip()
            pic_cek = str(row.get("pic_cek", "")).strip()
            if jam or antrian_status or antrian_detail or mesin_status or mesin_detail or kirim_status or pic_cek:
                active_ops_rows.append(row)
    if not active_ops_rows:
        errs.append("Isi minimal 1 log operasional vacum.")
    else:
        for idx, row in enumerate(active_ops_rows, start=1):
            jam = str(row.get("jam", "")).strip()
            antrian_status = str(row.get("antrian_status", "")).strip()
            antrian_detail = str(row.get("antrian_detail", "")).strip()
            mesin_status = str(row.get("mesin_status", "")).strip()
            mesin_detail = str(row.get("mesin_detail", "")).strip()
            kirim_status = str(row.get("kirim_status", "")).strip()
            pic_cek = str(row.get("pic_cek", "")).strip()
            if not jam:
                errs.append(f"Log operasional vacum #{idx}: jam wajib diisi.")
            if antrian_status not in {"O", "X"}:
                errs.append(f"Log operasional vacum #{idx}: status antrian wajib O/X.")
            if antrian_status == "O" and not antrian_detail:
                errs.append(f"Log operasional vacum #{idx}: detail antrian wajib diisi jika status O.")
            if mesin_status not in {"O", "X"}:
                errs.append(f"Log operasional vacum #{idx}: status mesin wajib O/X.")
            if mesin_status == "X" and not mesin_detail:
                errs.append(f"Log operasional vacum #{idx}: detail mesin wajib diisi jika status X.")
            if kirim_status not in {"O", "X"}:
                errs.append(f"Log operasional vacum #{idx}: status kirim wajib O/X.")
            if kirim_status == "X" and not pic_cek:
                errs.append(f"Log operasional vacum #{idx}: PIC cek wajib diisi jika status kirim X.")
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
    if details.get("tempat_buang_siap", "") not in {"O", "X"}:
        errs.append("2-2 wajib dipilih O atau X pada setiap laporan.")
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
    if "giling_rows_non" not in st.session_state:
        st.session_state["giling_rows_non"] = 1
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
                row_count = ensure_row_count_from_session(
                    "defrost_rows_non",
                    ["def_jam_non_", "def_isi_non_", "def_kg_non_", "def_cat_non_"],
                    min_rows=1,
                    max_rows=20,
                )
                defrost_lines: List[str] = []
                defrost_pack_sum = 0.0
                defrost_pack_invalid = 0
                for idx in range(int(row_count)):
                    dc1, dc2, dc3, dc4 = st.columns([2, 3, 2, 3])
                    jam = dc1.text_input(f"Jam #{idx+1}", placeholder="12:55", key=f"def_jam_non_{idx}")
                    isi = dc2.text_input(f"Status #{idx+1}", placeholder="BB fresh", key=f"def_isi_non_{idx}")
                    pack = dc3.text_input(f"Pack #{idx+1}", placeholder="75", key=f"def_kg_non_{idx}")
                    cat = dc4.text_input(f"Catatan #{idx+1}", placeholder="sudah termasuk campuran", key=f"def_cat_non_{idx}")
                    pack_val = parse_optional_float(pack)
                    if pack.strip():
                        if pack_val is None:
                            defrost_pack_invalid += 1
                        elif pack_val >= 0:
                            defrost_pack_sum += pack_val
                    if jam.strip() or isi.strip() or pack.strip():
                        status_part = isi.strip()
                        if pack.strip():
                            status_part = f"{status_part} = {pack.strip()}pack".strip()
                        defrost_lines.append(f"- {jam.strip()} {status_part}".strip())
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
                status_defrost = st.text_area(
                    "Status defrost (Kalau sudah habis dipakai, tulis habis)",
                    value=loaded_details.get("status_defrost", ""),
                    placeholder="- 12:55 BB fresh = 75kg\n(sudah termasuk campuran)\n- 13:00 BB fresh = 75kg",
                )
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
            total_akhir_kg = st.text_input(
                "Total akhir (kg)",
                key=total_akhir_kg_key,
                placeholder="contoh: 225",
            )

            calc_scope = f"{team_id}::{work_date}"
            calc_expr_key = f"kg_calc_expr_non::{calc_scope}"
            calc_history_key = f"kg_calc_history_non::{calc_scope}"
            calc_open_key = f"kg_calc_open_non::{calc_scope}"
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

            calc_button_label = "Buka kalkulator kg" if not st.session_state.get(calc_open_key, False) else "Tutup kalkulator kg"
            if st.button(calc_button_label, key=f"btn_toggle_kg_calc_non::{calc_scope}"):
                st.session_state[calc_open_key] = not st.session_state.get(calc_open_key, False)
                st.rerun()

            if st.session_state.get(calc_open_key, False):
                st.markdown("#### Kalkulator kg (opsional)")
                st.caption("Cara cepat: ketik rumus -> cek hasil -> tekan tombol field tujuan.")
                c1, c2, c3 = st.columns([6, 2, 2])
                with c1:
                    st.text_input(
                        "Rumus kg (+ - * / dan kurung)",
                        key=calc_expr_key,
                        placeholder="contoh: (75 + 75 + 90) - 10",
                    )
                calc_value, calc_err = eval_simple_math(st.session_state.get(calc_expr_key, ""))
                if calc_err:
                    st.warning(calc_err)
                elif calc_value is not None:
                    st.success(f"Hasil: {format_float_compact(calc_value)} kg")
                else:
                    st.info("Masukkan rumus untuk menghitung kg.")

                with c2:
                    if st.button("Simpan", key=f"btn_save_kg_calc_non::{calc_scope}", use_container_width=True):
                        if calc_value is None:
                            st.warning("Isi rumus valid dulu sebelum simpan.")
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
                with c3:
                    if st.button("Hapus", key=f"btn_clear_kg_calc_non::{calc_scope}", use_container_width=True):
                        st.session_state[calc_history_key] = []
                        st.rerun()

                if calc_value is not None:
                    st.caption("Pakai hasil ke field:")
                    ap1, ap2, ap3, ap4 = st.columns(4)
                    with ap1:
                        if st.button("-> Barang beku", key=f"btn_apply_beku_kg_non::{calc_scope}", use_container_width=True):
                            st.session_state[total_beku_kg_key] = format_float_compact(calc_value)
                            st.rerun()
                    with ap2:
                        if st.button("-> BB fresh", key=f"btn_apply_fresh_kg_non::{calc_scope}", use_container_width=True):
                            st.session_state[total_fresh_kg_key] = format_float_compact(calc_value)
                            st.rerun()
                    with ap3:
                        if st.button("-> BB dibuang", key=f"btn_apply_buang_kg_non::{calc_scope}", use_container_width=True):
                            st.session_state[total_buang_kg_key] = format_float_compact(calc_value)
                            st.rerun()
                    with ap4:
                        if st.button("-> Total akhir", key=f"btn_apply_akhir_kg_non::{calc_scope}", use_container_width=True):
                            st.session_state[total_akhir_kg_key] = format_float_compact(calc_value)
                            st.rerun()

                history = st.session_state.get(calc_history_key, [])
                st.caption(f"Riwayat kalkulasi ({len(history)} terakhir)")
                history_lines = []
                for item in history:
                    at = str(item.get("at", "")).strip() or "-"
                    expr_hist = str(item.get("expr", "")).strip() or "-"
                    res = str(item.get("result", "")).strip() or "-"
                    history_lines.append(f"{at} | {expr_hist} = {res} kg")
                st.code("\n".join(history_lines) if history_lines else "-")
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
            tempat_opts = ["", "O", "X"]
            default_tempat_non = str(loaded_details.get("tempat_buang_siap", "") or "")
            default_idx_non = tempat_opts.index(default_tempat_non) if default_tempat_non in tempat_opts else 0
            tempat_buang_siap = st.selectbox(
                "Tempat buang pillow sudah siap dekat meja/rak, dan sudah dikosongkan kalau sudah penuh? (O/X tiap laporan)",
                options=tempat_opts,
                index=default_idx_non,
                format_func=lambda x: "Pilih O/X" if x == "" else x,
                key="tempat_buang_siap_non",
            )
            st.markdown("### 3-1. Status Giling")
            mode_giling = st.radio(
                "Cara isi status giling",
                options=["List baris", "Tulis manual"],
                horizontal=True,
                key="mode_giling_non",
            )
            giling_total_pack_auto = str(loaded_details.get("giling_total_pack_auto", ""))
            if mode_giling == "List baris":
                g1, g2, g3 = st.columns([2, 2, 6])
                with g1:
                    if st.button("+ Tambah baris giling", key="btn_add_giling", use_container_width=True):
                        st.session_state["giling_rows_non"] = min(20, int(st.session_state.get("giling_rows_non", 1)) + 1)
                        st.rerun()
                with g2:
                    if st.button("- Hapus baris giling", key="btn_del_giling", use_container_width=True):
                        st.session_state["giling_rows_non"] = max(1, int(st.session_state.get("giling_rows_non", 1)) - 1)
                        st.rerun()
                with g3:
                    st.caption(f"Jumlah baris giling: {int(st.session_state.get('giling_rows_non', 1))}")
                row_count_giling = ensure_row_count_from_session(
                    "giling_rows_non",
                    ["gil_jam_non_", "gil_isi_non_", "gil_kg_non_", "gil_cat_non_"],
                    min_rows=1,
                    max_rows=20,
                )
                giling_lines: List[str] = []
                giling_pack_sum = 0.0
                giling_pack_invalid = 0
                for idx in range(int(row_count_giling)):
                    gc1, gc2, gc3, gc4 = st.columns([2, 3, 2, 3])
                    jam = gc1.text_input(f"Jam giling #{idx+1}", placeholder="11:30", key=f"gil_jam_non_{idx}")
                    isi = gc2.text_input(f"Status giling #{idx+1}", placeholder="mulai giling batch 0", key=f"gil_isi_non_{idx}")
                    pack = gc3.text_input(f"Pack giling #{idx+1}", placeholder="75", key=f"gil_kg_non_{idx}")
                    cat = gc4.text_input(f"Catatan giling #{idx+1}", placeholder="opsional", key=f"gil_cat_non_{idx}")
                    pack_val = parse_optional_float(pack)
                    if pack.strip():
                        if pack_val is None:
                            giling_pack_invalid += 1
                        elif pack_val >= 0:
                            giling_pack_sum += pack_val
                    if jam.strip() or isi.strip() or pack.strip():
                        status_part = isi.strip()
                        if pack.strip():
                            status_part = f"{status_part} = {pack.strip()}pack".strip()
                        giling_lines.append(f"- {jam.strip()} {status_part}".strip())
                    if cat.strip():
                        giling_lines.append(f"({cat.strip()})")
                status_giling = "\n".join(giling_lines).strip()
                st.caption("Preview status giling")
                st.code(status_giling or "-")
                giling_total_pack_auto = format_float_compact(giling_pack_sum)
                st.text_input("Total pack giling (otomatis)", value=giling_total_pack_auto, disabled=True)
                if giling_pack_invalid > 0:
                    st.warning(f"Ada {giling_pack_invalid} nilai pack giling yang bukan angka, tidak dihitung.")
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
                    placeholder="- 11:30 mulai giling batch 0\n- 11:50 selesai giling batch 0",
                )
            total_giling = st.text_input(
                "Total Giling (berapa resep)",
                value=str(loaded_details.get("total_giling", "")),
                placeholder="contoh: 15",
            )
            st.markdown("#### Log delay giling (tiap laporan)")
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

            gd1, gd2, gd3 = st.columns([2, 2, 6])
            with gd1:
                if st.button("+ Tambah log delay", key="btn_add_delay_giling", use_container_width=True):
                    st.session_state["giling_delay_rows_non"] = min(20, int(st.session_state.get("giling_delay_rows_non", 1)) + 1)
                    st.rerun()
            with gd2:
                if st.button("- Hapus log delay", key="btn_del_delay_giling", use_container_width=True):
                    st.session_state["giling_delay_rows_non"] = max(1, int(st.session_state.get("giling_delay_rows_non", 1)) - 1)
                    st.rerun()
            with gd3:
                st.caption(f"Jumlah log delay: {int(st.session_state.get('giling_delay_rows_non', 1))}")

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
                dgc1, dgc2, dgc3 = st.columns([2, 1, 5])
                jam_delay = dgc1.text_input(
                    f"Jam delay #{idx+1}",
                    placeholder="14:10",
                    key=f"delay_jam_non_{idx}",
                )
                delay_opts = ["X", "O"]
                delay_raw = str(st.session_state.get(f"delay_status_non_{idx}", "") or "")
                delay_idx = delay_opts.index(delay_raw) if delay_raw in delay_opts else delay_opts.index("X")
                status_delay = dgc2.selectbox(
                    f"Ada delay? #{idx+1}",
                    options=delay_opts,
                    index=delay_idx,
                    format_func=lambda x: "Ya" if x == "O" else "Tidak",
                    key=f"delay_status_non_{idx}",
                    label_visibility="visible",
                )
                if status_delay == "O":
                    detail_delay = dgc3.text_input(
                        f"Penyebab delay #{idx+1}",
                        placeholder="contoh: antrian packing 40 menit",
                        key=f"delay_detail_non_{idx}",
                    )
                else:
                    detail_delay = dgc3.text_input(
                        f"Penyebab delay #{idx+1} (opsional)",
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
                            f"- {jam_delay.strip() or 'Jam belum diisi'} | Delay ada | {detail_delay.strip() or 'Penyebab belum diisi'}"
                        )
                    else:
                        giling_delay_lines_preview.append(
                            f"- {jam_delay.strip() or 'Jam belum diisi'} | Tidak ada delay"
                        )
                    if status_delay == "O":
                        has_delay_o = True
                    if status_delay == "X":
                        has_delay_x = True
            giling_delay_lama = "O" if has_delay_o else ("X" if has_delay_x else "")
            giling_delay_detail = "\n".join(giling_delay_lines_preview).strip()
            st.caption("Ringkasan log delay giling")
            st.code(giling_delay_detail or "- Belum ada log delay")
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
                    if st.button("+ Tambah baris vacum", key="btn_add_vacum", use_container_width=True):
                        st.session_state["vacum_rows_non"] = min(20, int(st.session_state.get("vacum_rows_non", 1)) + 1)
                        st.rerun()
                with v2:
                    if st.button("- Hapus baris vacum", key="btn_del_vacum", use_container_width=True):
                        st.session_state["vacum_rows_non"] = max(1, int(st.session_state.get("vacum_rows_non", 1)) - 1)
                        st.rerun()
                with v3:
                    st.caption(f"Jumlah baris vacum: {int(st.session_state.get('vacum_rows_non', 1))}")

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
                    vc1, vc2, vc3, vc4 = st.columns([2, 3, 2, 3])
                    jam = vc1.text_input(f"Jam vacum #{idx+1}", placeholder="12:00", key=f"vac_jam_non_{idx}")
                    isi = vc2.text_input(f"Status vacum #{idx+1}", placeholder="mulai vacum batch 1", key=f"vac_isi_non_{idx}")
                    pack = vc3.text_input(f"Pack vacum #{idx+1}", placeholder="75", key=f"vac_kg_non_{idx}")
                    cat = vc4.text_input(f"Catatan vacum #{idx+1}", placeholder="opsional", key=f"vac_cat_non_{idx}")
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
                        vacum_lines.append(f"- {jam.strip()} {status_part}".strip())
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

            total_hasil_vakum = st.text_input(
                "Total vakum diproses (pack)",
                value=str(loaded_details.get("total_hasil_vakum", "")),
                placeholder="contoh: 88",
            )

            st.markdown("#### Jenis masalah vacum/pillow")
            loaded_vacum_defect_rows = loaded_details.get("vacum_defect_rows", [])
            if isinstance(loaded_vacum_defect_rows, list) and loaded_vacum_defect_rows:
                existing_defect_local = False
                for i in range(20):
                    if str(st.session_state.get(f"vac_defect_jenis_non_{i}", "")).strip() or str(
                        st.session_state.get(f"vac_defect_qty_non_{i}", "")
                    ).strip():
                        existing_defect_local = True
                        break
                if not existing_defect_local:
                    st.session_state["vacum_defect_rows_non"] = min(20, max(1, len(loaded_vacum_defect_rows)))
                    for idx, row in enumerate(loaded_vacum_defect_rows[:20]):
                        st.session_state[f"vac_defect_jenis_non_{idx}"] = str(row.get("jenis", ""))
                        st.session_state[f"vac_defect_qty_non_{idx}"] = str(row.get("jumlah_pack", ""))
            elif str(loaded_details.get("jenis_defect_vacum", "")).strip() or str(
                loaded_details.get("total_vacum_defect_pack", "")
            ).strip():
                # Backward compatibility for old single text fields.
                if not str(st.session_state.get("vac_defect_jenis_non_0", "")).strip() and not str(
                    st.session_state.get("vac_defect_qty_non_0", "")
                ).strip():
                    st.session_state["vacum_defect_rows_non"] = 1
                    st.session_state["vac_defect_jenis_non_0"] = str(loaded_details.get("jenis_defect_vacum", ""))
                    st.session_state["vac_defect_qty_non_0"] = str(loaded_details.get("total_vacum_defect_pack", ""))

            vd1, vd2, vd3 = st.columns([2, 2, 6])
            with vd1:
                if st.button("+ Tambah jenis masalah", key="btn_add_vac_defect", use_container_width=True):
                    st.session_state["vacum_defect_rows_non"] = min(20, int(st.session_state.get("vacum_defect_rows_non", 1)) + 1)
                    st.rerun()
            with vd2:
                if st.button("- Hapus jenis masalah", key="btn_del_vac_defect", use_container_width=True):
                    st.session_state["vacum_defect_rows_non"] = max(1, int(st.session_state.get("vacum_defect_rows_non", 1)) - 1)
                    st.rerun()
            with vd3:
                st.caption(f"Jumlah baris masalah vacum: {int(st.session_state.get('vacum_defect_rows_non', 1))}")

            row_count_vac_defect = ensure_row_count_from_session(
                "vacum_defect_rows_non",
                ["vac_defect_jenis_non_", "vac_defect_qty_non_"],
                min_rows=1,
                max_rows=20,
            )
            vacum_defect_rows: List[Dict[str, Any]] = []
            vacum_defect_lines: List[str] = []
            sum_vacum_defect = 0.0
            for idx in range(int(row_count_vac_defect)):
                vdc1, vdc2 = st.columns([4, 2])
                jenis = vdc1.text_input(
                    f"Jenis masalah #{idx+1}",
                    placeholder="contoh: bocor / basi / seal tidak rapat",
                    key=f"vac_defect_jenis_non_{idx}",
                )
                jumlah_pack = vdc2.text_input(
                    f"Jumlah pack #{idx+1}",
                    placeholder="contoh: 2",
                    key=f"vac_defect_qty_non_{idx}",
                )
                qty_val = parse_optional_float(jumlah_pack)
                if jenis.strip() or jumlah_pack.strip():
                    vacum_defect_rows.append({"jenis": jenis, "jumlah_pack": jumlah_pack})
                if jenis.strip() and qty_val is not None and qty_val >= 0:
                    vacum_defect_lines.append(f"{jenis.strip()} {format_float_compact(qty_val)} pack")
                    sum_vacum_defect += qty_val

            jenis_defect_vacum = ", ".join(vacum_defect_lines)
            total_vacum_defect_pack = format_float_compact(sum_vacum_defect)
            total_vacum_num = parse_optional_float(total_hasil_vakum)
            total_vacum_ok_pack = ""
            if total_vacum_num is not None:
                total_vacum_ok_pack = format_float_compact(total_vacum_num - sum_vacum_defect)
            st.text_input("Total vacum bermasalah (otomatis, pack)", value=total_vacum_defect_pack, disabled=True)
            st.text_input("Total vakum normal (otomatis, pack)", value=total_vacum_ok_pack, disabled=True)
            st.caption("Rumus: total normal = total diproses - total bermasalah")

            st.markdown("#### Log operasional vacum (tiap laporan)")
            loaded_vacum_ops_rows = loaded_details.get("vacum_ops_rows", [])
            if isinstance(loaded_vacum_ops_rows, list) and loaded_vacum_ops_rows:
                existing_ops_local = False
                for i in range(20):
                    if str(st.session_state.get(f"vac_ops_jam_non_{i}", "")).strip() or str(
                        st.session_state.get(f"vac_ops_antrian_non_{i}", "")
                    ).strip() or str(st.session_state.get(f"vac_ops_mesin_non_{i}", "")).strip() or str(
                        st.session_state.get(f"vac_ops_kirim_non_{i}", "")
                    ).strip():
                        existing_ops_local = True
                        break
                if not existing_ops_local:
                    st.session_state["vacum_ops_rows_non"] = min(20, max(1, len(loaded_vacum_ops_rows)))
                    for idx, row in enumerate(loaded_vacum_ops_rows[:20]):
                        st.session_state[f"vac_ops_jam_non_{idx}"] = str(row.get("jam", ""))
                        st.session_state[f"vac_ops_antrian_non_{idx}"] = str(row.get("antrian_status", ""))
                        st.session_state[f"vac_ops_antrian_det_non_{idx}"] = str(row.get("antrian_detail", ""))
                        st.session_state[f"vac_ops_mesin_non_{idx}"] = str(row.get("mesin_status", ""))
                        st.session_state[f"vac_ops_mesin_det_non_{idx}"] = str(row.get("mesin_detail", ""))
                        st.session_state[f"vac_ops_kirim_non_{idx}"] = str(row.get("kirim_status", ""))
                        st.session_state[f"vac_ops_pic_non_{idx}"] = str(row.get("pic_cek", ""))
            elif (
                str(loaded_details.get("vacum_antrian_lama", "")).strip()
                or str(loaded_details.get("mesin_vacum_istirahat", "")).strip()
                or str(loaded_details.get("sudah_dikirim_semua", "")).strip()
                or str(loaded_details.get("vacum_antrian_detail", "")).strip()
                or str(loaded_details.get("mesin_vacum_istirahat_detail", "")).strip()
                or str(loaded_details.get("nama_pic_cek", "")).strip()
            ):
                if not str(st.session_state.get("vac_ops_jam_non_0", "")).strip() and not str(
                    st.session_state.get("vac_ops_antrian_non_0", "")
                ).strip() and not str(st.session_state.get("vac_ops_mesin_non_0", "")).strip():
                    st.session_state["vacum_ops_rows_non"] = 1
                    st.session_state["vac_ops_jam_non_0"] = ""
                    st.session_state["vac_ops_antrian_non_0"] = str(loaded_details.get("vacum_antrian_lama", ""))
                    st.session_state["vac_ops_antrian_det_non_0"] = str(loaded_details.get("vacum_antrian_detail", ""))
                    st.session_state["vac_ops_mesin_non_0"] = str(loaded_details.get("mesin_vacum_istirahat", ""))
                    st.session_state["vac_ops_mesin_det_non_0"] = str(loaded_details.get("mesin_vacum_istirahat_detail", ""))
                    st.session_state["vac_ops_kirim_non_0"] = str(loaded_details.get("sudah_dikirim_semua", ""))
                    st.session_state["vac_ops_pic_non_0"] = str(loaded_details.get("nama_pic_cek", ""))

            vo1, vo2, vo3 = st.columns([2, 2, 6])
            with vo1:
                if st.button("+ Tambah log operasional", key="btn_add_vac_ops", use_container_width=True):
                    st.session_state["vacum_ops_rows_non"] = min(20, int(st.session_state.get("vacum_ops_rows_non", 1)) + 1)
                    st.rerun()
            with vo2:
                if st.button("- Hapus log operasional", key="btn_del_vac_ops", use_container_width=True):
                    st.session_state["vacum_ops_rows_non"] = max(1, int(st.session_state.get("vacum_ops_rows_non", 1)) - 1)
                    st.rerun()
            with vo3:
                st.caption(f"Jumlah log operasional vacum: {int(st.session_state.get('vacum_ops_rows_non', 1))}")

            row_count_vac_ops = ensure_row_count_from_session(
                "vacum_ops_rows_non",
                [
                    "vac_ops_jam_non_",
                    "vac_ops_antrian_non_",
                    "vac_ops_antrian_det_non_",
                    "vac_ops_mesin_non_",
                    "vac_ops_mesin_det_non_",
                    "vac_ops_kirim_non_",
                    "vac_ops_pic_non_",
                ],
                min_rows=1,
                max_rows=20,
            )
            vacum_ops_rows: List[Dict[str, Any]] = []
            antrian_detail_lines: List[str] = []
            mesin_detail_lines: List[str] = []
            has_antrian_o = False
            has_antrian_x = False
            has_mesin_o = False
            has_mesin_x = False
            has_kirim_o = False
            has_kirim_x = False
            pic_candidates: List[str] = []
            for idx in range(int(row_count_vac_ops)):
                st.markdown(f"**Log operasional #{idx+1}**")
                voc1, voc2, voc3, voc4 = st.columns([2, 2, 2, 2])
                jam_ops = voc1.text_input(f"Jam #{idx+1}", placeholder="14:20", key=f"vac_ops_jam_non_{idx}")
                ops_opts = ["", "O", "X"]
                antrian_raw = str(st.session_state.get(f"vac_ops_antrian_non_{idx}", "") or "")
                antrian_idx = ops_opts.index(antrian_raw) if antrian_raw in ops_opts else 0
                antrian_status = voc2.selectbox(
                    f"Antrian #{idx+1}",
                    options=ops_opts,
                    index=antrian_idx,
                    format_func=lambda x: "Pilih" if x == "" else x,
                    key=f"vac_ops_antrian_non_{idx}",
                )
                mesin_raw = str(st.session_state.get(f"vac_ops_mesin_non_{idx}", "") or "")
                mesin_idx = ops_opts.index(mesin_raw) if mesin_raw in ops_opts else 0
                mesin_status = voc3.selectbox(
                    f"Mesin #{idx+1}",
                    options=ops_opts,
                    index=mesin_idx,
                    format_func=lambda x: "Pilih" if x == "" else x,
                    key=f"vac_ops_mesin_non_{idx}",
                )
                kirim_raw = str(st.session_state.get(f"vac_ops_kirim_non_{idx}", "") or "")
                kirim_idx = ops_opts.index(kirim_raw) if kirim_raw in ops_opts else 0
                kirim_status = voc4.selectbox(
                    f"Kirim #{idx+1}",
                    options=ops_opts,
                    index=kirim_idx,
                    format_func=lambda x: "Pilih" if x == "" else x,
                    key=f"vac_ops_kirim_non_{idx}",
                )
                vod1, vod2, vod3 = st.columns([3, 3, 2])
                antrian_detail = vod1.text_input(
                    f"Detail antrian #{idx+1}",
                    placeholder="isi jika antrian = O",
                    key=f"vac_ops_antrian_det_non_{idx}",
                )
                mesin_detail = vod2.text_input(
                    f"Detail mesin #{idx+1}",
                    placeholder="isi jika mesin = X",
                    key=f"vac_ops_mesin_det_non_{idx}",
                )
                pic_cek = vod3.text_input(
                    f"PIC cek #{idx+1}",
                    placeholder="isi jika kirim = X",
                    key=f"vac_ops_pic_non_{idx}",
                )
                if jam_ops.strip() or antrian_status.strip() or antrian_detail.strip() or mesin_status.strip() or mesin_detail.strip() or kirim_status.strip() or pic_cek.strip():
                    vacum_ops_rows.append(
                        {
                            "jam": jam_ops,
                            "antrian_status": antrian_status,
                            "antrian_detail": antrian_detail,
                            "mesin_status": mesin_status,
                            "mesin_detail": mesin_detail,
                            "kirim_status": kirim_status,
                            "pic_cek": pic_cek,
                        }
                    )
                if antrian_status == "O":
                    has_antrian_o = True
                if antrian_status == "X":
                    has_antrian_x = True
                if mesin_status == "O":
                    has_mesin_o = True
                if mesin_status == "X":
                    has_mesin_x = True
                if kirim_status == "O":
                    has_kirim_o = True
                if kirim_status == "X":
                    has_kirim_x = True
                if antrian_detail.strip():
                    antrian_detail_lines.append(f"{jam_ops.strip() or '-'}: {antrian_detail.strip()}")
                if mesin_detail.strip():
                    mesin_detail_lines.append(f"{jam_ops.strip() or '-'}: {mesin_detail.strip()}")
                if pic_cek.strip():
                    pic_candidates.append(pic_cek.strip())

            vacum_antrian_lama = "O" if has_antrian_o else ("X" if has_antrian_x else "")
            vacum_antrian_detail = "\n".join(antrian_detail_lines).strip()
            mesin_vacum_istirahat = "O" if has_mesin_o else ("X" if has_mesin_x else "")
            mesin_vacum_istirahat_detail = "\n".join(mesin_detail_lines).strip()
            sudah_dikirim_semua = "X" if has_kirim_x else ("O" if has_kirim_o else "")
            nama_pic_cek = ", ".join(dict.fromkeys(pic_candidates))
            st.markdown("### 4. Total barang ada masalah")
            masalah_total_barang = st.text_area(
                "Tulis masalah barang (contoh: basi, kemasan sobek, dll)",
                value=loaded_details.get("masalah_total_barang", ""),
                placeholder="- Basi 2kg\n- Kemasan/pojac sobek 10 pack",
            )
            st.markdown("### 5. Total barang dikirim ke packing (atau press)")
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
                        st.session_state[f"handover_pic_non_{idx}"] = str(row.get("pic_packing", ""))
                        st.session_state[f"handover_alasan_non_{idx}"] = str(row.get("alasan_selisih", ""))

            h1, h2, h3 = st.columns([2, 2, 6])
            with h1:
                if st.button("+ Tambah baris handover", key="btn_add_handover", use_container_width=True):
                    st.session_state["handover_rows_non"] = min(30, int(st.session_state.get("handover_rows_non", 1)) + 1)
                    st.rerun()
            with h2:
                if st.button("- Hapus baris handover", key="btn_del_handover", use_container_width=True):
                    st.session_state["handover_rows_non"] = max(1, int(st.session_state.get("handover_rows_non", 1)) - 1)
                    st.rerun()
            with h3:
                st.caption(f"Jumlah baris handover: {int(st.session_state.get('handover_rows_non', 1))}")

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
                jam = hc1.text_input(f"Jam handover #{idx+1}", placeholder="14:20", key=f"handover_jam_non_{idx}")
                kirim = hc2.text_input(f"Kirim #{idx+1}", placeholder="120", key=f"handover_kirim_non_{idx}")
                terima = hc3.text_input(f"Terima #{idx+1}", placeholder="116", key=f"handover_terima_non_{idx}")
                tl = hc4.text_input(f"TL packing #{idx+1}", placeholder="Ibu Rina", key=f"handover_tl_non_{idx}")
                pic = hc5.text_input(f"PIC packing #{idx+1}", placeholder="Siti", key=f"handover_pic_non_{idx}")
                alasan = hc6.text_input(
                    f"Alasan selisih #{idx+1}",
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
            uploaded_handover_photo = st.file_uploader(
                "Upload foto bukti serah-terima (jpg/png/webp)",
                type=["jpg", "jpeg", "png", "webp"],
                key=f"handover_photo_upload_non::{photo_scope}",
            )
            if uploaded_handover_photo is not None:
                upload_sig = f"{uploaded_handover_photo.name}:{uploaded_handover_photo.size}"
                if st.session_state.get(handover_photo_sig_key, "") != upload_sig:
                    suffix = Path(uploaded_handover_photo.name).suffix.lower() or ".jpg"
                    safe_team = "".join(ch for ch in str(team_id) if ch.isalnum() or ch in {"-", "_"}).strip() or "team"
                    file_name = f"handover_{safe_team}_{str(work_date).replace('-', '')}_{datetime.now(APP_TZ).strftime('%H%M%S')}{suffix}"
                    target_path = EVIDENCE_DIR / file_name
                    target_path.write_bytes(uploaded_handover_photo.getvalue())
                    st.session_state[handover_photo_path_key] = str(target_path)
                    st.session_state[handover_photo_name_key] = uploaded_handover_photo.name
                    st.session_state[handover_photo_sig_key] = upload_sig

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
                "defrost_total_pack_auto": defrost_total_pack_auto,
                "total_beku": total_beku,
                "total_beku_kg": total_beku_kg,
                "total_fresh_kg": total_fresh_kg,
                "total_buang_kg": total_buang_kg,
                "total_akhir_kg": total_akhir_kg,
                "kg_calc_history": st.session_state.get(calc_history_key, []),
                "tempat_buang_siap": tempat_buang_siap,
                "status_giling": status_giling,
                "giling_total_pack_auto": giling_total_pack_auto,
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
            st.markdown("### Data Umum")
            produk = st.text_input("Produk", value=loaded_details.get("produk", ""))
            alat = st.text_input("Nama alat", value=loaded_details.get("alat", ""))
            nama_petugas_raw = st.text_area(
                "Nama Petugas (satu baris satu nama)",
                value=loaded_details.get("nama_petugas_raw", ""),
                placeholder="Linda\nLian",
            )
            st.caption("Tambah manual: 1 baris = 1 nama petugas.")
            nama_petugas_list = parse_name_lines(nama_petugas_raw)
            timer_ada = st.selectbox(
                "Timer ada?",
                options=["O", "X"],
                index=0 if loaded_details.get("timer_ada", "O") == "O" else 1,
            )
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
            tempat_opts = ["", "O", "X"]
            default_tempat_st = str(loaded_details.get("tempat_buang_siap", "") or "")
            default_idx_st = tempat_opts.index(default_tempat_st) if default_tempat_st in tempat_opts else 0
            tempat_buang_siap = st.selectbox(
                "Tempat buang pillow sudah siap dekat meja/rak, dan sudah dikosongkan kalau sudah penuh? (O/X tiap laporan)",
                options=tempat_opts,
                index=default_idx_st,
                format_func=lambda x: "Pilih O/X" if x == "" else x,
                key="tempat_buang_siap_st",
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
        submitted = st.button("Kirim Laporan", type="primary")

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
            if result.telegram_error.strip():
                st.warning(f"Catatan Telegram: {result.telegram_error}")
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
            for k in ["tempat_buang_siap_non", "tempat_buang_siap_st"]:
                if k in st.session_state:
                    st.session_state[k] = ""


if __name__ == "__main__":
    main()
