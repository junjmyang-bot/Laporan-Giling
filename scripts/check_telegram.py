import json
import os
import sys
from urllib import parse, request


def fail(msg: str, code: int = 1) -> None:
    print(msg)
    sys.exit(code)


def get_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        fail(f"[ERROR] Missing env: {name}")
    return value


def http_get(url: str):
    req = request.Request(url=url, method="GET")
    with request.urlopen(req, timeout=20) as resp:
        text = resp.read().decode("utf-8", errors="ignore")
        try:
            return resp.status, json.loads(text)
        except Exception:
            return resp.status, {"raw": text}


def main() -> None:
    token = get_env("TELEGRAM_BOT_TOKEN")
    chat_id = get_env("TELEGRAM_CHAT_ID")

    base = f"https://api.telegram.org/bot{token}"

    status, me = http_get(f"{base}/getMe")
    if status != 200 or not me.get("ok"):
        fail(f"[ERROR] getMe failed: status={status} payload={me}")
    print(f"[OK] Bot connected: @{me.get('result', {}).get('username', '-')}")

    text = "Telegram check from Laporan Giling app."
    qs = parse.urlencode({"chat_id": chat_id, "text": text})
    status, sent = http_get(f"{base}/sendMessage?{qs}")
    if status != 200 or not sent.get("ok"):
        fail(f"[ERROR] sendMessage failed: status={status} payload={sent}")

    msg_id = sent.get("result", {}).get("message_id")
    print(f"[OK] Message sent. chat_id={chat_id}, message_id={msg_id}")


if __name__ == "__main__":
    main()
