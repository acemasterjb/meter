from datetime import datetime


def log(context: str, msg: str):
    print(f"[{context} {datetime.now().isoformat(timespec="seconds")}] {msg}")
