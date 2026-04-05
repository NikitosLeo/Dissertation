#!/usr/bin/env python3
import json
import re
from pathlib import Path

INPUT_DIR = Path("/opt/Digital_Footprint_System/result/leakcheck/new")
OUTPUT_DIR = Path("/opt/Digital_Footprint_System/result/leakcheck/jsonl")


def try_fix_json_text(text: str) -> str:
    text = text.strip()
    text = text.lstrip("\ufeff")
    text = re.sub(r",\s*([\]}])", r"\1", text)
    return text


def load_json_flexible(src_path: Path):
    raw_text = src_path.read_text(encoding="utf-8", errors="replace")
    fixed_text = try_fix_json_text(raw_text)
    return json.loads(fixed_text)


def normalize_to_records(data, src_path: Path):
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    raise ValueError(f"{src_path}: unsupported JSON root type: {type(data).__name__}")


def convert_file(src_path: Path, dst_path: Path):
    data = load_json_flexible(src_path)
    records = normalize_to_records(data, src_path)

    with dst_path.open("w", encoding="utf-8") as out:
        for item in records:
            out.write(json.dumps(item, ensure_ascii=False) + "\n")


def main():
    if not INPUT_DIR.exists():
        print(f"ERROR: input dir not found: {INPUT_DIR}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(INPUT_DIR.glob("*.json"))
    if not files:
        print(f"No .json files found in {INPUT_DIR}")
        return

    ok_count = 0
    err_count = 0

    for src in files:
        dst = OUTPUT_DIR / f"{src.stem}.jsonl"
        try:
            convert_file(src, dst)
            print(f"OK: {src} -> {dst}")
            ok_count += 1
        except Exception as e:
            print(f"ERROR: {src}: {e}")
            err_count += 1

    print(f"Done. Success: {ok_count}, Errors: {err_count}")


if __name__ == "__main__":
    main()