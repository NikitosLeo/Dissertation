#!/usr/bin/env python3
import json
from pathlib import Path

SRC_DIR = Path("/opt/Digital_Footprint_System/result/infosearch/new")
DST_DIR = Path("/opt/Digital_Footprint_System/result/infosearch/jsonl")


def convert_file(src_path: Path, dst_path: Path):
    with src_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    domain_zone = data.get("domain_zone")
    items = data.get("new_result", [])

    if not isinstance(items, list):
        raise ValueError(f"{src_path}: field 'new_result' is not a list")

    with dst_path.open("w", encoding="utf-8") as out:
        for item in items:
            if not isinstance(item, dict):
                continue

            record = dict(item)
            if domain_zone is not None and "domain_zone" not in record:
                record["domain_zone"] = domain_zone

            out.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    if not SRC_DIR.exists():
        print(f"ERROR: source directory not found: {SRC_DIR}")
        return

    DST_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(SRC_DIR.glob("*.json"))
    if not files:
        print(f"No JSON files found in {SRC_DIR}")
        return

    for src in files:
        dst = DST_DIR / f"{src.stem}.jsonl"
        try:
            convert_file(src, dst)
            print(f"OK: {src} -> {dst}")
        except Exception as e:
            print(f"ERROR: {src}: {e}")


if __name__ == "__main__":
    main()