import json
from pathlib import Path

INPUT_DIR = Path("/opt/Digital_Footprint_System/result/domains-monitor/new")
OUTPUT_DIR = Path("/opt/Digital_Footprint_System/result/domains-monitor/jsonl")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def normalize(value):
    if value == "N/A":
        return None
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value

for input_file in INPUT_DIR.glob("*.json"):
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    find_domain = data.get("find_domain")
    new_result = data.get("new_result", [])

    output_file = OUTPUT_DIR / f"{input_file.stem}.jsonl"

    with open(output_file, "w", encoding="utf-8") as out:
        for item in new_result:
            domain_info = item.get("domain_info", {}) or {}
            whois = domain_info.get("whois", {}) or {}
            ssl = domain_info.get("ssl", {}) or {}
            shodan = domain_info.get("shodan", {}) or {}

            doc = {
                "find_domain": find_domain,
                "api_item": item.get("api_item"),
                "domain": domain_info.get("domain"),
                "ip": domain_info.get("ip"),
                "whois": whois,
                "ssl": ssl,
                "shodan": shodan,
                "whois_registrar": whois.get("registrar"),
                "whois_org": whois.get("org"),
                "whois_country": whois.get("country"),
                "ssl_has_ssl": ssl.get("has_ssl"),
                "ssl_subject_cn": ssl.get("subject_cn"),
                "ssl_issuer_cn": ssl.get("issuer_cn"),
                "shodan_found": shodan.get("found"),
                "shodan_org": shodan.get("org"),
                "shodan_country": shodan.get("country_name"),
                "shodan_city": shodan.get("city"),
                "shodan_ports_count": len(shodan.get("ports", [])) if isinstance(shodan.get("ports"), list) else None,
                "pipeline": "domains_monitor",
                "source_file": input_file.name
            }

            out.write(json.dumps(normalize(doc), ensure_ascii=False) + "\n")

    print(f"Converted: {input_file} -> {output_file}")
