import json
import logging
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Пути к входным файлам
API_KEY_FILE = Path("/opt/Digital_Footprint_System/variables/tokens/google_api_key")
QUERIES_FILE = Path("/opt/Digital_Footprint_System/variables/domains")

# Базовые директории для результатов и логов
BASE_RESULT_DIR = Path("/opt/Digital_Footprint_System/result/google_cse")
NEW_DIR = BASE_RESULT_DIR / "new"
OLD_DIR = BASE_RESULT_DIR / "old"
LOG_DIR = BASE_RESULT_DIR / "logs"
LOG_FILE = LOG_DIR / "google_cse.log"

# Захардкоженный Search Engine ID
GOOGLE_CSE_CX = "907f43a7d67264257"

# Настройки Google CSE
RESULTS_PER_PAGE = 10
MAX_RESULTS = 50


def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


def read_single_value(file_path: Path, required: bool = True) -> str:
    if not file_path.exists():
        if required:
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        return ""

    value = file_path.read_text(encoding="utf-8").strip()

    if required and not value:
        raise ValueError(f"Файл пустой: {file_path}")

    return value


def read_list(file_path: Path) -> list[str]:
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    content = file_path.read_text(encoding="utf-8")
    if not content.strip():
        raise ValueError(f"Файл пустой: {file_path}")

    values = []

    for line in content.splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue

        for part in line.split(","):
            item = part.strip()
            if item:
                values.append(item)

    unique_values = []
    seen = set()

    for item in values:
        if item not in seen:
            seen.add(item)
            unique_values.append(item)

    if not unique_values:
        raise ValueError(f"Файл не содержит корректных значений: {file_path}")

    return unique_values


def sanitize_filename(value: str) -> str:
    safe_chars = []

    for char in value:
        if char.isalnum() or char in ("-", "_", "."):
            safe_chars.append(char)
        else:
            safe_chars.append("_")

    sanitized = "".join(safe_chars).strip("._")
    return sanitized or "query"


def save_json(file_path: Path, data):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_result_items(file_path: Path, result_key: str) -> list:
    if not file_path.exists():
        return []

    try:
        content = file_path.read_text(encoding="utf-8").strip()
        if not content:
            return []

        data = json.loads(content)

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            result = data.get(result_key, [])
            if isinstance(result, list):
                return result

        return []

    except json.JSONDecodeError:
        logging.warning(f"Некорректный JSON в файле {file_path}, будет использован пустой список")
        return []


def normalize_google_item(item: dict) -> dict:
    pagemap = item.get("pagemap", {}) if isinstance(item, dict) else {}

    return {
        "title": item.get("title", ""),
        "link": item.get("link", ""),
        "displayLink": item.get("displayLink", ""),
        "snippet": item.get("snippet", ""),
        "formattedUrl": item.get("formattedUrl", ""),
        "htmlFormattedUrl": item.get("htmlFormattedUrl", ""),
        "pagemap": pagemap
    }


def make_item_key(item) -> str:
    if isinstance(item, dict):
        link = item.get("link")
        if link:
            return f"link:{link}"

    return json.dumps(item, sort_keys=True, ensure_ascii=False)


def merge_without_duplicates(old_items: list, new_items: list) -> list:
    merged = []
    seen = set()

    for item in old_items + new_items:
        key = make_item_key(item)
        if key not in seen:
            seen.add(key)
            merged.append(item)

    return merged


def save_new_file(file_path: Path, query: str, new_items: list):
    data = {
        "query": query,
        "new_result": new_items
    }
    save_json(file_path, data)


def save_old_file(file_path: Path, query: str, old_items: list):
    data = {
        "query": query,
        "old_result": old_items
    }
    save_json(file_path, data)


def fetch_all_google_results(service, query: str) -> list:
    all_items = []
    start_index = 1

    while start_index <= MAX_RESULTS:
        logging.info(f"Google CSE запрос: query='{query}', start={start_index}")

        response = service.cse().list(
            q=query,
            cx=GOOGLE_CSE_CX,
            num=RESULTS_PER_PAGE,
            start=start_index
        ).execute()

        items = response.get("items", [])
        if not isinstance(items, list):
            raise ValueError(f"Поле 'items' не является списком для query='{query}'")

        normalized_items = [normalize_google_item(item) for item in items]
        all_items.extend(normalized_items)

        logging.info(
            f"Получено результатов для query='{query}' на start={start_index}: {len(items)}"
        )

        queries_info = response.get("queries", {})
        next_page = queries_info.get("nextPage", [])

        if not next_page:
            break

        next_start = next_page[0].get("startIndex")
        if not next_start or next_start <= start_index:
            break

        start_index = next_start

    return all_items


def process_query(service, query: str):
    safe_query_name = sanitize_filename(query)
    new_file = NEW_DIR / f"{safe_query_name}_new.json"
    old_file = OLD_DIR / f"{safe_query_name}_old.json"

    logging.info(f"Обработка query: {query}")

    try:
        current_items = fetch_all_google_results(service, query)

        logging.info(f"Всего получено результатов для query='{query}': {len(current_items)}")

        old_items = load_result_items(old_file, "old_result")
        old_keys = {make_item_key(item) for item in old_items}

        new_items = [
            item for item in current_items
            if make_item_key(item) not in old_keys
        ]

        updated_old_items = merge_without_duplicates(old_items, new_items)

        save_new_file(new_file, query, new_items)
        save_old_file(old_file, query, updated_old_items)

        if new_items:
            logging.info(f"Найдено новых результатов для query='{query}': {len(new_items)}")
        else:
            logging.info(f"Новых результатов для query='{query}' не найдено")

    except HttpError as e:
        logging.error(f"Google API HTTP-ошибка для query='{query}': {e}")
        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)

    except Exception as e:
        logging.error(f"Ошибка для query='{query}': {e}")
        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)


def main():
    setup_logging()

    NEW_DIR.mkdir(parents=True, exist_ok=True)
    OLD_DIR.mkdir(parents=True, exist_ok=True)

    api_key = read_single_value(API_KEY_FILE, required=True)
    queries = read_list(QUERIES_FILE)

    logging.info(f"Загружен Google API key из {API_KEY_FILE}")
    logging.info(f"Используется встроенный Google CX: {GOOGLE_CSE_CX}")
    logging.info(f"Загружены queries: {queries}")

    service = build("customsearch", "v1", developerKey=api_key)

    for query in queries:
        process_query(service, query)


if __name__ == "__main__":
    main()