import json
import logging
from pathlib import Path

from leakcheck import LeakCheckAPI


# Пути к входным файлам
TOKEN_FILE = Path("/opt/Digital_Footprint_System/variables/tokens/leakcheck")
QUERIES_FILE = Path("/opt/Digital_Footprint_System/variables/domains")

# Базовые директории для результатов и логов
BASE_RESULT_DIR = Path("/opt/Digital_Footprint_System/result/leakcheck")
NEW_DIR = BASE_RESULT_DIR / "new"
OLD_DIR = BASE_RESULT_DIR / "old"
LOG_DIR = BASE_RESULT_DIR / "logs"
LOG_FILE = LOG_DIR / "leakcheck.log"


def setup_logging():
    """
    Настраивает логирование:
    - вывод в консоль
    - запись в файл leakcheck.log
    """
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
    """
    Читает одно значение из файла.

    Используется для API key.
    """
    if not file_path.exists():
        if required:
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        return ""

    value = file_path.read_text(encoding="utf-8").strip()

    if required and not value:
        raise ValueError(f"Файл пустой: {file_path}")

    return value


def read_list(file_path: Path) -> list[str]:
    """
    Читает список значений из файла.

    Поддерживает:
    - построчный формат
    - через запятую
    - смешанный формат

    Также:
    - удаляет комментарии после '#'
    - убирает дубликаты с сохранением порядка
    """
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
    """
    Преобразует строку в безопасное имя файла.
    """
    safe_chars = []

    for char in value:
        if char.isalnum() or char in ("-", "_", "."):
            safe_chars.append(char)
        else:
            safe_chars.append("_")

    sanitized = "".join(safe_chars).strip("._")
    return sanitized or "query"


def load_result_items(file_path: Path, result_key: str) -> list:
    """
    Загружает список результатов из JSON-файла.

    Если файл отсутствует, пустой или содержит некорректный JSON,
    возвращает пустой список.
    """
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


def save_json(file_path: Path, data):
    """
    Сохраняет данные в JSON-файл.
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def make_item_key(item) -> str:
    """
    Формирует уникальный ключ для результата.

    Приоритет:
    - leak_id
    - id
    - email + source + password
    - сериализация объекта
    """
    if isinstance(item, dict):
        if item.get("leak_id") is not None:
            return f"leak_id:{item['leak_id']}"

        if item.get("id") is not None:
            return f"id:{item['id']}"

        email = item.get("email")
        source = item.get("source")
        password = item.get("password")

        if email or source or password:
            return f"{email}|{source}|{password}"

    return json.dumps(item, sort_keys=True, ensure_ascii=False)


def merge_without_duplicates(old_items: list, new_items: list) -> list:
    """
    Объединяет старые и новые элементы без дубликатов.
    """
    merged = []
    seen = set()

    for item in old_items + new_items:
        key = make_item_key(item)
        if key not in seen:
            seen.add(key)
            merged.append(item)

    return merged


def save_new_file(file_path: Path, query: str, new_items: list):
    """
    Сохраняет новые результаты текущего запуска.
    """
    data = {
        "query": query,
        "new_result": new_items
    }
    save_json(file_path, data)


def save_old_file(file_path: Path, query: str, old_items: list):
    """
    Сохраняет накопленные результаты.
    """
    data = {
        "query": query,
        "old_result": old_items
    }
    save_json(file_path, data)


def normalize_wrapper_result(result) -> list:
    """
    Приводит результат python-wrapper к списку словарей.

    Возможные варианты:
    - уже list
    - dict с ключами results/result/found/data
    - одиночный dict
    """
    if result is None:
        return []

    if isinstance(result, list):
        return result

    if isinstance(result, dict):
        for key in ("results", "result", "found", "data"):
            value = result.get(key)
            if isinstance(value, list):
                return value

        return [result]

    return []


def perform_leakcheck_lookup(api: LeakCheckAPI, query: str) -> list:
    """
    Выполняет поиск через LeakCheck python-wrapper.

    В зависимости от версии библиотеки сигнатура может отличаться.
    Поэтому реализованы несколько попыток вызова.

    Наиболее типовые варианты:
    - api.lookup(query=query)
    - api.lookup(query)
    - api.search(query)
    - api.search(query=query)
    """
    logging.info(f"LeakCheck wrapper lookup для query='{query}'")

    errors = []

    try:
        result = api.lookup(query=query)
        return normalize_wrapper_result(result)
    except Exception as e:
        errors.append(f"api.lookup(query=query): {e}")

    try:
        result = api.lookup(query)
        return normalize_wrapper_result(result)
    except Exception as e:
        errors.append(f"api.lookup(query): {e}")

    try:
        result = api.search(query=query)
        return normalize_wrapper_result(result)
    except Exception as e:
        errors.append(f"api.search(query=query): {e}")

    try:
        result = api.search(query)
        return normalize_wrapper_result(result)
    except Exception as e:
        errors.append(f"api.search(query): {e}")

    raise RuntimeError(
        "Не удалось выполнить запрос через LeakCheck wrapper. "
        + " | ".join(errors)
    )


def process_query(api: LeakCheckAPI, query: str):
    """
    Выполняет полный цикл обработки одного значения:
    1. Делает запрос через LeakCheck python-wrapper
    2. Получает результаты
    3. Загружает старые результаты
    4. Определяет новые записи
    5. Сохраняет new/old JSON
    """
    safe_query_name = sanitize_filename(query)
    new_file = NEW_DIR / f"{safe_query_name}_new.json"
    old_file = OLD_DIR / f"{safe_query_name}_old.json"

    logging.info(f"Обработка query: {query}")

    try:
        current_items = perform_leakcheck_lookup(api, query)

        logging.info(f"Получено результатов для query='{query}': {len(current_items)}")

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

    except Exception as e:
        logging.error(f"Ошибка для query='{query}': {e}")
        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)


def main():
    """
    Основная функция:
    - настраивает логирование
    - создаёт директории
    - читает API key
    - читает список значений из domains
    - создаёт клиент LeakCheckAPI
    - обрабатывает каждое значение
    """
    setup_logging()

    NEW_DIR.mkdir(parents=True, exist_ok=True)
    OLD_DIR.mkdir(parents=True, exist_ok=True)

    api_key = read_single_value(TOKEN_FILE, required=True)
    queries = read_list(QUERIES_FILE)

    logging.info(f"Загружен LeakCheck token из {TOKEN_FILE}")
    logging.info(f"Загружены queries: {queries}")

    api = LeakCheckAPI(api_key=api_key)

    for query in queries:
        process_query(api, query)


if __name__ == "__main__":
    main()