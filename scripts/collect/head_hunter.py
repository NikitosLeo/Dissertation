import json
import logging
from pathlib import Path

import requests


# Пути к входным файлам
TOKEN_FILE = Path("/opt/Digital_Footprint_System/variables/tokens/hh")
USER_AGENT_FILE = Path("/opt/Digital_Footprint_System/variables/tokens/hh_user_agent")
QUERIES_FILE = Path("/opt/Digital_Footprint_System/variables/domains")

# Базовые директории для результатов и логов
BASE_RESULT_DIR = Path("/opt/Digital_Footprint_System/result/hh")
NEW_DIR = BASE_RESULT_DIR / "new"
OLD_DIR = BASE_RESULT_DIR / "old"
LOG_DIR = BASE_RESULT_DIR / "logs"
LOG_FILE = LOG_DIR / "hh.log"

# HH API
HH_API_URL = "https://api.hh.ru/resumes"
PER_PAGE = 100
MAX_PAGES = 10
REQUEST_TIMEOUT = 60


def setup_logging():
    """
    Настраивает логирование:
    - вывод в консоль
    - запись в файл /opt/Digital_Footprint_System/result/hh/logs/hh.log

    Если директории для логов нет, она будет создана автоматически.
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

    Используется для token и User-Agent.

    :param file_path: путь к файлу
    :param required: если True, отсутствие файла или пустое значение вызывает исключение
    :return: строка со значением
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
    - значения через запятую
    - смешанный формат

    Также:
    - удаляет комментарии после символа '#'
    - убирает дубликаты с сохранением порядка

    Пример допустимого формата:
        example
        test, demo
        shop # комментарий

    :param file_path: путь к файлу
    :return: список уникальных значений
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

        parts = line.split(",")
        for part in parts:
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


def load_result_items(file_path: Path, result_key: str) -> list:
    """
    Загружает список результатов из JSON-файла.

    Если файл отсутствует, пустой или содержит некорректный JSON,
    возвращает пустой список.

    :param file_path: путь к файлу
    :param result_key: ключ, по которому извлекается список
    :return: список элементов
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

    :param file_path: путь к файлу
    :param data: данные для записи
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def sanitize_filename(value: str) -> str:
    """
    Преобразует строку в безопасное имя файла.

    :param value: исходная строка
    :return: безопасная строка
    """
    safe_chars = []

    for char in value:
        if char.isalnum() or char in ("-", "_", "."):
            safe_chars.append(char)
        else:
            safe_chars.append("_")

    sanitized = "".join(safe_chars).strip("._")
    return sanitized or "query"


def make_item_key(item) -> str:
    """
    Формирует уникальный ключ для элемента результата HH API.

    Основной идентификатор — поле 'id'.
    Если структура неожиданная, используется сериализация объекта.

    :param item: элемент результата
    :return: уникальный строковый ключ
    """
    if isinstance(item, dict) and item.get("id") is not None:
        return f"id:{item['id']}"

    return json.dumps(item, sort_keys=True, ensure_ascii=False)


def merge_without_duplicates(old_items: list, new_items: list) -> list:
    """
    Объединяет старые и новые элементы без дубликатов.

    :param old_items: старые записи
    :param new_items: новые записи
    :return: объединённый список
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
    Сохраняет новые результаты в файл.

    Формат:
    {
        "query": "value",
        "new_result": [...]
    }

    :param file_path: путь к файлу
    :param query: поисковое значение
    :param new_items: список новых записей
    """
    data = {
        "query": query,
        "new_result": new_items
    }
    save_json(file_path, data)


def save_old_file(file_path: Path, query: str, old_items: list):
    """
    Сохраняет накопленные результаты в файл.

    Формат:
    {
        "query": "value",
        "old_result": [...]
    }

    :param file_path: путь к файлу
    :param query: поисковое значение
    :param old_items: накопленный список записей
    """
    data = {
        "query": query,
        "old_result": old_items
    }
    save_json(file_path, data)


def build_headers(token: str, user_agent: str) -> dict:
    """
    Формирует HTTP-заголовки для HH API.

    :param token: Bearer token
    :param user_agent: корректный HH-User-Agent
    :return: словарь заголовков
    """
    return {
        "Authorization": f"Bearer {token}",
        "HH-User-Agent": user_agent
    }


def fetch_all_resume_results(session: requests.Session, headers: dict, query: str) -> list:
    """
    Получает все доступные результаты поиска резюме с учётом пагинации.

    Параметры:
    - text: поисковое значение
    - per_page: до 100
    - page: номер страницы
    - order_by: relevance

    Ограничение:
    - запрашивается не более MAX_PAGES страниц

    :param session: requests.Session
    :param headers: HTTP-заголовки
    :param query: поисковое значение
    :return: список найденных элементов
    """
    all_items = []
    page = 0

    while page < MAX_PAGES:
        params = {
            "text": query,
            "per_page": PER_PAGE,
            "page": page,
            "order_by": "relevance"
        }

        logging.info(f"HH API запрос: query='{query}', page={page}, per_page={PER_PAGE}")

        response = session.get(
            HH_API_URL,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        result = response.json()
        items = result.get("items", [])

        if not isinstance(items, list):
            raise ValueError(f"Поле 'items' не является списком для query='{query}'")

        all_items.extend(items)

        pages = result.get("pages", 0)
        found = result.get("found", 0)

        logging.info(
            f"Получено записей на странице {page}: {len(items)}; "
            f"всего found={found}, pages={pages}"
        )

        if not pages or page >= pages - 1:
            break

        page += 1

    return all_items


def process_query(session: requests.Session, token: str, user_agent: str, query: str):
    """
    Выполняет полный цикл обработки одного поискового значения:

    1. Делает запросы к HH API
    2. Собирает результаты со страниц
    3. Загружает старые результаты из *_old.json
    4. Определяет только новые записи
    5. Сохраняет новые записи в *_new.json
    6. Обновляет накопленный файл *_old.json

    При ошибках:
    - в *_new.json сохраняется пустой список
    - *_old.json сохраняется без изменений

    :param session: requests.Session
    :param token: HH token
    :param user_agent: HH User-Agent
    :param query: поисковое значение
    """
    safe_query_name = sanitize_filename(query)
    new_file = NEW_DIR / f"{safe_query_name}_new.json"
    old_file = OLD_DIR / f"{safe_query_name}_old.json"

    logging.info(f"Обработка query: {query}")

    try:
        headers = build_headers(token, user_agent)
        current_items = fetch_all_resume_results(session, headers, query)

        logging.info(f"Всего найдено записей для query='{query}': {len(current_items)}")

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
            logging.info(f"Найдено новых резюме для query='{query}': {len(new_items)}")
        else:
            logging.info(f"Новых резюме для query='{query}' не найдено")

    except requests.exceptions.ConnectionError as e:
        logging.error(f"Ошибка соединения для query='{query}': {e}")
        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP-ошибка для query='{query}': {e}")

        response = getattr(e, "response", None)
        if response is not None:
            logging.error(f"Ответ HH API: status={response.status_code}, body={response.text}")

        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)

    except requests.exceptions.Timeout as e:
        logging.error(f"Таймаут для query='{query}': {e}")
        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)

    except json.JSONDecodeError as e:
        logging.error(f"Ошибка декодирования JSON для query='{query}': {e}")
        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)

    except Exception as e:
        logging.error(f"Другая ошибка для query='{query}': {e}")
        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)


def main():
    """
    Основная функция запуска скрипта.

    Выполняет:
    - настройку логирования
    - создание директории результатов
    - чтение token
    - чтение user-agent
    - чтение списка ключевых слов из файла domains
    - обработку каждого поискового значения
    """
    setup_logging()

    NEW_DIR.mkdir(parents=True, exist_ok=True)
    OLD_DIR.mkdir(parents=True, exist_ok=True)

    token = read_single_value(TOKEN_FILE, required=True)
    user_agent = read_single_value(USER_AGENT_FILE, required=True)
    queries = read_list(QUERIES_FILE)

    logging.info(f"Загружен HH token из {TOKEN_FILE}")
    logging.info(f"Загружен HH User-Agent из {USER_AGENT_FILE}")
    logging.info(f"Загружены queries: {queries}")

    with requests.Session() as session:
        for query in queries:
            process_query(session, token, user_agent, query)


if __name__ == "__main__":
    main()