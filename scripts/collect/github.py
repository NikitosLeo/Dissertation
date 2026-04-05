import json
import logging
from pathlib import Path
from urllib.parse import quote_plus

import requests


# Пути к входным файлам
TOKEN_FILE = Path("/opt/Digital_Footprint_System/variables/tokens/github")
QUERIES_FILE = Path("/opt/Digital_Footprint_System/variables/github_queries")

# Базовые директории для результатов и логов
BASE_RESULT_DIR = Path("/opt/Digital_Footprint_System/result/github-monitor")
NEW_DIR = BASE_RESULT_DIR / "new"
OLD_DIR = BASE_RESULT_DIR / "old"
LOG_DIR = BASE_RESULT_DIR / "logs"
LOG_FILE = LOG_DIR / "github-monitor.log"

# GitHub API
GITHUB_API_URL = "https://api.github.com/search/code"
PER_PAGE = 100
REQUEST_TIMEOUT = 60


def setup_logging():
    """
    Настраивает логирование:
    - вывод в консоль
    - запись в файл /opt/Digital_Footprint_System/result/github-monitor/logs/github-monitor.log

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


def read_single_value(file_path: Path) -> str:
    """
    Читает одно значение из файла.

    Используется для GitHub token.

    :param file_path: путь к файлу
    :return: строка со значением
    :raises FileNotFoundError: если файл не существует
    :raises ValueError: если файл пустой
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    value = file_path.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"Файл пустой: {file_path}")

    return value


def read_list(file_path: Path) -> list[str]:
    """
    Читает список значений из файла.

    Поддерживаемые форматы:
    - по одному значению в строке
    - несколько значений через запятую
    - смешанный формат

    Также убирает дубликаты, сохраняя исходный порядок.

    :param file_path: путь к файлу
    :return: список уникальных строк
    :raises FileNotFoundError: если файл не существует
    :raises ValueError: если файл пустой или не содержит корректных значений
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    content = file_path.read_text(encoding="utf-8").strip()
    if not content:
        raise ValueError(f"Файл пустой: {file_path}")

    values = []

    for line in content.splitlines():
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

    Ожидаемый формат:
    {
        "query": "search_value",
        "old_result": [...]
    }

    или:
    {
        "query": "search_value",
        "new_result": [...]
    }

    Если файл отсутствует, пустой или содержит некорректный JSON,
    функция возвращает пустой список.

    :param file_path: путь к файлу
    :param result_key: ключ, по которому нужно извлечь список
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
    Сохраняет данные в JSON-файл в читаемом формате.

    :param file_path: путь к файлу
    :param data: данные для записи
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def sanitize_filename(value: str) -> str:
    """
    Преобразует строку в безопасное имя файла.

    :param value: исходная строка
    :return: безопасная строка для имени файла
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
    Формирует уникальный ключ для элемента GitHub search result.

    Используется комбинация:
    - repository.full_name
    - path
    - sha

    Если структура отличается, используется сериализация объекта.

    :param item: элемент результата
    :return: строковый уникальный ключ
    """
    if isinstance(item, dict):
        repository = item.get("repository", {})
        repository_full_name = repository.get("full_name", "")
        path = item.get("path", "")
        sha = item.get("sha", "")

        if repository_full_name and path and sha:
            return f"{repository_full_name}:{path}:{sha}"

    return json.dumps(item, sort_keys=True, ensure_ascii=False)


def merge_without_duplicates(old_items: list, new_items: list) -> list:
    """
    Объединяет старые и новые записи без дубликатов.

    Сохраняет порядок:
    - сначала все старые записи
    - затем только те новые, которых ещё не было

    :param old_items: список старых записей
    :param new_items: список новых записей
    :return: объединённый список без дублей
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
    Сохраняет файл с новыми результатами.

    Формат:
    {
        "query": "search_value",
        "new_result": [...]
    }

    :param file_path: путь к файлу
    :param query: поисковый запрос
    :param new_items: список новых записей
    """
    data = {
        "query": query,
        "new_result": new_items
    }
    save_json(file_path, data)


def save_old_file(file_path: Path, query: str, old_items: list):
    """
    Сохраняет файл со всеми ранее найденными результатами.

    Формат:
    {
        "query": "search_value",
        "old_result": [...]
    }

    :param file_path: путь к файлу
    :param query: поисковый запрос
    :param old_items: накопленный список старых записей
    """
    data = {
        "query": query,
        "old_result": old_items
    }
    save_json(file_path, data)


def build_headers(token: str) -> dict:
    """
    Формирует HTTP-заголовки для GitHub API.

    :param token: GitHub token
    :return: словарь заголовков
    """
    return {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {token}"
    }


def fetch_all_github_code_results(session: requests.Session, headers: dict, query: str) -> list:
    """
    Получает все результаты GitHub Code Search с учётом пагинации.

    Ограничения:
    - GitHub Search API обычно ограничивает доступную глубину выборки
    - за один запрос можно получить до 100 записей
    - страницы запрашиваются последовательно до исчерпания результатов

    :param session: requests.Session
    :param headers: HTTP-заголовки
    :param query: поисковый запрос
    :return: список найденных элементов
    """
    all_items = []
    page = 1

    while True:
        params = {
            "q": query,
            "per_page": PER_PAGE,
            "page": page
        }

        logging.info(f"GitHub API запрос: query='{query}', page={page}, per_page={PER_PAGE}")

        response = session.get(
            GITHUB_API_URL,
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

        logging.info(f"Получено записей на странице {page}: {len(items)}")

        if len(items) < PER_PAGE:
            break

        page += 1

    return all_items


def process_query(session: requests.Session, token: str, query: str):
    """
    Выполняет полный цикл обработки одного поискового запроса:

    1. Выполняет поиск через GitHub Code Search API
    2. Собирает все страницы результатов
    3. Загружает старые результаты из *_old.json
    4. Определяет только новые записи
    5. Сохраняет новые записи в *_new.json
    6. Обновляет накопленный файл *_old.json

    При ошибках:
    - в *_new.json сохраняется пустой список
    - *_old.json сохраняется без изменений

    :param session: requests.Session
    :param token: GitHub token
    :param query: поисковый запрос
    """
    safe_query_name = sanitize_filename(query)
    new_file = NEW_DIR / f"{safe_query_name}_new.json"
    old_file = OLD_DIR / f"{safe_query_name}_old.json"

    logging.info(f"Обработка query: {query}")

    try:
        headers = build_headers(token)
        current_items = fetch_all_github_code_results(session, headers, query)

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
            logging.info(f"Найдено новых записей для query='{query}': {len(new_items)}")
        else:
            logging.info(f"Новых записей для query='{query}' не найдено")

    except requests.exceptions.ConnectionError as e:
        logging.error(f"Ошибка соединения для query='{query}': {e}")
        save_new_file(new_file, query, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, query, old_items)

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP-ошибка для query='{query}': {e}")

        response = getattr(e, "response", None)
        if response is not None:
            logging.error(f"Ответ GitHub: status={response.status_code}, body={response.text}")

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
    - создание нужных директорий
    - чтение token и списка поисковых запросов
    - создание HTTP-сессии
    - обработку каждого запроса
    """
    setup_logging()

    NEW_DIR.mkdir(parents=True, exist_ok=True)
    OLD_DIR.mkdir(parents=True, exist_ok=True)

    token = read_single_value(TOKEN_FILE)
    queries = read_list(QUERIES_FILE)

    logging.info(f"Загружен GitHub token из {TOKEN_FILE}")
    logging.info(f"Загружены queries: {queries}")

    with requests.Session() as session:
        for query in queries:
            process_query(session, token, query)


if __name__ == "__main__":
    main()