import requests
import json
import logging
from pathlib import Path


# Пути к входным файлам
TOKEN_FILE = Path("/opt/Digital_Footprint_System/variables/tokens/infosearch")
DOMAINS_FILE = Path("/opt/Digital_Footprint_System/variables/domains")
ZONES_FILE = Path("/opt/Digital_Footprint_System/variables/zone")

# Базовые директории для результатов и логов
BASE_RESULT_DIR = Path("/opt/Digital_Footprint_System/result/infosearch")
NEW_DIR = BASE_RESULT_DIR / "new"
OLD_DIR = BASE_RESULT_DIR / "old"
LOG_DIR = BASE_RESULT_DIR / "logs"
LOG_FILE = LOG_DIR / "infosearch.log"


def setup_logging():
    """
    Настраивает логирование:
    - вывод в консоль
    - запись в файл /opt/Digital_Footprint_System/result/infosearch/logs/infosearch.log

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

    Используется для token, так как в файле должен быть один токен.
    Удаляет лишние пробелы и переводы строки по краям.

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

      Если файл отсутствует, пустой или содержит некорректный JSON,
    функция возвращает пустой список.

    :param file_path: путь к JSON-файлу
    :param result_key: ключ, по которому нужно извлечь список
                       ("old_result" или "new_result")
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


def make_item_key(item) -> str:
    """
    Формирует уникальный ключ для элемента результата.

    Нужен для сравнения записей и определения, какие из них новые.
    Сначала пытается использовать наиболее вероятные уникальные поля:
    - domain
    - email
    - id
    - url
    - phone

    Если таких полей нет, используется полная сериализация объекта в JSON.

    :param item: элемент результата
    :return: строковый уникальный ключ
    """
    if not isinstance(item, dict):
        return str(item)

    for key in ("domain", "email", "id", "url", "phone"):
        if key in item and item[key]:
            return f"{key}:{item[key]}"

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


def save_new_file(file_path: Path, fqdn: str, new_items: list):
    """
    Сохраняет файл с новыми результатами в формате:

    {
        "domain_zone": "abc.ru",
        "new_result": [...]
    }

    :param file_path: путь к файлу
    :param fqdn: домен с зоной, например abc.ru
    :param new_items: список новых записей
    """
    data = {
        "domain_zone": fqdn,
        "new_result": new_items
    }
    save_json(file_path, data)


def save_old_file(file_path: Path, fqdn: str, old_items: list):
    """
    Сохраняет файл со всеми ранее найденными результатами в формате:

    {
        "domain_zone": "abc.ru",
        "old_result": [...]
    }

    :param file_path: путь к файлу
    :param fqdn: домен с зоной, например abc.ru
    :param old_items: накопленный список старых записей
    """
    data = {
        "domain_zone": fqdn,
        "old_result": old_items
    }
    save_json(file_path, data)


def process_request(token: str, domain: str, zone: str):
    """
    Выполняет полный цикл обработки для одного domain.zone:

    1. Формирует URL запроса
    2. Выполняет запрос к API
    3. Получает текущие результаты
    4. Загружает старые результаты из *_old.json
    5. Определяет только новые записи
    6. Сохраняет новые записи в *_new.json
    7. Обновляет накопленный файл *_old.json

    При ошибках:
    - в *_new.json сохраняется пустой список
    - *_old.json сохраняется без изменений

    :param token: токен API
    :param domain: доменное имя без зоны
    :param zone: доменная зона
    """
    fqdn = f"{domain}.{zone}"
    url = f"https://infoapi24.store/api/{token}/search/{fqdn}"

    new_file = NEW_DIR / f"{fqdn}_new.json"
    old_file = OLD_DIR / f"{fqdn}_old.json"

    logging.info(f"Обработка: {fqdn}")
    logging.info(f"Запрос: {url}")

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        current_result = response.json()

        if "result" not in current_result:
            raise ValueError(f"Ключ 'result' не найден в ответе API для {fqdn}")

        current_items = current_result.get("result", [])
        if not isinstance(current_items, list):
            raise ValueError(f"Поле 'result' не является списком для {fqdn}")

        old_items = load_result_items(old_file, "old_result")
        old_keys = {make_item_key(item) for item in old_items}

        new_items = [item for item in current_items if make_item_key(item) not in old_keys]
        updated_old_items = merge_without_duplicates(old_items, new_items)

        save_new_file(new_file, fqdn, new_items)
        save_old_file(old_file, fqdn, updated_old_items)

        if new_items:
            logging.info(f"Найдено новых записей для {fqdn}: {len(new_items)}")
        else:
            logging.info(f"Новых записей для {fqdn} не найдено")

    except requests.exceptions.ConnectionError as e:
        logging.error(f"Ошибка соединения для {fqdn}: {e}")
        save_new_file(new_file, fqdn, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, fqdn, old_items)

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP-ошибка для {fqdn}: {e}")
        save_new_file(new_file, fqdn, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, fqdn, old_items)

    except requests.exceptions.Timeout as e:
        logging.error(f"Таймаут для {fqdn}: {e}")
        save_new_file(new_file, fqdn, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, fqdn, old_items)

    except json.JSONDecodeError as e:
        logging.error(f"Ошибка декодирования JSON для {fqdn}: {e}")
        save_new_file(new_file, fqdn, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, fqdn, old_items)

    except Exception as e:
        logging.error(f"Другая ошибка для {fqdn}: {e}")
        save_new_file(new_file, fqdn, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, fqdn, old_items)


def main():
    """
    Основная функция запуска скрипта.

    Выполняет:
    - настройку логирования
    - создание нужных директорий
    - чтение token, domains и zones из файлов
    - перебор всех комбинаций domain + zone
    - вызов обработки для каждой комбинации
    """
    setup_logging()

    NEW_DIR.mkdir(parents=True, exist_ok=True)
    OLD_DIR.mkdir(parents=True, exist_ok=True)

    token = read_single_value(TOKEN_FILE)
    domains = read_list(DOMAINS_FILE)
    zones = read_list(ZONES_FILE)

    logging.info(f"Загружен token из {TOKEN_FILE}")
    logging.info(f"Загружено domains: {domains}")
    logging.info(f"Загружено zones: {zones}")

    for domain in domains:
        for zone in zones:
            process_request(token, domain, zone)


if __name__ == "__main__":
    main()
