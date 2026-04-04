import json
import logging
import socket
import ssl
from pathlib import Path

import requests
import whois
import OpenSSL
import shodan


# Пути к входным файлам
TOKEN_FILE = Path("/opt/Digital_Footprint_System/variables/tokens/domains_monitor")
SHODAN_TOKEN_FILE = Path("/opt/Digital_Footprint_System/variables/tokens/shodan")
DOMAINS_FILE = Path("/opt/Digital_Footprint_System/variables/domains")

# Базовые директории для результатов и логов
BASE_RESULT_DIR = Path("/opt/Digital_Footprint_System/result/domains-monitor")
NEW_DIR = BASE_RESULT_DIR / "new"
OLD_DIR = BASE_RESULT_DIR / "old"
LOG_DIR = BASE_RESULT_DIR / "logs"
LOG_FILE = LOG_DIR / "domains-monitor.log"


def setup_logging():
    """
    Настраивает логирование:
    - вывод в консоль
    - запись в файл domains-monitor.log

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

    Используется для token и shodan API key.

    :param file_path: путь к файлу
    :param required: если True — выбрасывает исключение при отсутствии файла,
                     если False — возвращает пустую строку
    :return: строка со значением
    """
    if not file_path.exists():
        if required:
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        return ""

    value = file_path.read_text(encoding="utf-8").strip()
    if not value and required:
        raise ValueError(f"Файл пустой: {file_path}")

    return value


def read_list(file_path: Path) -> list[str]:
    """
    Читает список значений из файла.

    Поддерживает:
    - построчный формат
    - значения через запятую
    - смешанный формат

    Убирает дубликаты с сохранением порядка.

    :param file_path: путь к файлу
    :return: список уникальных значений
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
    возвращает пустой список.

    :param file_path: путь к файлу
    :param result_key: ключ, по которому читается список
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


def normalize_to_list(value):
    """
    Приводит значение к списку.

    Если значение уже список — возвращает его.
    Если значение пустое — возвращает пустой список.
    Иначе оборачивает одиночное значение в список.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def format_whois_date(value):
    """
    Приводит whois-дату к строке.

    Whois-библиотека может вернуть:
    - None
    - datetime
    - list[datetime]
    - строку

    :param value: исходное значение
    :return: строка или список строк
    """
    if value is None:
        return "N/A"

    if isinstance(value, list):
        result = []
        for item in value:
            if hasattr(item, "strftime"):
                result.append(item.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                result.append(str(item))
        return result if result else "N/A"

    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    return str(value)


def get_ip_address(domain: str) -> str:
    """
    Получает IP-адрес домена.

    :param domain: домен
    :return: IP или 'N/A'
    """
    try:
        return socket.gethostbyname(domain)
    except Exception:
        return "N/A"


def get_whois_info(domain: str) -> dict:
    """
    Получает WHOIS-информацию по домену через python-whois.

    :param domain: домен
    :return: словарь с whois-данными
    """
    try:
        w = whois.whois(domain)

        whois_info = {
            "domain_name": normalize_to_list(w.domain_name),
            "registrar": w.registrar if getattr(w, "registrar", None) else "N/A",
            "whois_server": w.whois_server if getattr(w, "whois_server", None) else "N/A",
            "referral_url": w.referral_url if getattr(w, "referral_url", None) else "N/A",
            "name_servers": normalize_to_list(w.name_servers),
            "emails": normalize_to_list(w.emails),
            "dnssec": w.dnssec if getattr(w, "dnssec", None) else "N/A",
            "name": w.name if getattr(w, "name", None) else "N/A",
            "org": w.org if getattr(w, "org", None) else "N/A",
            "address": w.address if getattr(w, "address", None) else "N/A",
            "city": w.city if getattr(w, "city", None) else "N/A",
            "state": w.state if getattr(w, "state", None) else "N/A",
            "zipcode": w.zipcode if getattr(w, "zipcode", None) else "N/A",
            "country": w.country if getattr(w, "country", None) else "N/A",
            "creation_date": format_whois_date(getattr(w, "creation_date", None)),
            "updated_date": format_whois_date(getattr(w, "updated_date", None)),
            "expiration_date": format_whois_date(getattr(w, "expiration_date", None)),
            "status": normalize_to_list(getattr(w, "status", None)),
        }

        return whois_info

    except Exception as e:
        logging.warning(f"Не удалось получить WHOIS для {domain}: {e}")
        return {
            "error": str(e)
        }


def get_ssl_info(domain: str) -> dict:
    """
    Проверяет наличие SSL-сертификата у домена и возвращает базовую информацию.

    :param domain: домен
    :return: словарь с информацией о сертификате
    """
    try:
        context = ssl.create_default_context()

        with socket.create_connection((domain, 443), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=domain) as ssock:
                der_cert = ssock.getpeercert(binary_form=True)
                x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_ASN1, der_cert)

                issuer_components = dict(x509.get_issuer().get_components())
                subject_components = dict(x509.get_subject().get_components())

                ssl_info = {
                    "has_ssl": True,
                    "issuer_cn": issuer_components.get(b"CN", b"").decode(errors="ignore") or "N/A",
                    "subject_cn": subject_components.get(b"CN", b"").decode(errors="ignore") or "N/A",
                    "not_before": x509.get_notBefore().decode(errors="ignore"),
                    "not_after": x509.get_notAfter().decode(errors="ignore"),
                    "serial_number": str(x509.get_serial_number()),
                    "version": x509.get_version(),
                }

                return ssl_info

    except Exception as e:
        return {
            "has_ssl": False,
            "error": str(e)
        }


def get_shodan_info(domain: str, shodan_api_key: str) -> dict:
    """
    Получает информацию из Shodan.

    Логика:
    - сначала резолвит домен в IP
    - затем ищет данные по IP в Shodan

    Если API-ключ не указан, проверка пропускается.

    :param domain: домен
    :param shodan_api_key: API key Shodan
    :return: словарь с результатом
    """
    if not shodan_api_key:
        return {
            "enabled": False,
            "message": "Shodan API key не задан"
        }

    ip = get_ip_address(domain)
    if ip == "N/A":
        return {
            "enabled": True,
            "ip": "N/A",
            "found": False,
            "error": "Не удалось определить IP"
        }

    try:
        api = shodan.Shodan(shodan_api_key)
        host = api.host(ip)

        result = {
            "enabled": True,
            "ip": ip,
            "found": True,
            "org": host.get("org", "N/A"),
            "os": host.get("os", "N/A"),
            "isp": host.get("isp", "N/A"),
            "asn": host.get("asn", "N/A"),
            "ports": host.get("ports", []),
            "hostnames": host.get("hostnames", []),
            "domains": host.get("domains", []),
            "country_name": host.get("country_name", "N/A"),
            "city": host.get("city", "N/A"),
            "last_update": host.get("last_update", "N/A"),
            "vulns": list(host.get("vulns", [])) if isinstance(host.get("vulns", []), (list, set, tuple)) else host.get("vulns", []),
        }

        return result

    except shodan.APIError as e:
        logging.warning(f"Shodan API error для {domain}: {e}")
        return {
            "enabled": True,
            "ip": ip,
            "found": False,
            "error": str(e)
        }
    except Exception as e:
        logging.warning(f"Ошибка Shodan для {domain}: {e}")
        return {
            "enabled": True,
            "ip": ip,
            "found": False,
            "error": str(e)
        }


def enrich_domain_info(domain: str, shodan_api_key: str) -> dict:
    """
    Обогащает найденный домен дополнительной информацией:
    - IP
    - WHOIS
    - SSL
    - Shodan

    :param domain: найденный домен
    :param shodan_api_key: API key Shodan
    :return: словарь с дополнительной информацией
    """
    ip = get_ip_address(domain)

    return {
        "domain": domain,
        "ip": ip,
        "whois": get_whois_info(domain),
        "ssl": get_ssl_info(domain),
        "shodan": get_shodan_info(domain, shodan_api_key),
    }


def extract_domain_value(item):
    """
    Пытается извлечь доменное имя из элемента результата API.

    Возможны варианты:
    - элемент уже строка
    - элемент словарь с ключами domain / host / name / value

    :param item: элемент результата
    :return: домен в виде строки или None
    """
    if isinstance(item, str):
        return item.strip()

    if isinstance(item, dict):
        for key in ("domain", "host", "name", "value"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    return None


def make_item_key(item) -> str:
    """
    Формирует уникальный ключ для элемента результата.

    Если у записи можно извлечь домен — используем его.
    Иначе используем сериализацию всего объекта.

    :param item: элемент результата
    :return: уникальный строковый ключ
    """
    domain_value = extract_domain_value(item)
    if domain_value:
        return f"domain:{domain_value}"

    if not isinstance(item, dict):
        return str(item)

    return json.dumps(item, sort_keys=True, ensure_ascii=False)


def merge_without_duplicates(old_items: list, new_items: list) -> list:
    """
    Объединяет старые и новые элементы без дубликатов.

    :param old_items: старые элементы
    :param new_items: новые элементы
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


def save_new_file(file_path: Path, find_domain: str, new_items: list):
    """
    Сохраняет новые результаты в файл.

    :param file_path: путь к файлу
    :param find_domain: исходный поисковый домен
    :param new_items: список новых записей
    """
    data = {
        "find_domain": find_domain,
        "new_result": new_items
    }
    save_json(file_path, data)


def save_old_file(file_path: Path, find_domain: str, old_items: list):
    """
    Сохраняет накопленные результаты в файл.

    :param file_path: путь к файлу
    :param find_domain: исходный поисковый домен
    :param old_items: список накопленных записей
    """
    data = {
        "find_domain": find_domain,
        "old_result": old_items
    }
    save_json(file_path, data)


def process_request(token: str, find_domain: str, shodan_api_key: str):
    """
    Выполняет полный цикл обработки одного find_domain:

    1. Делает запрос к API domains-monitor
    2. Получает текущие результаты
    3. Загружает старые результаты
    4. Определяет новые домены
    5. Обогащает каждый новый домен данными WHOIS / SSL / Shodan
    6. Сохраняет new_result
    7. Обновляет old_result

    :param token: токен domains-monitor
    :param find_domain: значение поиска
    :param shodan_api_key: ключ Shodan
    """
    url = f"https://domains-monitor.com/api/v1/{token}/search/full/{find_domain}/json"

    new_file = NEW_DIR / f"{find_domain}_new.json"
    old_file = OLD_DIR / f"{find_domain}_old.json"

    logging.info(f"Обработка: {find_domain}")
    logging.info(f"Запрос: {url}")

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        current_result = response.json()

        if "domain" not in current_result:
            raise ValueError(f"Ключ 'domain' не найден в ответе API для {find_domain}")

        current_items = current_result.get("domain", [])
        if not isinstance(current_items, list):
            raise ValueError(f"Поле 'domain' не является списком для {find_domain}")

        old_items = load_result_items(old_file, "old_result")
        old_keys = {make_item_key(item) for item in old_items}

        raw_new_items = [
            item for item in current_items
            if make_item_key(item) not in old_keys
        ]

        enriched_new_items = []

        for item in raw_new_items:
            domain_value = extract_domain_value(item)

            enriched_item = {
                "api_item": item,
                "domain_info": None
            }

            if domain_value:
                logging.info(f"Обогащение домена: {domain_value}")
                enriched_item["domain_info"] = enrich_domain_info(domain_value, shodan_api_key)
            else:
                enriched_item["domain_info"] = {
                    "error": "Не удалось извлечь домен из элемента результата"
                }

            enriched_new_items.append(enriched_item)

        updated_old_items = merge_without_duplicates(old_items, enriched_new_items)

        save_new_file(new_file, find_domain, enriched_new_items)
        save_old_file(old_file, find_domain, updated_old_items)

        if enriched_new_items:
            logging.info(f"Найдено новых записей для {find_domain}: {len(enriched_new_items)}")
        else:
            logging.info(f"Новых записей для {find_domain} не найдено")

    except requests.exceptions.ConnectionError as e:
        logging.error(f"Ошибка соединения для {find_domain}: {e}")
        save_new_file(new_file, find_domain, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, find_domain, old_items)

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP-ошибка для {find_domain}: {e}")
        save_new_file(new_file, find_domain, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, find_domain, old_items)

    except requests.exceptions.Timeout as e:
        logging.error(f"Таймаут для {find_domain}: {e}")
        save_new_file(new_file, find_domain, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, find_domain, old_items)

    except json.JSONDecodeError as e:
        logging.error(f"Ошибка декодирования JSON для {find_domain}: {e}")
        save_new_file(new_file, find_domain, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, find_domain, old_items)

    except Exception as e:
        logging.error(f"Другая ошибка для {find_domain}: {e}")
        save_new_file(new_file, find_domain, [])
        old_items = load_result_items(old_file, "old_result")
        save_old_file(old_file, find_domain, old_items)


def main():
    """
    Основная функция:
    - настраивает логирование
    - создаёт директории
    - читает token и список domains
    - опционально читает shodan token
    - обрабатывает каждый find_domain
    """
    setup_logging()

    NEW_DIR.mkdir(parents=True, exist_ok=True)
    OLD_DIR.mkdir(parents=True, exist_ok=True)

    token = read_single_value(TOKEN_FILE, required=True)
    find_domains = read_list(DOMAINS_FILE)
    shodan_api_key = read_single_value(SHODAN_TOKEN_FILE, required=False)

    logging.info(f"Загружен token из {TOKEN_FILE}")
    logging.info(f"Загружены domains: {find_domains}")

    if shodan_api_key:
        logging.info(f"Shodan API key загружен из {SHODAN_TOKEN_FILE}")
    else:
        logging.info("Shodan API key не найден, проверка Shodan будет пропущена")

    for find_domain in find_domains:
        process_request(token, find_domain, shodan_api_key)


if __name__ == "__main__":
    main()