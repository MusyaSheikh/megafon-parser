import asyncio
import random
import string
import re
import base64
import logging
import json
import urllib.parse
from typing import Optional, List, Tuple
from curl_cffi.requests import AsyncSession
from datetime import datetime

# Настройка логирования
LOG_FILE = "megafon.log"
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8', mode='w')  # mode='w' перезаписывает
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

logger = logging.getLogger('megafon')
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)


def log_request(method: str, url: str, headers: dict = None, body: dict = None):
    """Логирует запрос в файл"""
    logger.debug(f">>> {method} {url}")
    if headers:
        logger.debug(f"    Headers: {json.dumps(headers, ensure_ascii=False)}")
    if body:
        logger.debug(f"    Body: {json.dumps(body, ensure_ascii=False)}")


def log_response(status: int, text: str, truncate: int = 1000):
    """Логирует ответ в файл"""
    body = text[:truncate] + "..." if len(text) > truncate else text
    logger.debug(f"<<< Status: {status}")
    logger.debug(f"    Response: {body}")


def log_info(message: str):
    """Логирует информацию"""
    logger.info(message)


def log_error(message: str):
    """Логирует ошибку"""
    logger.error(message)


RUCAPTCHA_KEY = "YOUR_RUCAPTCHA_KEY"  # Вставьте свой ключ с rucaptcha.com

REGIONS = {
    'altay': '738', 'amur': '495', 'arhangelsk': '655', 'astrakhan': '255', 'bel': '815', 'brn': '596',
    'vl': '615', 'volgograd': '97', 'vologda': '656', 'vrn': '835', 'eao': '455', 'chita': '519', 'iv': '658',
    'irkutsk': '496', 'kaliningrad': '657', 'klg': '616', 'kamchatka': '517', 'kem': '742', 'kirov': '477',
    'kis': '855', 'kstr': '695', 'krasnodar': '756', 'kras': '675', 'kurgan': '415', 'ks': '535', 'lc': '875',
    'magadan': '516', 'moscow': '3', 'murmansk': '696', 'nn': '356', 'chelny': '376', 'novgorod': '697',
    'nkz': '737', 'nsk': '555', 'omsk': '736', 'orenburg': '275', 'orl': '617', 'penza': '315', 'perm': '215',
    'prim': '75', 'pskov': '698', 'adygea': '895', 'altrep': '741', 'bashkortostan': '156', 'buryatia': '518',
    'dagestan': '1015', 'ingushetia': '1035', 'kbr': '775', 'kalmykia': '375', 'kchr': '955', 'karelia': '699',
    'komi': '478', 'mariel': '255', 'mordovia': '335', 'sakha': '521', 'alania': '975', 'tatarstan': '116',
    'tyva': '740', 'hakas': '739', 'rostov': '995', 'rzn': '536', 'samara': '12', 'spb': '14',
    'saratov': '175', 'skh': '520', 'svr': '36', 'sml': '700', 'sochi': '915', 'stavropol': '795',
    'syzran': '12', 'tmb': '935', 'tver': '701', 'tlt': '12', 'tom': '735', 'tula': '595', 'tyumen': '395',
    'udm': '476', 'ulyanovsk': '635', 'khb': '60', 'xmao': '136', 'chel': '195', 'cher': '715',
    'chechnya': '1036', 'chuvashia': '475', 'chukotka': '435', 'yanao': '416', 'yar': '702'
}

all_numbers = set()
lock = asyncio.Lock()

LIMIT = 44


async def solve_captcha(session: AsyncSession, captcha_html: str, city: str = "") -> Optional[str]:
    """Решает капчу через rucaptcha (без прокси)"""
    captcha_base64 = captcha_html

    if "<img" in captcha_html:
        match = re.search(r'src=\\?"([^"]+)\\?"', captcha_html)
        if match:
            captcha_base64 = match.group(1)

    if "base64," in captcha_base64:
        captcha_base64 = captcha_base64.split("base64,")[1]

    match = re.match(r'^([A-Za-z0-9+/=]+)', captcha_base64)
    if match:
        captcha_base64 = match.group(1)

    try:
        img_data = base64.b64decode(captcha_base64)
        with open(f"captcha_{city}.jpg", "wb") as f:
            f.write(img_data)
    except:
        pass

    in_url = "http://rucaptcha.com/in.php"
    data = {
        "key": RUCAPTCHA_KEY,
        "method": "base64",
        "body": captcha_base64,
        "json": "1",
        "numeric": "0",
    }

    # Используем отдельную сессию без прокси для rucaptcha
    try:
        async with AsyncSession(timeout=30) as captcha_session:
            response = await captcha_session.post(in_url, data=data)
            result = response.json()

            if result.get("status") != 1:
                print(f"[{city}] Captcha error: {result.get('error_text', result)}")
                return None

            captcha_id = result.get("request")
            print(f"[{city}] Captcha ID: {captcha_id}")

            res_url = "http://rucaptcha.com/res.php"
            params = {"key": RUCAPTCHA_KEY, "action": "get", "id": captcha_id, "json": "1"}

            for i in range(20):
                await asyncio.sleep(5)
                response = await captcha_session.get(res_url, params=params)
                result = response.json()

                if result.get("status") == 1:
                    code = result.get("request")
                    print(f"[{city}] Captcha solved: {code}")
                    return code
                if result.get("request") != "CAPCHA_NOT_READY":
                    print(f"[{city}] Captcha failed: {result}")
                    return None

            print(f"[{city}] Captcha timeout")
            return None
    except Exception as e:
        print(f"[{city}] Captcha exception: {e}")
        return None


def parse_numbers(result: dict) -> list:
    numbers = []

    for section in ['regular', 'vip']:
        if section in result:
            for class_data in result[section].get('numbers', []):
                phones = class_data.get('phones', [])
                numbers.extend([str(p) for p in phones])

    if 'payload' in result:
        for item in result['payload'].get('msisdns', []):
            if 'msisdn' in item:
                numbers.append(str(item['msisdn']))

    return numbers


async def worker_fetch(
    worker_id: int,
    proxy: str,
    city: str,
    branch_id: str,
    base_url: str,
    number_classes: list,
    masks: list,
    result_list: list
):
    """Воркер обрабатывает свой список масок"""

    if not masks:
        return

    tag = f"[W{worker_id}][{city}]"
    print(f"{tag} Старт воркера, масок: {len(masks)}")
    log_info(f"{tag} Worker start, masks: {masks}, proxy: {proxy}")

    try:
        async with AsyncSession(impersonate="safari17_0", proxy=proxy, timeout=20) as session:
            # Генерация ID для кук (эмуляция JS-трекеров)
            device_uuid = f"{random.randint(10000000,99999999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(100000000000,999999999999)}"
            ym_uid = str(random.randint(1000000000000000, 9999999999999999))
            ym_d = str(int(datetime.now().timestamp()))
            st_uid = ''.join(random.choices('0123456789abcdef', k=32))
            domain_sid = ''.join(random.choices(string.ascii_letters + string.digits, k=20))

            # Инициализация сессии - собираем куки
            cookies = {
                "branchId": branch_id,  # Важная кука региона
                "screenType": "Desktop",
                "isEmployee": "0",
                "homeRegion": city,
                # Яндекс.Метрика
                "_ym_uid": ym_uid,
                "_ym_d": ym_d,
                "_ym_isad": "1",
                # Трекеры
                "mindboxDeviceUUID": device_uuid,
                "directCrm-session": urllib.parse.quote(json.dumps({"deviceGuid": device_uuid})),
                "st_uid": st_uid,
                "domain_sid": f"{domain_sid}:{int(datetime.now().timestamp() * 1000)}",
                "tmr_lvid": st_uid[:32],
                "tmr_lvidTS": str(int(datetime.now().timestamp() * 1000)),
                "tmr_detect": "0%7C" + str(int(datetime.now().timestamp() * 1000)),
                "cookie_toast": "cookie_toast",
            }
            page_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            }

            # 1. Главная страница
            log_request("GET", base_url, page_headers)
            response = await session.get(base_url, headers=page_headers, cookies=cookies, allow_redirects=True, timeout=20)
            for c in response.cookies.jar:
                cookies[c.name] = c.value
            log_response(response.status_code, response.text)
            await asyncio.sleep(random.uniform(0.5, 1))

            # 2. Сначала fullnumber
            fullnumber_url = f"{base_url}/connect/chnumber/fullnumber"
            page_headers["Referer"] = base_url + "/"
            page_headers["Sec-Fetch-Site"] = "same-origin"
            log_request("GET", fullnumber_url, page_headers)
            response = await session.get(fullnumber_url, headers=page_headers, cookies=cookies, allow_redirects=True, timeout=20)
            for c in response.cookies.jar:
                cookies[c.name] = c.value
            log_response(response.status_code, response.text)
            await asyncio.sleep(random.uniform(0.3, 0.6))

            # 3. Затем lnumber - RSC-запрос (Next.js client navigation)
            # С ретраями при ошибке 404
            api_referer = f"{base_url}/connect/chnumber/lnumber"
            lnumber_success = False

            for lnumber_attempt in range(5):  # До 5 попыток
                rsc_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
                lnumber_url = f"{base_url}/connect/chnumber/lnumber?_rsc={rsc_id}"

                # next-router-state-tree для перехода fullnumber -> lnumber
                router_state = json.dumps(
                    ["", {"children": [[f"branchName", city, "d"], {"children": ["connect", {"children": ["chnumber", {"children": [["slug", "fullnumber", "oc"], {"children": ["__PAGE__", {}, None, None]}, None, None]}, None, None, True]}, None, None]}, None, None]}, None, None, True],
                    separators=(',', ':')
                )

                rsc_headers = {
                    "Accept": "*/*",
                    "Accept-Language": "ru",
                    "Referer": fullnumber_url,
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                    "rsc": "1",
                    "next-router-state-tree": urllib.parse.quote(router_state),
                    "next-url": f"/{city}/connect/chnumber/fullnumber",
                }

                log_request("GET", lnumber_url, rsc_headers)
                log_info(f"{tag} Cookies sent: {list(cookies.keys())}")
                response = await session.get(lnumber_url, headers=rsc_headers, cookies=cookies, allow_redirects=True, timeout=20)
                for c in response.cookies.jar:
                    cookies[c.name] = c.value
                log_response(response.status_code, response.text)

                if response.status_code == 200:
                    lnumber_success = True
                    break
                elif response.status_code == 404:
                    wait_time = 3 + lnumber_attempt * 2  # 3, 5, 7, 9, 11 секунд
                    print(f"{tag} lnumber 404, ретрай {lnumber_attempt + 1}/5 через {wait_time}с...")
                    log_info(f"{tag} lnumber 404, retry {lnumber_attempt + 1}/5 in {wait_time}s")
                    await asyncio.sleep(wait_time)

                    # Перезагружаем fullnumber перед повторной попыткой
                    log_request("GET", fullnumber_url, page_headers)
                    response = await session.get(fullnumber_url, headers=page_headers, cookies=cookies, allow_redirects=True, timeout=20)
                    for c in response.cookies.jar:
                        cookies[c.name] = c.value
                    log_response(response.status_code, response.text)
                    await asyncio.sleep(random.uniform(0.5, 1))
                else:
                    # Другие ошибки - пробуем продолжить
                    print(f"{tag} lnumber {response.status_code}, пробую продолжить")
                    log_info(f"{tag} lnumber {response.status_code}, trying to continue")
                    break

            if not lnumber_success:
                print(f"{tag} lnumber не загрузился после 5 попыток, пробую API напрямую")
                log_info(f"{tag} lnumber failed after 5 attempts, trying API directly")

            await asyncio.sleep(random.uniform(0.5, 1))

            # API headers
            api_headers = {
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Origin": base_url,
                "Referer": api_referer,
                "X-Branch-Id": branch_id,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }

            # Body для первого запроса (С classes)
            body_first = {
                "captchaCode": "",
                "branchId": int(branch_id),
                "currentTab": "favoriteNumber",
                "classes": {"numberClasses": number_classes}
            }

            # Body для последующих запросов (БЕЗ classes)
            body_next = {
                "captchaCode": "",
                "branchId": int(branch_id),
                "currentTab": "favoriteNumber"
            }

            # Обрабатываем каждую маску
            for mask in masks:
                mask_numbers = []

                # 1. Первый запрос - получаем классы которые есть для этой маски
                api_url = f"{base_url}/api/msisdn/msisdn?offset=0&limit={LIMIT}&mask={mask}"
                api_headers["X-Ecom-Request-Trace-Id"] = ''.join(random.choices(string.ascii_lowercase + string.digits, k=20))

                result, cookies = await self_request_with_captcha(session, api_url, api_headers, body_first, cookies, city, worker_id, mask)

                if not result:
                    continue

                # Собираем номера и определяем какие классы есть
                found_classes = {}  # classType -> count
                for section in ['regular', 'vip']:
                    if section in result:
                        for item in result[section].get('numbers', []):
                            class_type = item.get('classType')
                            phones = item.get('phones', [])
                            if phones:
                                mask_numbers.extend([str(p) for p in phones])
                                found_classes[class_type] = len(phones)

                # Показываем какие классы будут дозагружаться
                need_more = [f"{ct}:{cnt}" for ct, cnt in found_classes.items() if cnt >= LIMIT]
                if need_more:
                    print(f"{tag}[{mask}] +{len(mask_numbers)}, дозагрузка: {need_more}")
                else:
                    print(f"{tag}[{mask}] +{len(mask_numbers)} (все)")
                log_info(f"{tag}[{mask}] First request: {len(mask_numbers)} numbers, classes: {found_classes}")

                # 2. Для каждого класса с >= LIMIT номерами - загружаем остальные
                for class_type, count in found_classes.items():
                    if count < LIMIT:
                        continue  # Уже все номера этого класса получены

                    offset = LIMIT
                    while True:
                        api_url = f"{base_url}/api/msisdn/msisdn?classIds={class_type}&limit={LIMIT+1}&offset={offset}&mask={mask}"
                        api_headers["X-Ecom-Request-Trace-Id"] = ''.join(random.choices(string.ascii_lowercase + string.digits, k=20))

                        result, cookies = await self_request_with_captcha(session, api_url, api_headers, body_next, cookies, city, worker_id, mask)

                        if not result:
                            break

                        # Проверяем есть ли номера
                        has_numbers = False
                        for section in ['regular', 'vip']:
                            if section in result:
                                for item in result[section].get('numbers', []):
                                    phones = item.get('phones', [])
                                    if phones:
                                        has_numbers = True
                                        mask_numbers.extend([str(p) for p in phones])

                        if not has_numbers:
                            break  # Номера закончились

                        offset += LIMIT + 1
                        await asyncio.sleep(random.uniform(0.3, 0.7))

                if mask_numbers:
                    result_list.extend(mask_numbers)
                    print(f"{tag}[{mask}] Итого: +{len(mask_numbers)}")
                    log_info(f"{tag}[{mask}] Total: {len(mask_numbers)} numbers")
                else:
                    log_info(f"{tag}[{mask}] No numbers found")

                await asyncio.sleep(random.uniform(0.5, 1))

        log_info(f"{tag} Worker finished, total numbers: {len(result_list)}")

    except Exception as e:
        print(f"{tag} Ошибка: {e}")
        log_error(f"{tag} Worker error: {e}")


async def self_request_with_captcha(session, url, headers, body, cookies, city, worker_id, mask):
    """Делает запрос с обработкой капчи, возвращает (result, updated_cookies)"""
    body = body.copy()
    cookies = cookies.copy()
    tag = f"[W{worker_id}][{city}][{mask}]"

    for attempt in range(5):
        try:
            # Логируем запрос в файл
            log_request("POST", url, headers, body)
            log_info(f"{tag} Cookies: {list(cookies.keys())}")

            response = await session.post(url, headers=headers, cookies=cookies, json=body, timeout=30)

            # Обновляем куки
            for c in response.cookies.jar:
                cookies[c.name] = c.value

            # Логируем ответ в файл
            log_response(response.status_code, response.text)

            if response.status_code == 404:
                log_info(f"{tag} 404, attempt {attempt+1}")
                if attempt < 2:  # Пробуем 3 раза для 404
                    await asyncio.sleep(2 + attempt * 2)
                    continue
                log_info(f"{tag} 404 - не найдено после {attempt+1} попыток")
                return None, cookies

            if response.status_code >= 500:
                log_error(f"{tag} Server error {response.status_code}, attempt {attempt+1}")
                await asyncio.sleep(5 + attempt * 3)
                continue

            if not response.text or len(response.text) < 10:
                log_error(f"{tag} Empty response")
                await asyncio.sleep(2)
                continue

            result = response.json()

            # Проверка на капчу (409)
            if response.status_code == 409 or ("errors" in result and "payload" in result and "captcha" in result.get("payload", {})):
                print(f"{tag} Капча...")
                log_info(f"{tag} Captcha required")
                captcha_code = await solve_captcha(session, result["payload"]["captcha"], city)
                if captcha_code:
                    body["captchaCode"] = captcha_code
                    print(f"{tag} Повтор с кодом капчи...")
                    log_info(f"{tag} Retrying with captcha code: {captcha_code}")
                    await asyncio.sleep(2)
                    continue
                print(f"{tag} Капча не решена")
                log_error(f"{tag} Captcha not solved")
                return None, cookies

            if "errors" in result and result["errors"]:
                log_error(f"{tag} API errors: {result['errors']}")
                await asyncio.sleep(2)
                continue

            log_info(f"{tag} Success")
            return result, cookies

        except Exception as e:
            print(f"{tag} Exception: {e}")
            log_error(f"{tag} Exception: {e}")
            await asyncio.sleep(3)

    log_error(f"{tag} Max attempts reached")
    return None, cookies


async def fetch_region(city: str, proxies: List[str], masks: List[str], all_proxies: List[str] = None) -> list:
    """Получает номера региона для всех масок, распределяя по воркерам"""
    global all_numbers

    branch_id = REGIONS[city]
    base_url = f"https://{city}.shop.megafon.ru"
    num_workers = len(proxies)

    print(f"\n[{city}] Старт: {num_workers} воркеров, {len(masks)} масок...")

    # Сначала получаем классы номеров (с ротацией прокси при ошибке)
    number_classes = None
    classes_url = f"https://api.shop.megafon.ru/catalog/v1/showcases/1/branches/{branch_id}/numbers/classes"

    # Собираем все доступные прокси для попыток: сначала региональные, потом из общего пула
    proxies_to_try = list(proxies)
    if all_proxies:
        for p in all_proxies:
            if p not in proxies_to_try:
                proxies_to_try.append(p)

    working_proxy_idx = 0  # Индекс рабочего прокси для этого региона

    for attempt, proxy in enumerate(proxies_to_try):
        if proxy is None:
            continue
        try:
            proxy_short = proxy.split('@')[-1] if '@' in proxy else proxy.replace('http://', '').replace('socks5://', '')
            print(f"[{city}] Получение классов (попытка {attempt + 1}, прокси: {proxy_short})...")
            log_info(f"[{city}] Getting classes, attempt {attempt + 1}, proxy: {proxy_short}")

            async with AsyncSession(impersonate="chrome120", proxy=proxy, timeout=20) as session:
                headers = {"Accept": "application/json"}
                response = await session.get(classes_url, headers=headers, timeout=20)

                if response.status_code == 200:
                    classes_result = response.json()
                    number_classes = classes_result.get("payload", {}).get("numberClasses", [])
                    if number_classes:
                        print(f"[{city}] Классы: найдено {len(number_classes)} классов")
                        # Заменяем первый прокси на рабочий
                        if attempt > 0 and attempt < len(proxies):
                            print(f"[{city}] Заменяем нерабочий прокси #{1} на #{attempt + 1}")
                            proxies[0] = proxy
                        working_proxy_idx = attempt
                        break
                    else:
                        print(f"[{city}] Классы пусты, пробую другой прокси...")
                else:
                    print(f"[{city}] Классы: status={response.status_code}, пробую другой прокси...")

        except Exception as e:
            err_short = str(e)[:80].replace('\n', ' ')
            print(f"[{city}] Ошибка получения классов: {err_short}")
            log_error(f"[{city}] Classes error with proxy {proxy}: {e}")

        # Небольшая пауза перед следующей попыткой (ротация прокси)
        await asyncio.sleep(1)

    if not number_classes:
        print(f"[{city}] Не удалось получить классы после {len(proxies_to_try)} попыток, пропускаю регион")
        return []

    result_list = []

    # Распределяем маски по воркерам
    masks_per_worker = [[] for _ in range(num_workers)]
    for i, mask in enumerate(masks):
        masks_per_worker[i % num_workers].append(mask)

    print(f"[{city}] Маски по воркерам: {masks_per_worker}")

    # Запуск воркеров параллельно
    tasks = []
    for i in range(num_workers):
        if masks_per_worker[i]:
            task = worker_fetch(
                worker_id=i + 1,
                proxy=proxies[i],
                city=city,
                branch_id=branch_id,
                base_url=base_url,
                number_classes=number_classes,
                masks=masks_per_worker[i],
                result_list=result_list
            )
            tasks.append(task)

    await asyncio.gather(*tasks)

    # Добавляем уникальные номера
    new_count = 0
    async with lock:
        for num in result_list:
            if num not in all_numbers:
                all_numbers.add(num)
                new_count += 1

    print(f"[{city}] Готово: +{new_count} уникальных (всего собрано: {len(result_list)})")
    return result_list


def select_regions():
    print("\n=== Регионы ===")
    cities = list(REGIONS.keys())

    for i, city in enumerate(cities, 1):
        print(f"{i:2}. {city}", end="  ")
        if i % 5 == 0:
            print()

    print("\n\nВведите номера через запятую, диапазон (1-10) или 'all' для всех:")
    choice = input("> ").strip().lower()

    if choice == 'all':
        return cities

    selected = []
    for part in choice.split(','):
        part = part.strip()
        if '-' in part:
            start, end = part.split('-')
            for i in range(int(start), int(end) + 1):
                if 1 <= i <= len(cities):
                    selected.append(cities[i - 1])
        elif part.isdigit():
            idx = int(part) - 1
            if 0 <= idx < len(cities):
                selected.append(cities[idx])
        elif part in REGIONS:
            selected.append(part)

    return list(set(selected))


def load_proxies(filename: str, proxy_type: str) -> List[str]:
    """Загружает прокси из файла с указанным типом.

    Формат в файле: ip:port или user:pass@ip:port
    proxy_type: 'http' или 'socks5'
    """
    try:
        with open(filename, 'r') as f:
            proxies = []
            for line in f:
                p = line.strip()
                if p:
                    # Убираем протокол если есть
                    if '://' in p:
                        p = p.split('://', 1)[1]

                    # Добавляем нужный протокол
                    if proxy_type == 'socks5':
                        proxies.append(f'socks5://{p}')
                    else:
                        proxies.append(f'http://{p}')
            return proxies
    except:
        return []


def load_masks(filename: str) -> List[str]:
    try:
        with open(filename, 'r') as f:
            masks = [line.strip() for line in f if line.strip()]
            return masks
    except:
        return []


async def check_proxy(proxy: str, index: int) -> Tuple[str, bool]:
    """Проверяет работоспособность прокси на сайте мегафона"""
    # Определяем тип прокси
    if proxy.startswith('socks5'):
        ptype = "SOCKS5"
    elif proxy.startswith('https://'):
        ptype = "HTTPS"
    else:
        ptype = "HTTP"

    proxy_short = proxy.split('@')[-1] if '@' in proxy else proxy.replace('http://', '').replace('https://', '').replace('socks5://', '')

    try:
        async with AsyncSession(impersonate="chrome120", proxy=proxy, timeout=20) as session:
            response = await session.get("https://moscow.shop.megafon.ru", allow_redirects=True)
            if response.status_code == 200:
                print(f"  [{index}] ✓ {ptype} {proxy_short}")
                return proxy, True
            else:
                print(f"  [{index}] ✗ {ptype} {proxy_short} -> status {response.status_code}")
    except Exception as e:
        err = str(e)[:50].replace('\n', ' ')
        print(f"  [{index}] ✗ {ptype} {proxy_short} -> {err}")
    return proxy, False


async def check_all_proxies(proxies: List[str], max_concurrent: int = 50) -> List[str]:
    """Проверяет все прокси параллельно (с лимитом) и возвращает рабочие"""
    print(f"\nПроверка прокси ({len(proxies)} шт, {max_concurrent} потоков)...")

    semaphore = asyncio.Semaphore(max_concurrent)

    async def check_with_limit(proxy, index):
        async with semaphore:
            return await check_proxy(proxy, index)

    tasks = [check_with_limit(p, i+1) for i, p in enumerate(proxies)]
    results = await asyncio.gather(*tasks)

    working = [p for p, ok in results if ok]
    print(f"\nРабочих прокси: {len(working)}/{len(proxies)}")

    return working


async def main():
    print(f"\n=== Megafon Parser ===")
    print(f"Лог файл: {LOG_FILE}")
    log_info("=" * 50)
    log_info("Megafon Parser started")

    # Загружаем маски (обязательно)
    masks = load_masks("mask.txt")
    if not masks:
        print("Ошибка: файл mask.txt не найден или пуст!")
        print("Создайте файл mask.txt с масками (по одной на строку):")
        print("  6666")
        print("  4444")
        print("  7777")
        return

    print(f"Загружено масок: {len(masks)}")
    print(f"Маски: {', '.join(masks)}")
    log_info(f"Masks loaded: {masks}")

    regions = select_regions()
    if not regions:
        print("Регионы не выбраны")
        return

    print(f"\nВыбрано регионов: {len(regions)}")

    # Прокси - тип
    print("\nТип прокси в файле proxies.txt:")
    print("  1. HTTP (ip:port или user:pass@ip:port)")
    print("  2. SOCKS5 (ip:port или user:pass@ip:port)")
    print("  3. Без прокси")
    proxy_type_choice = input("> ").strip()

    proxies = []
    proxy_type = 'http'

    if proxy_type_choice == "1":
        proxy_type = 'http'
        proxies = load_proxies("proxies.txt", proxy_type)
        if proxies:
            print(f"Загружено {len(proxies)} HTTP прокси")
        else:
            print("Файл proxies.txt не найден или пуст")
            return
    elif proxy_type_choice == "2":
        proxy_type = 'socks5'
        proxies = load_proxies("proxies.txt", proxy_type)
        if proxies:
            print(f"Загружено {len(proxies)} SOCKS5 прокси")
        else:
            print("Файл proxies.txt не найден или пуст")
            return
    elif proxy_type_choice == "3":
        proxies = [None]
        print("Работа без прокси")
    else:
        print("Неверный выбор")
        return

    # Количество потоков на регион
    if proxies[0] is not None:
        print(f"\nКоличество потоков на регион (доступно прокси: {len(proxies)}):")
        try:
            threads_per_region = int(input("> ").strip())
            if threads_per_region < 1:
                threads_per_region = 1
        except ValueError:
            threads_per_region = 1
            print("Используется 1 поток")

        total_threads_needed = threads_per_region * len(regions)
        print(f"\nВсего нужно потоков: {threads_per_region} × {len(regions)} регионов = {total_threads_needed}")

        # Проверяем прокси
        proxies = await check_all_proxies(proxies)
        if not proxies:
            print("Нет рабочих прокси!")
            return

        if len(proxies) < total_threads_needed:
            print(f"\n⚠ Внимание: рабочих прокси ({len(proxies)}) меньше чем нужно ({total_threads_needed})")
            print("Варианты:")
            print(f"  1. Продолжить с {len(proxies)} потоками (прокси будут переиспользоваться)")
            print("  2. Уменьшить потоки до {:.0f} на регион".format(len(proxies) / len(regions)))
            print("  3. Отмена")
            choice = input("> ").strip()

            if choice == "2":
                threads_per_region = len(proxies) // len(regions)
                if threads_per_region < 1:
                    threads_per_region = 1
                print(f"Используется {threads_per_region} потоков на регион")
            elif choice == "3":
                return
            # choice == "1" - продолжаем как есть
    else:
        threads_per_region = 1

    print(f"\nСтарт: {len(regions)} регионов × {threads_per_region} потоков = {len(regions) * threads_per_region} всего")
    print(f"Рабочих прокси: {len(proxies) if proxies[0] else 0}")
    print("-" * 50)
    log_info(f"Starting: {len(regions)} regions, {threads_per_region} threads/region, {len(masks)} masks")
    log_info(f"Regions: {regions}")
    log_info(f"Working proxies: {len(proxies) if proxies[0] else 0}")

    # Распределяем прокси по регионам
    # Каждый регион получает свой УНИКАЛЬНЫЙ набор прокси
    # Например: регион 1 = прокси 0-9, регион 2 = прокси 10-19 и т.д.
    proxy_index = 0
    region_proxy_map = {}

    for city in regions:
        region_proxies = []
        for i in range(threads_per_region):
            if proxies[0] is None:
                region_proxies.append(None)
            else:
                # Берём уникальный прокси для этого региона
                region_proxies.append(proxies[proxy_index % len(proxies)])
                proxy_index += 1
        region_proxy_map[city] = region_proxies

    # Показываем распределение прокси по регионам
    if proxies[0] is not None:
        print(f"\nРаспределение прокси по регионам:")
        for city, rp in region_proxy_map.items():
            proxy_ids = [proxies.index(p) + 1 if p in proxies else '?' for p in rp]
            print(f"  {city}: прокси #{proxy_ids}")
        print()
        log_info(f"Proxy distribution: {len(regions)} regions × {threads_per_region} proxies each")

    for city in regions:
        log_info(f"Processing region: {city}")
        await fetch_region(city, region_proxy_map[city], masks, all_proxies=proxies)

    # Сохранение
    print("-" * 50)
    print(f"Всего уникальных номеров: {len(all_numbers)}")
    log_info(f"Total unique numbers: {len(all_numbers)}")

    if all_numbers:
        filename = f"numbers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(filename, 'w') as f:
            for num in sorted(all_numbers):
                f.write(f"{num}\n")
        print(f"Сохранено в: {filename}")
        log_info(f"Numbers saved to: {filename}")

    log_info("Megafon Parser finished")
    log_info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
