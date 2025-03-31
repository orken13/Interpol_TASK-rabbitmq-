import requests
import time
import json
import random
from datetime import datetime, timedelta
import pika
import logging
import string
import itertools

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Keep your existing country codes list
countries = [
    "AF", "AL", "DZ", "AD", "AO", "AR", "AM", "AU", "AT", "AZ", "BS", "BH", "BD",
    "BB", "BY", "BE", "BZ", "BJ", "BT", "BO", "BA", "BW", "BR", "BN", "BG", "BF",
    "BI", "CV", "KH", "CM", "CA", "CF", "TD", "CL", "CN", "CO", "KM", "CG", "CD",
    "CR", "HR", "CU", "CY", "CZ", "DK", "DJ", "DM", "DO", "EC", "EG", "SV", "GQ",
    "ER", "EE", "SZ", "ET", "FJ", "FI", "FR", "GA", "GM", "GE", "DE", "GH", "GR",
    "GD", "GT", "GN", "GW", "GY", "HT", "VA", "HN", "HU", "IS", "IN", "ID", "IR",
    "IQ", "IE", "IL", "IT", "JM", "JP", "JO", "KZ", "KE", "KI", "KP", "KR", "KW",
    "KG", "LA", "LV", "LB", "LS", "LR", "LY", "LI", "LT", "LU", "MG", "MW", "MY",
    "MV", "ML", "MT", "MH", "MR", "MU", "MX", "FM", "MD", "MC", "MN", "ME", "MA",
    "MZ", "MM", "NA", "NR", "NP", "NL", "NZ", "NI", "NE", "NG", "MK", "NO", "OM",
    "PK", "PW", "PS", "PA", "PG", "PY", "PE", "PH", "PL", "PT", "QA", "RO", "RU",
    "RW", "KN", "LC", "VC", "WS", "SM", "ST", "SA", "SN", "RS", "SC", "SL", "SG",
    "SK", "SI", "SB", "SO", "ZA", "SS", "ES", "LK", "SD", "SR", "SE", "CH", "SY",
    "TJ", "TZ", "TH", "TL", "TG", "TO", "TT", "TN", "TR", "TM", "TV", "UG", "UA",
    "AE", "GB", "US", "UY", "UZ", "VU", "VE", "VN", "YE", "ZM", "ZW"
]

# Enhanced search parameters
# More granular age ranges for better coverage
age_ranges = [
    (18, 25), (26, 30), (31, 35), (36, 40), (41, 45),
    (46, 50), (51, 55), (56, 60), (61, 65), (66, 70),
    (71, 100)
]
gender_types = ["M", "F"]

# Extended name search configurations
common_letters = list(string.ascii_lowercase)  # All lowercase letters
common_prefixes = [
    "a", "al", "an", "b", "be", "c", "ch", "d", "e", "el",
    "f", "g", "h", "i", "j", "k", "l", "m", "mo", "n",
    "o", "p", "q", "r", "s", "sh", "t", "u", "v", "w", "x", "y", "z"
]

# Common name combinations for targeted searches
common_name_patterns = [
    "mohammed", "muhammad", "alexander", "ahmad", "ali", "jose", "john", "david",
    "maria", "anna", "zhang", "wang", "li", "chen", "singh", "kumar", "khan",
    "smith", "johnson", "williams", "brown", "jones", "garcia", "martinez",
    "rodriguez", "lee", "kim", "nguyen", "patel"
]

# Extended Accept-Language values for better rotation
accept_languages = [
    "en-US,en;q=0.9",
    "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
    "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "hi-IN,hi;q=0.9,en-US;q=0.8,en;q=0.7",
    "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
    "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    "sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7"
]

# Expanded user agents list
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Edge/133.0.0.0",
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (HTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/119.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
]

# Global RabbitMQ connection
rabbitmq_connection = None
rabbitmq_channel = None
RABBITMQ_QUEUE = "deneme103"


def setup_rabbitmq():

    global rabbitmq_connection, rabbitmq_channel
    try:
        rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        rabbitmq_channel = rabbitmq_connection.channel()
        rabbitmq_channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        logger.info(" RabbitMQ connection established")
        return True
    except Exception as e:
        logger.error(f" Failed to establish RabbitMQ connection: {str(e)}")
        return False


def publish_single_record(record):
    """Publish a single record to RabbitMQ"""
    global rabbitmq_connection, rabbitmq_channel


    if rabbitmq_connection is None or rabbitmq_connection.is_closed:
        if not setup_rabbitmq():
            logger.error(" Cannot publish record - RabbitMQ connection failed")
            return False

    try:
        rabbitmq_channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(record),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Makes message persistent
            )
        )
        logger.info(f" Published record to RabbitMQ: {record.get('Entity_id')} - {record.get('Name')}")
        return True
    except Exception as e:
        logger.error(f" Failed to publish record to RabbitMQ: {str(e)}")

        if setup_rabbitmq():
            try:

                rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key=RABBITMQ_QUEUE,
                    body=json.dumps(record),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                    )
                )
                logger.info(f" Published record to RabbitMQ after reconnection: {record.get('Entity_id')}")
                return True
            except Exception as retry_e:
                logger.error(f" Failed to publish record after reconnection: {str(retry_e)}")
        return False


def get_random_headers():
    """Generate random headers to avoid detection"""
    return {
        "authority": "ws-public.interpol.int",
        "method": "GET",
        "scheme": "https",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": random.choice(accept_languages),
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "DNT": "1",
        "Host": "ws-public.interpol.int",
        "Origin": "https://www.interpol.int",
        "Pragma": "no-cache",
        "Referer": "https://www.interpol.int/",
        "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Google Chrome\";v=\"133\", \"Chromium\";v=\"133\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": random.choice(user_agents)
    }


def handle_request_exception(e, retry_delays=[5, 10, 15, 30, 60], current_retry=0):
    """Handle request exceptions with exponential backoff"""
    if current_retry >= len(retry_delays):
        logger.error(f"Maximum retry count reached. Operation failed: {str(e)}")
        return False

    delay = retry_delays[current_retry]
    logger.warning(
        f"Connection error: {str(e)}. Waiting {delay} seconds... (Attempt {current_retry + 1}/{len(retry_delays)})")
    time.sleep(delay)
    return current_retry + 1


def make_api_request(url, max_retries=5):
    """Make API request with retries and error handling"""
    retry_count = 0
    consecutive_failures = 0

    while retry_count < max_retries:
        current_headers = get_random_headers()

        try:
            # Random delay to avoid detection
            sleep_time = random.uniform(1.8, 4.2)
            time.sleep(sleep_time)

            # Make request with timeout
            response = requests.get(url, headers=current_headers, timeout=20)

            # Handle response status
            if response.status_code != 200:
                consecutive_failures += 1
                logger.error(f"ERROR: HTTP {response.status_code} - URL: {url}")

                # Handle specific status codes
                if response.status_code == 429:  # Too Many Requests
                    wait_time = 120 + random.randint(20, 60)
                    logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                elif response.status_code >= 500:  # Server error
                    wait_time = 30 + random.randint(10, 30)
                    logger.warning(f"Server error. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    wait_time = 15 + random.randint(5, 15)
                    logger.warning(f"Unexpected error code: {response.status_code}. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)

                retry_count += 1
                continue

            # Reset failures counter on success
            consecutive_failures = 0

            # Try to parse JSON response
            try:
                return response.json()
            except json.JSONDecodeError as je:
                logger.error(f"JSON parsing error: {str(je)} - URL: {url}")
                retry_count += 1
                time.sleep(5)
                continue

        except requests.exceptions.RequestException as e:
            retry_result = handle_request_exception(e, current_retry=retry_count)
            if retry_result is False:
                return None
            retry_count = retry_result

    return None


def find_red_notices_multi_approach():
    """Find red notices using multiple search approaches to maximize data collection"""
    unique_entity_ids = set()
    all_red_notices = []
    search_stats = {}


    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"interpol_scraping_{timestamp}.log"


    if not setup_rabbitmq():
        logger.error("Failed to set up RabbitMQ. Will continue without publishing.")
#Yaklaşım
    approaches = [
        ("nationality", process_country_searches),
        ("age_gender", process_age_gender_searches),
        ("name_prefix", process_name_searches),
        ("date_based1", process_advanced_date_searches1),
        ("common_name", process_common_name_searches),
        ("special_char", process_special_char_searches),
        ("combined_filters", process_combined_searches),
        ("date_based", process_advanced_date_searches),
    ]


    for approach_name, approach_func in approaches:
        logger.info(f"\n===== STARTING: {approach_name} approach =====")

        try:
            notices, approach_stats = approach_func(unique_entity_ids)


            search_stats[approach_name] = approach_stats


            all_red_notices.extend(notices)

            logger.info(f"{approach_name} approach completed: {len(notices)} new records found")
            logger.info(f"Total so far: {len(unique_entity_ids)} unique records")


            with open(f"interpol_{approach_name}_{timestamp}.json", "w", encoding="utf-8") as f:
                json.dump(notices, f, ensure_ascii=False, indent=2)


            pause_time = random.uniform(15, 30)
            logger.info(f"Waiting {pause_time:.1f} seconds before next approach...")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"ERROR: Problem occurred in {approach_name} approach: {str(e)}")
            with open(log_file, "a") as log:
                log.write(f"ERROR: {approach_name}: {str(e)}\n")


    with open(f"interpol_all_notices_{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump(all_red_notices, f, ensure_ascii=False, indent=2)


    total_notices = len(all_red_notices)


    with open(f"interpol_stats_{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump({
            "total_notices": total_notices,
            "unique_notices": len(unique_entity_ids),
            "approach_stats": search_stats,
            "timestamp": timestamp
        }, f, indent=2)


    logger.info("\n===== SEARCH STATISTICS =====")
    logger.info(f"Total unique persons found: {len(unique_entity_ids)}")
    for approach, stats in search_stats.items():
        logger.info(f"{approach} approach: {stats.get('total_found', 0)} records found")

    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        rabbitmq_connection.close()
        logger.info("RabbitMQ connection closed.")

    return all_red_notices


def process_advanced_date_searches1(unique_ids):
    """Process searches with more specific date ranges and criteria to find unique notices"""
    notices = []
    stats = {"by_date_range": {}, "total_found": 0}

    # Daha spesifik tarih aralıkları (daha kısa zaman dilimleri)
    date_ranges = [


        ("1996-01-01", "1998-12-31"),
        ("1990-01-01", "1992-12-31"),


        #############

    ]

    # Updated country groups focusing on countries with higher numbers of Red Notices
    country_groups = [
        ["RU", "VE", "CO", "MX", "BR"],  # Countries known for high Red Notice counts
        ["TR", "UA", "RO", "BG", "RS"],  # Eastern Europe/Turkey region
        ["CN", "VN", "TH", "IN", "PK"],  # Asia with high counts
        ["NG", "ZA", "DZ", "MA", "EG"],  # Africa countries with significant notices
        ["IR", "IQ", "SY", "AF", "PK"],  # Middle East/Central Asia
        ["US", "GB", "FR", "DE", "IT"]  # Western countries (for comparison)
    ]

    # Adjusting physical features to emphasize male subjects (more male profiles)
    physical_features = [
        {"ageMin": "18", "ageMax": "25", "sexId": "M"},
        {"ageMin": "26", "ageMax": "35", "sexId": "M"},
        {"ageMin": "36", "ageMax": "45", "sexId": "M"},
        {"ageMin": "46", "ageMax": "55", "sexId": "M"},
        {"ageMin": "56", "ageMax": "100", "sexId": "M"},
        # Reduced female profiles (just keeping two age ranges for comparison)
        {"ageMin": "18", "ageMax": "35", "sexId": "F"},
        {"ageMin": "36", "ageMax": "100", "sexId": "F"}
    ]


    # Her tarih aralığı için
    for start_date, end_date in date_ranges:
        date_range_key = f"{start_date}_to_{end_date}"
        date_notices = []
        stats["by_date_range"][date_range_key] = 0

        # Her fiziksel özellik kombinasyonu için
        for features in physical_features:
            # Ülke grupları ile kombine et
            for country_group in country_groups:
                # Her ülke için
                for country in country_group:
                    logger.info(
                        f"\n--- Retrieving notices for date {start_date} to {end_date}, country {country}, features {features} ---")

                    # Parametreleri oluştur
                    params = {
                        "arrestWarrantDate": start_date,
                        "nationality": country,
                        **features
                    }

                    # URL'yi oluştur
                    param_string = "&".join([f"{k}={v}" for k, v in params.items()])

                    page_num = 1
                    more_pages = True

                    # Tüm sayfaları tara
                    while more_pages:
                        api_url = f"https://ws-public.interpol.int/notices/v1/red?{param_string}&page={page_num}&resultPerPage=160"

                        data = make_api_request(api_url)
                        if data is None:
                            logger.error(f"API request failed for {param_string} - Page {page_num}")
                            break

                        # Veri var mı kontrol et
                        if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                                data["_embedded"]["notices"]) == 0:
                            logger.info(f"No data found for {param_string} - Page {page_num}")
                            break

                        # Notları işle
                        notices_count = len(data["_embedded"]["notices"])
                        new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids,
                                                                  date_notices, notices)

                        # Yeni kişi eklendi mi kontrol et
                        if new_notices_count > 0:
                            logger.info(
                                f"Success: {param_string} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                            # Sonraki sayfa var mı?
                            if "_links" in data and "next" in data["_links"]:
                                page_num += 1
                            else:
                                more_pages = False
                        else:
                            logger.info(f"No new persons for {param_string} - Page {page_num}")
                            more_pages = False  # Yeni kişi yoksa sonraki sayfayı aramaya gerek yok

                    # Ülkeler arası bekle
                    pause_time = random.uniform(2.0, 5.0)
                    logger.info(f"Waiting {pause_time:.1f} seconds...")
                    time.sleep(pause_time)

            # Özellik kombinasyonları arası daha uzun bekle
            pause_time = random.uniform(5.0, 10.0)
            logger.info(f"Completed feature set. Waiting {pause_time:.1f} seconds...")
            time.sleep(pause_time)

        # İstatistikleri güncelle
        stats["by_date_range"][date_range_key] = len(date_notices)
        stats["total_found"] += len(date_notices)

        # Tarih aralıkları arası daha uzun bekle
        pause_time = random.uniform(15.0, 25.0)
        logger.info(f"Completed date range {start_date} to {end_date}. Waiting {pause_time:.1f} seconds...")
        time.sleep(pause_time)

    return notices, stats


def process_advanced_date_searches(unique_ids):
    """Process searches with more specific date ranges and criteria to find unique notices"""
    notices = []
    stats = {"by_date_range": {}, "total_found": 0}

    # Daha spesifik tarih aralıkları (daha kısa zaman dilimleri)
    date_ranges = [


        ("1996-01-01", "1998-12-31"),
        ("1990-01-01", "1992-12-31"),

        ("2005-01-01", "2007-12-31"),

        #############

    ]

    # Farklı fiziksel özellik kombinasyonları (sadece tek bir özellik kullanmak yerine)
    physical_features = [
        {"ageMin": "18", "ageMax": "25", "sexId": "M"},
        {"ageMin": "26", "ageMax": "35", "sexId": "M"},
        {"ageMin": "36", "ageMax": "45", "sexId": "M"},
        {"ageMin": "46", "ageMax": "55", "sexId": "M"},
        {"ageMin": "56", "ageMax": "100", "sexId": "M"},
            {"ageMin": "18", "ageMax": "25", "sexId": "F"},
            {"ageMin": "26", "ageMax": "35", "sexId": "F"},
            {"ageMin": "36", "ageMax": "45", "sexId": "F"},
            {"ageMin": "46", "ageMax": "55", "sexId": "F"},
            {"ageMin": "56", "ageMax": "100", "sexId": "F"}
    ]

    # Farklı ülkeler ve nüfus için ülke grupları
    country_groups = [
        ["US", "CA", "MX", "BR", "AR"],  # Kuzey ve Güney Amerika
        ["GB", "FR", "DE", "IT", "ES"],  # Batı Avrupa
        ["RU", "UA", "BY", "PL", "RO"],  # Doğu Avrupa
        ["CN", "JP", "KR", "IN", "ID"],  # Asya
        ["EG", "ZA", "NG", "MA", "DZ"],  # Afrika
        ["AU", "NZ", "PG", "FJ", "SB"]  # Okyanusya
    ]




    # Her tarih aralığı için
    for start_date, end_date in date_ranges:
        date_range_key = f"{start_date}_to_{end_date}"
        date_notices = []
        stats["by_date_range"][date_range_key] = 0

        # Her fiziksel özellik kombinasyonu için
        for features in physical_features:
            # Ülke grupları ile kombine et
            for country_group in country_groups:
                # Her ülke için
                for country in country_group:
                    logger.info(
                        f"\n--- Retrieving notices for date {start_date} to {end_date}, country {country}, features {features} ---")

                    # Parametreleri oluştur
                    params = {
                        "arrestWarrantDate": start_date,
                        "nationality": country,
                        **features
                    }

                    # URL'yi oluştur
                    param_string = "&".join([f"{k}={v}" for k, v in params.items()])

                    page_num = 1
                    more_pages = True

                    # Tüm sayfaları tara
                    while more_pages:
                        api_url = f"https://ws-public.interpol.int/notices/v1/red?{param_string}&page={page_num}&resultPerPage=160"

                        data = make_api_request(api_url)
                        if data is None:
                            logger.error(f"API request failed for {param_string} - Page {page_num}")
                            break

                        # Veri var mı kontrol et
                        if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                                data["_embedded"]["notices"]) == 0:
                            logger.info(f"No data found for {param_string} - Page {page_num}")
                            break

                        # Notları işle
                        notices_count = len(data["_embedded"]["notices"])
                        new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids,
                                                                  date_notices, notices)

                        # Yeni kişi eklendi mi kontrol et
                        if new_notices_count > 0:
                            logger.info(
                                f"Success: {param_string} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                            # Sonraki sayfa var mı?
                            if "_links" in data and "next" in data["_links"]:
                                page_num += 1
                            else:
                                more_pages = False
                        else:
                            logger.info(f"No new persons for {param_string} - Page {page_num}")
                            more_pages = False  # Yeni kişi yoksa sonraki sayfayı aramaya gerek yok

                    # Ülkeler arası bekle
                    pause_time = random.uniform(2.0, 5.0)
                    logger.info(f"Waiting {pause_time:.1f} seconds...")
                    time.sleep(pause_time)

            # Özellik kombinasyonları arası daha uzun bekle
            pause_time = random.uniform(5.0, 10.0)
            logger.info(f"Completed feature set. Waiting {pause_time:.1f} seconds...")
            time.sleep(pause_time)

        # İstatistikleri güncelle
        stats["by_date_range"][date_range_key] = len(date_notices)
        stats["total_found"] += len(date_notices)

        # Tarih aralıkları arası daha uzun bekle
        pause_time = random.uniform(15.0, 25.0)
        logger.info(f"Completed date range {start_date} to {end_date}. Waiting {pause_time:.1f} seconds...")
        time.sleep(pause_time)

    return notices, stats
def process_country_searches(unique_ids):
    """Process searches based on nationality"""
    notices = []
    stats = {"by_country": {}, "total_found": 0}

    # Create smaller batches of countries to better manage API connections
    country_batches = [countries[i:i + 10] for i in range(0, len(countries), 10)]

    for country_batch in country_batches:
        for country_code in country_batch:
            try:
                logger.info(f"\n--- Retrieving red notices for country {country_code} ---")

                country_notices = []
                page_num = 1
                more_pages = True

                # Iterate through all pages
                while more_pages:
                    api_url = f"https://ws-public.interpol.int/notices/v1/red?nationality={country_code}&page={page_num}&resultPerPage=160"

                    data = make_api_request(api_url)
                    if data is None:
                        logger.error(f"API request failed: {country_code} - Page {page_num}")
                        break

                    # Check if data exists
                    if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                            data["_embedded"]["notices"]) == 0:
                        logger.info(f"{country_code} - No more pages or no data found.")
                        break

                    # Process the notices
                    notices_count = len(data["_embedded"]["notices"])
                    new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, country_notices,
                                                              notices)

                    logger.info(
                        f"{country_code} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                    # Check for next page
                    if "_links" in data and "next" in data["_links"]:
                        page_num += 1
                    else:
                        more_pages = False

                # Update country statistics
                stats["by_country"][country_code] = len(country_notices)
                stats["total_found"] += len(country_notices)

                logger.info(f"Total {len(country_notices)} persons found for {country_code}")

                # Pause between countries
                pause_time = random.uniform(3.5, 8.5)
                logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
                time.sleep(pause_time)

            except Exception as e:
                logger.error(f"ERROR: Problem occurred while retrieving data for {country_code}: {str(e)}")

        # Pause between country batches
        batch_pause = random.uniform(8, 15)
        logger.info(f"Completed a batch of countries. Waiting {batch_pause:.1f} seconds...")
        time.sleep(batch_pause)

    return notices, stats





def process_age_gender_searches(unique_ids):
    """Process searches based on age ranges and gender"""
    notices = []
    stats = {"by_gender": {"M": 0, "F": 0}, "by_age": {}, "total_found": 0}

    # Iterate over gender and age ranges
    for gender in gender_types:
        for age_min, age_max in age_ranges:
            try:
                logger.info(f"\n--- Searching for Gender: {gender}, Age range: {age_min}-{age_max} ---")

                search_notices = []
                page_num = 1
                more_pages = True

                # Iterate through pages
                while more_pages:
                    api_url = f"https://ws-public.interpol.int/notices/v1/red?ageMin={age_min}&ageMax={age_max}&sexId={gender}&page={page_num}&resultPerPage=160"

                    data = make_api_request(api_url)
                    if data is None:
                        logger.error(
                            f"API request failed: Gender {gender}, Age {age_min}-{age_max} - Page {page_num}")
                        break

                    # Check if data exists
                    if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                            data["_embedded"]["notices"]) == 0:
                        logger.info(
                            f"Gender {gender}, Age {age_min}-{age_max} - No more pages or no data found.")
                        break

                    # Process notices
                    notices_count = len(data["_embedded"]["notices"])
                    new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, search_notices,
                                                              notices)

                    logger.info(
                        f"Gender {gender}, Age {age_min}-{age_max} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                    # Check for next page
                    if "_links" in data and "next" in data["_links"]:
                        page_num += 1
                    else:
                        more_pages = False

                # Update statistics
                age_key = f"{age_min}-{age_max}"
                if age_key not in stats["by_age"]:
                    stats["by_age"][age_key] = 0
                stats["by_age"][age_key] += len(search_notices)
                stats["by_gender"][gender] += len(search_notices)
                stats["total_found"] += len(search_notices)

                logger.info(
                    f"Total {len(search_notices)} persons found for Gender {gender}, Age {age_min}-{age_max}")

                # Pause between searches
                pause_time = random.uniform(3.5, 8.5)
                logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
                time.sleep(pause_time)

            except Exception as e:
                logger.error(
                    f"ERROR: Problem occurred while retrieving data for Gender {gender}, Age {age_min}-{age_max}: {str(e)}")

    return notices, stats


def process_name_searches(unique_ids):
    """Process searches based on name prefixes with enhanced scope"""
    notices = []
    stats = {"by_letter": {}, "by_prefix": {}, "total_found": 0}

    # Search by letter prefixes (short form searches)
    for letter in common_letters:
        try:
            logger.info(f"\n--- Searching for names starting with '{letter}' ---")

            letter_notices = []
            page_num = 1
            more_pages = True

            # Iterate through pages for name field
            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?name={letter}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Letter {letter} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Letter {letter} (name field) - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, letter_notices,
                                                          notices)

                logger.info(
                    f"Letter {letter} (name field) - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Iterate through pages for forename field
            page_num = 1
            more_pages = True

            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?forename={letter}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Forename {letter} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Forename {letter} - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, letter_notices,
                                                          notices)

                logger.info(
                    f"Forename {letter} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Update statistics
            stats["by_letter"][letter] = len(letter_notices)
            stats["total_found"] += len(letter_notices)

            logger.info(f"Total {len(letter_notices)} persons found for letter {letter}")

            # Pause between searches
            pause_time = random.uniform(4, 9)
            logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"ERROR: Problem occurred while retrieving data for letter {letter}: {str(e)}")

    # Search by common name prefixes (more precise searches)
    for prefix in common_prefixes:
        # Skip if already searched by single letter
        if len(prefix) == 1:
            continue

        try:
            logger.info(f"\n--- Searching for names with prefix '{prefix}' ---")

            prefix_notices = []
            page_num = 1
            more_pages = True

            # Iterate through pages for name field
            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?name={prefix}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Prefix {prefix} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Prefix {prefix} (name field) - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, prefix_notices,
                                                          notices)

                logger.info(
                    f"Prefix {prefix} (name field) - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Also search in forename field
            page_num = 1
            more_pages = True

            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?forename={prefix}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Prefix {prefix} (forename) - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Prefix {prefix} (forename) - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, prefix_notices,
                                                          notices)

                logger.info(
                    f"Prefix {prefix} (forename) - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Update statistics
            stats["by_prefix"][prefix] = len(prefix_notices)
            stats["total_found"] += len(prefix_notices)

            logger.info(f"Total {len(prefix_notices)} persons found for prefix {prefix}")

            # Pause between searches
            pause_time = random.uniform(4, 9)
            logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"ERROR: Problem occurred while retrieving data for prefix {prefix}: {str(e)}")

    return notices, stats


def process_common_name_searches(unique_ids):
    """Process searches for common names across different cultures"""
    notices = []
    stats = {"by_common_name": {}, "total_found": 0}

    # Search for specific common names
    for name_pattern in common_name_patterns:
        try:
            logger.info(f"\n--- Searching for common name '{name_pattern}' ---")

            name_notices = []
            page_num = 1
            more_pages = True

            # Iterate through pages for name field
            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?name={name_pattern}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Name {name_pattern} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Name {name_pattern} - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, name_notices,
                                                        notices)

                logger.info(
                    f"Name {name_pattern} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Also search in forename field
            page_num = 1
            more_pages = True

            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?forename={name_pattern}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Forename {name_pattern} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Forename {name_pattern} - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, name_notices,
                                                        notices)

                logger.info(
                    f"Forename {name_pattern} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Update statistics
            stats["by_common_name"][name_pattern] = len(name_notices)
            stats["total_found"] += len(name_notices)

            logger.info(f"Total {len(name_notices)} persons found for name {name_pattern}")

            # Pause between searches
            pause_time = random.uniform(4, 9)
            logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"ERROR: Problem occurred while retrieving data for name {name_pattern}: {str(e)}")

    return notices, stats

def process_special_char_searches(unique_ids):
    """Process searches based on special characters in names"""
    notices = []
    stats = {"by_special_char": {}, "total_found": 0}

    # Özel karakter ve alfabe setleri
    special_chars = [

        "á", "é", "í", "ó", "ú", "ü", "ñ",
    ]

    for char in special_chars:
        try:
            logger.info(f"\n--- Searching for names with special character: {char} ---")

            char_notices = []
            page_num = 1
            more_pages = True

            # Iterate through pages for name field
            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?name={char}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Special char {char} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Special char {char} (name field) - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, char_notices,
                                                        notices)

                logger.info(
                    f"Special char {char} (name field) - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Also search in forename field
            page_num = 1
            more_pages = True

            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?forename={char}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Special char {char} (forename) - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Special char {char} (forename) - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, char_notices,
                                                        notices)

                logger.info(
                    f"Special char {char} (forename) - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Update statistics
            stats["by_special_char"][char] = len(char_notices)
            stats["total_found"] += len(char_notices)

            logger.info(f"Total {len(char_notices)} persons found for special character {char}")

            # Pause between searches
            pause_time = random.uniform(4, 9)
            logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"ERROR: Problem occurred while retrieving data for special character {char}: {str(e)}")

    return notices, stats



def process_combined_searches(unique_ids):
    """Process searches using combined filters for more precision"""
    notices = []
    stats = {"by_combination": {}, "total_found": 0}

    # Use combinations of filters for more targeted searches
    # Example: Gender + Age + Country combinations
    combinations = [
        # Sample of higher-priority combinations
        {"gender": "M", "age_min": 25, "age_max": 40, "country": "RU"},
        {"gender": "M", "age_min": 41, "age_max": 60, "country": "RU"},
        {"gender": "M", "age_min": 41, "age_max": 60, "country": "IN"},
        {"gender": "F", "age_min": 25, "age_max": 40, "country": "RU"},

    ]

    for combo in combinations:
        try:
            gender = combo["gender"]
            age_min = combo["age_min"]
            age_max = combo["age_max"]
            country = combo["country"]

            combo_key = f"{gender}-{age_min}-{age_max}-{country}"
            logger.info(f"\n--- Searching with combination: {combo_key} ---")

            combo_notices = []
            page_num = 1
            more_pages = True

            # Iterate through pages
            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?ageMin={age_min}&ageMax={age_max}&sexId={gender}&nationality={country}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Combination {combo_key} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Combination {combo_key} - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, combo_notices,
                                                          notices)

                logger.info(
                    f"Combination {combo_key} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Update statistics
            stats["by_combination"][combo_key] = len(combo_notices)
            stats["total_found"] += len(combo_notices)

            logger.info(f"Total {len(combo_notices)} persons found for combination {combo_key}")

            # Pause between searches
            pause_time = random.uniform(4, 9)
            logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"ERROR: Problem occurred while retrieving data for combination {combo_key}: {str(e)}")

    return notices, stats

def process_name_searches(unique_ids):
    """Process searches based on name prefixes with enhanced scope"""
    notices = []
    stats = {"by_letter": {}, "by_prefix": {}, "total_found": 0}

    # Search by letter prefixes (short form searches)
    for letter in common_letters:
        try:
            logger.info(f"\n--- Searching for names starting with '{letter}' ---")

            letter_notices = []
            page_num = 1
            more_pages = True

            # Iterate through pages for name field
            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?name={letter}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Letter {letter} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Letter {letter} (name field) - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, letter_notices,
                                                          notices)

                logger.info(
                    f"Letter {letter} (name field) - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Iterate through pages for forename field
            page_num = 1
            more_pages = True

            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?forename={letter}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Forename {letter} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Forename {letter} - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, letter_notices,
                                                          notices)

                logger.info(
                    f"Forename {letter} - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Update statistics
            stats["by_letter"][letter] = len(letter_notices)
            stats["total_found"] += len(letter_notices)

            logger.info(f"Total {len(letter_notices)} persons found for letter {letter}")

            # Pause between searches
            pause_time = random.uniform(4, 9)
            logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"ERROR: Problem occurred while retrieving data for letter {letter}: {str(e)}")

    # Search by common name prefixes (more precise searches)
    for prefix in common_prefixes:
        # Skip if already searched by single letter
        if len(prefix) == 1:
            continue

        try:
            logger.info(f"\n--- Searching for names with prefix '{prefix}' ---")

            prefix_notices = []
            page_num = 1
            more_pages = True

            # Iterate through pages for name field
            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?name={prefix}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Prefix {prefix} - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Prefix {prefix} (name field) - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, prefix_notices,
                                                          notices)

                logger.info(
                    f"Prefix {prefix} (name field) - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Also search in forename field
            page_num = 1
            more_pages = True

            while more_pages:
                api_url = f"https://ws-public.interpol.int/notices/v1/red?forename={prefix}&page={page_num}&resultPerPage=160"

                data = make_api_request(api_url)
                if data is None:
                    logger.error(f"API request failed: Prefix {prefix} (forename) - Page {page_num}")
                    break

                # Check if data exists
                if "_embedded" not in data or "notices" not in data["_embedded"] or len(
                        data["_embedded"]["notices"]) == 0:
                    logger.info(f"Prefix {prefix} (forename) - No more pages or no data found.")
                    break

                # Process notices
                notices_count = len(data["_embedded"]["notices"])
                new_notices_count = process_notices_batch(data["_embedded"]["notices"], unique_ids, prefix_notices,
                                                          notices)

                logger.info(
                    f"Prefix {prefix} (forename) - Page {page_num}: {notices_count} persons found, {new_notices_count} new persons added")

                # Check for next page
                if "_links" in data and "next" in data["_links"]:
                    page_num += 1
                else:
                    more_pages = False

            # Update statistics
            stats["by_prefix"][prefix] = len(prefix_notices)
            stats["total_found"] += len(prefix_notices)

            logger.info(f"Total {len(prefix_notices)} persons found for prefix {prefix}")

            # Pause between searches
            pause_time = random.uniform(4, 9)
            logger.info(f"Waiting {pause_time:.1f} seconds against API rate limiting...")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"ERROR: Problem occurred while retrieving data for prefix {prefix}: {str(e)}")

    return notices, stats


def process_notices_batch(notices_data, unique_ids, category_notices, all_notices):
    """Process a batch of notices and update collections + immediately publish to RabbitMQ"""
    new_notices_count = 0

    for notice in notices_data:
        entity_id = notice.get('entity_id', '')

        # Skip if already seen
        if entity_id in unique_ids:
            continue

        # Add to unique set
        unique_ids.add(entity_id)
        new_notices_count += 1

        # Get thumbnail URL
        thumbnail_url = notice.get('_links', {}).get('thumbnail', {}).get('href', '')

        # Create record
        notice_data = {
            "Name": f"{notice.get('forename', '')} {notice.get('name', '')}".strip(),
            "Age": notice.get('date_of_birth', ''),
            "Nationalities": notice.get('nationalities', []),
            "Entity_id": entity_id,
            "Gender": notice.get('sex_id', ''),
            "Wanted_by": notice.get('entity_id', '').split('/')[0] if '/' in notice.get('entity_id', '') else '',
            "Thumbnail_url": thumbnail_url
        }

        # Add to collections
        category_notices.append(notice_data)
        all_notices.append(notice_data)

        # Immediately publish to RabbitMQ - this is the key change
        publish_single_record(notice_data)

        # Print to console
        print(json.dumps(notice_data, ensure_ascii=False))

    return new_notices_count


# Main execution
if __name__ == "__main__":
    try:
        logger.info("Starting Interpol Red Notice data collection...")
        all_notices = find_red_notices_multi_approach()
        logger.info(f"Data collection completed. Total records found: {len(all_notices)}")
    except Exception as e:
        logger.error(f"Critical error in main execution: {str(e)}")
        # Make sure to close RabbitMQ connection in case of error
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            rabbitmq_connection.close()
            logger.info("RabbitMQ connection closed due to error.")


