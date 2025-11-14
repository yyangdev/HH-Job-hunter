from dataclasses import dataclass, asdict
from datetime import datetime
import logging
from functools import wraps
import json
from pathlib import Path
import requests
import time
from typing import List, Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# HH API URL
url = "https://api.hh.ru/vacancies"

@dataclass
class VacancyData:
    title: str
    url: str
    salary_from: Optional[int]
    salary_to: Optional[int]
    salary_currency: Optional[str]
    salary_gross: Optional[bool]
    retrieved_at: str

    @classmethod
    def api_response(cls, vacancy_data: Dict[str, Any]) -> Optional['VacancyData']:
        try:
            title = vacancy_data.get('name','')
            url = vacancy_data.get('alternate_url', '')
            
            salary_info = vacancy_data.get('salary')
            salary_from = salary_info.get('from') if salary_info else None
            salary_to = salary_info.get('to') if salary_info else None
            salary_currency = salary_info.get('currency') if salary_info else None
            salary_gross = salary_info.get('gross') if salary_info else None

            retrieved_at = datetime.now().isoformat()

            return cls(
                title = title,
                url = url,
                salary_from = salary_from,
                salary_to = salary_to,
                salary_currency = salary_currency,
                salary_gross = salary_gross,
                retrieved_at = retrieved_at
            )

        except Exception as e:
            logger.error(f"Ошибка при создании VacancyData: {e}")
            return None
def retry_request(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    retryable_status_codes: List[int] = [429, 500, 502, 503, 504]
):
    def decorator(func: callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            
            while retries <= max_retries:
                try:
                    result = func(*args, **kwargs)
                    
                    # Если функция возвращает response объект
                    if hasattr(result, 'status_code'):
                        if result.status_code == 200:
                            return result
                        elif result.status_code in retryable_status_codes:
                            logger.warning(f"Получен статус {result.status_code}, попытка {retries + 1}/{max_retries}")
                        else:
                            return result  # Не retry-able ошибка
                    else:
                        return result  # Если функция возвращает не response
                        
                except (requests.exceptions.RequestException, 
                       requests.exceptions.Timeout,
                       requests.exceptions.ConnectionError) as e:
                    logger.warning(f"Ошибка сети: {e}, попытка {retries + 1}/{max_retries}")
                
                # Если достигли максимума попыток, выходим
                if retries == max_retries:
                    logger.error(f"Превышено максимальное количество попыток ({max_retries})")
                    return None
                
                # Ждем перед следующей попыткой (экспоненциальная backoff задержка)
                time.sleep(delay)
                delay *= backoff_factor
                retries += 1
                
            return None
        return wrapper
    return decorator

@retry_request(max_retries=3, initial_delay=1.0, backoff_factor=2.0)
def make_request(url: str, params: Dict[str, Any]) -> Optional[requests.Response]:
    return requests.get(url, params=params, timeout=10)


def fetch_hh_vac(url: str, page: int) -> Optional[Dict[str, Any]]:

    query_params = {
        "text": "python OR SQL OR fastapi",
        "per_page": 100,
        "page": page,
        "area": 1,
        "only_with_salary": True,
    }
    
    try:
        response = make_request(url, query_params)
        
        if not response:
            logger.error(f"Не удалось выполнить запрос для страницы {page}")
            return None
        
        if response.status_code != 200:
            logger.error(f"Ошибка HTTP {response.status_code} для страницы {page}")
            return None
        
        logger.info(f"Вакансии успешно со страницы {page+1} получены!")
        return response.json()
        
    except requests.exceptions.Timeout:
        logger.error(f"Таймаут запроса для страницы {page}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON для страницы {page}: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка для страницы {page}: {e}")
        return None

def extract_vacancy_data(vacancies: List[Dict[str, Any]]) -> List[VacancyData]:
    structured_data = []

    for vacancy in vacancies:
        vacancy_data = VacancyData.api_response(vacancy)
        if vacancy_data:
            structured_data.append(vacancy_data)
    
    return structured_data



def filter_by_salary(vacancies: List[Dict[str, Any]], min_salary: int) -> List[Dict[str, Any]]:
    filtered = []
    for vacancy in vacancies:
        if (vacancy.salary_from and vacancy.salary_from >= min_salary) or \
           (vacancy.salary_to and vacancy.salary_to >= min_salary):
            filtered.append(vacancy)
    
    return filtered

def fetch_all(url: str, min_salary: int = 250000) -> List[VacancyData]:
    
    page = 0
    all_vacancies = []

    while True:
        vacancies = fetch_hh_vac(url, page)
        
        if not vacancies or 'items' not in vacancies:
            logger.warning(f"Не удалось получить данные для страницы {page}")
            break
            
        current_vacancies = vacancies.get('items', [])
        if not current_vacancies:
            logger.warning(f"Отсутствует ключ 'items' в ответе для страницы {page}")
            break

        # Структурируем данные 
        structured_vacancies = extract_vacancy_data(current_vacancies)
        
        # Фильтруем по зарплате
        filtered_vacancies = filter_by_salary(structured_vacancies, min_salary)
        all_vacancies.extend(filtered_vacancies)

        # Проверяем, есть ли следующая страница
        pages = vacancies.get('pages', 0)
        if page >= pages - 1 or page >= 19:  # HH API ограничивает 2000 вакансий (20 страниц)
            logger.info(f"Достигнут предел страниц ({pages})")
            break
            
        page += 1
        time.sleep(0.2) 

    return all_vacancies

def save_to_file(vacancies: List[Dict[str, Any]], filename: str = "./data/vacancies_data.json") -> None:
    try:
        # Создаём директорию, если она не существует
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        # Преобразуем датаклассы в словари 
        vacancies_dict = [asdict(vacancy) for vacancy in vacancies]

        with open(filename, "w", encoding="utf-8") as file:
            json.dump(vacancies_dict, file, ensure_ascii=False, indent=2)
        
        logging.info(f"Данные успешно сохранены в {filename}")
    
    except IOError as e:
        logging.info(f"Ошибка при сохранении файла: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при сохранении: {e}")


def main():
    logger.info("Начинаем сбор вакансий...")
    
    vacancies = fetch_all(url, min_salary=250000) # Запрос вакансий от 250 000 р.
    
    if vacancies:
        logger.info(f"Найдено {len(vacancies)} вакансий с зарплатой от 250000 руб.")

        # Пять вакансий выводим в качестве демонстрации
        for i, vacancy in enumerate(vacancies[:5]):
            logger.info(f"Пример {i+1}: {vacancy.title}: {vacancy.salary_from}-{vacancy.salary_to} {vacancy.salary_currency}")

        save_to_file(vacancies)
    else:
        logger.warning("Не удалось получить вакансии или подходящие вакансии не найдены.")


if __name__ == "__main__":
    main()