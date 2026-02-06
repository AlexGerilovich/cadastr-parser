import requests
import pandas as pd
from urllib.parse import quote
import warnings
from tqdm import tqdm
import time
import os
import signal
import sys
import json
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('land_parcel_scan.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class LandParcelScanner:
    def __init__(self, kvartal_number, start=1, end=9999):
        self.kvartal_number = kvartal_number
        self.start = start
        self.end = end
        self.land_parcels = []
        self.current_num = start
        self.found_count = 0
        self.not_found_streak = 0
        self.max_not_found = 100  # Лимит пустых ответов для остановки
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json, text/plain, */*"
        })
        
        # Создаем папку с датой для сохранения результатов
        self.folder_name = f"results_{datetime.now().strftime('%Y-%m-%d_%H-%M')}"
        os.makedirs(self.folder_name, exist_ok=True)
        
        self.temp_file = f"{self.folder_name}/temp_{kvartal_number.replace(':', '_')}.json"
        self.result_file = f"{self.folder_name}/parcels_{kvartal_number.replace(':', '_')}.xlsx"
        
        signal.signal(signal.SIGINT, self.handle_interrupt)
    
    def handle_interrupt(self, signum, frame):
        logger.info("\nПрерывание операции. Сохраняю данные...")
        self.save_progress()
        logger.info(f"Данные сохранены. Последний проверенный номер: {self.current_num}")
        sys.exit(0)
    
    def save_progress(self):
        """Сохраняет текущий прогресс"""
        try:
            progress_data = {
                'metadata': {
                    'kvartal': self.kvartal_number,
                    'start_num': self.start,
                    'end_num': self.end,
                    'last_checked': self.current_num,
                    'timestamp': datetime.now().isoformat()
                },
                'stats': {
                    'total_checked': self.current_num - self.start,
                    'found_count': self.found_count,
                    'completion': f"{((self.current_num - self.start) / (self.end - self.start + 1)) * 100:.2f}%"
                },
                'land_parcels': self.land_parcels
            }
            
            with open(self.temp_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
            
            if self.land_parcels:
                df = pd.DataFrame(self.land_parcels)
                df.to_excel(self.result_file, index=False, engine='openpyxl')
                logger.info(f"Сохранено {len(self.land_parcels)} участков в {self.result_file}")
            
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения: {str(e)}")
            return False
    
    def load_progress(self):
        """Загружает предыдущий прогресс"""
        if os.path.exists(self.temp_file):
            try:
                with open(self.temp_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                self.land_parcels = data.get('land_parcels', [])
                self.found_count = len(self.land_parcels)
                self.current_num = data['metadata']['last_checked'] + 1
                
                logger.info(f"Загружен прогресс: найдено {self.found_count} участков")
                logger.info(f"Продолжаем с номера: {self.current_num}")
                
                if self.land_parcels:
                    pd.DataFrame(self.land_parcels).to_excel(
                        self.result_file, index=False, engine='openpyxl'
                    )
                
                return True
            except Exception as e:
                logger.error(f"Ошибка загрузки: {str(e)}")
                return False
        return False
    
    def fetch_parcel_data(self, parcel_num):
        """Запрашивает данные участка"""
        try:
            url = "https://nspd.gov.ru/api/geoportal/v2/search/geoportal"
            params = {
                "query": parcel_num,
                "thematicSearchId": 1,
                "_": int(time.time() * 1000)
            }
            
            response = self.session.get(
                url,
                params=params,
                headers={"Referer": f"https://nspd.gov.ru/map?kadastr={quote(self.kvartal_number)}"},
                verify=False,
                timeout=15
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                logger.warning(f"Неожиданный статус {response.status_code} для {parcel_num}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка запроса {parcel_num}: {str(e)}")
            return None
    
    def parse_parcel_data(self, data, parcel_num):
        """Извлекает данные из ответа API"""
        try:
            features = data.get('data', {}).get('features', [])
            for feature in features:
                props = feature.get('properties', {})
                if props.get('categoryName') == 'Земельные участки ЕГРН':
                    options = props.get('options', {})
                    
                    return {
                        'Кадастровый номер': options.get('cad_num', parcel_num),
                        'Адрес': options.get('readable_address', 'Не указан'),
                        'Площадь (кв.м)': options.get('area') or options.get('specified_area', ''),
                        'Категория земли': options.get('land_record_category_type', ''),
                        'Вид использования': options.get('permitted_use_established_by_document', ''),
                        'Статус': options.get('status', ''),
                        'Кадастровая стоимость': options.get('cost_value', ''),
                        'Дата оценки': options.get('cost_determination_date', ''),
                        'Дата регистрации': options.get('land_record_reg_date', ''),
                        'Тип собственности': options.get('ownership_type', ''),
                        'Тип права': options.get('right_type', ''),
                        'Координаты': str(feature.get('geometry', {}).get('coordinates', '')),
                        'Источник данных': 'НСПД РФ',
                        'Дата проверки': datetime.now().strftime('%Y-%m-%d %H:%M')
                    }
            return None
        except Exception as e:
            logger.error(f"Ошибка парсинга {parcel_num}: {str(e)}")
            return None
    
    def scan(self):
        """Основной метод сканирования"""
        logger.info(f"\nНачало сканирования квартала {self.kvartal_number}")
        logger.info(f"Диапазон номеров: {self.start}-{self.end}")
        logger.info(f"Результаты будут сохранены в: {os.path.abspath(self.folder_name)}")
        
        start_time = time.time()
        
        try:
            with tqdm(initial=self.current_num-1, total=self.end, desc="Поиск участков", unit="уч") as pbar:
                while self.current_num <= self.end:
                    parcel_num = f"{self.kvartal_number}:{self.current_num}"
                    
                    data = self.fetch_parcel_data(parcel_num)
                    
                    if data is None:
                        self.not_found_streak += 1
                    else:
                        parcel_info = self.parse_parcel_data(data, parcel_num)
                        if parcel_info:
                            self.land_parcels.append(parcel_info)
                            self.found_count += 1
                            self.not_found_streak = 0
                            pbar.set_postfix({'Найдено': self.found_count})
                    
                    # Сохранение прогресса
                    if self.found_count > 0 and (self.found_count % 10 == 0 or self.current_num % 50 == 0):
                        self.save_progress()
                    
                    # Проверка лимита пустых ответов
                    if self.not_found_streak >= self.max_not_found:
                        logger.info(f"\nАвтоостановка после {self.max_not_found} ненайденных участков подряд")
                        break
                    
                    time.sleep(0.5)  # Задержка между запросами
                    self.current_num += 1
                    pbar.update(1)
            
            # Финальное сохранение
            self.save_progress()
            
            # Формирование отчета
            elapsed_time = time.time() - start_time
            logger.info(f"\n{'='*40}")
            logger.info("Сканирование завершено")
            logger.info(f"Проверено участков: {self.current_num - self.start}")
            logger.info(f"Найдено участков: {self.found_count}")
            logger.info(f"Затраченное время: {elapsed_time:.2f} сек.")
            logger.info(f"Средняя скорость: {(self.current_num - self.start)/elapsed_time:.2f} участков/сек.")
            logger.info(f"Результаты сохранены в: {os.path.abspath(self.result_file)}")
            logger.info(f"{'='*40}")
            
            return pd.DataFrame(self.land_parcels) if self.land_parcels else None
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}")
            self.save_progress()
            return None

def validate_input(value, min_val, max_val):
    """Проверяет корректность ввода"""
    try:
        num = int(value)
        return min_val <= num <= max_val
    except ValueError:
        return False

def main():
    print(f"\n{'='*50}")
    print(" ПАРСЕР ЗЕМЕЛЬНЫХ УЧАСТКОВ КАДАСТРОВОГО КВАРТАЛА")
    print(f"{'='*50}\n")
    
    # Ввод кадастрового номера квартала
    while True:
        kvartal = input("Введите номер кадастрового квартала (XX:XX:XXXXXXX): ").strip()
        parts = kvartal.split(':')
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            break
        print("Ошибка! Формат должен быть XX:XX:XXXXXXX (только цифры)")
    
    # Инициализация сканера
    scanner = LandParcelScanner(kvartal)
    
    # Проверка существующего прогресса
    if os.path.exists(scanner.temp_file):
        print("\nОбнаружен предыдущий запуск для этого квартала.")
        choice = input("Продолжить сканирование (y) или начать заново (n)? [y/n]: ").lower().strip()
        if choice == 'y':
            if not scanner.load_progress():
                print("Не удалось загрузить прогресс. Начнем заново.")
        else:
            os.remove(scanner.temp_file)
            if os.path.exists(scanner.result_file):
                os.remove(scanner.result_file)
    
    # Настройка диапазона
    print("\nНастройка диапазона номеров участков:")
    while True:
        start = input(f"Начать с номера [по умолчанию {scanner.start}]: ").strip() or scanner.start
        if validate_input(start, 1, 99999):
            scanner.start = int(start)
            break
    
    while True:
        end = input(f"Закончить на номере [по умолчанию {scanner.end}]: ").strip() or scanner.end
        if validate_input(end, scanner.start, 99999):
            scanner.end = int(end)
            break
    
    scanner.current_num = scanner.start
    
    # Запуск сканирования
    print("\nЗапуск сканирования...")
    result = scanner.scan()
    
    if result is None:
        print("\nУчастки не найдены. Проверьте правильность кадастрового номера квартала.")
    else:
        print(f"\nНайдено {len(result)} участков. Результаты сохранены в:")
        print(f"- Excel файл: {os.path.abspath(scanner.result_file)}")
        print(f"- Лог файл: {os.path.abspath('land_parcel_scan.log')}")

if __name__ == "__main__":
    main()