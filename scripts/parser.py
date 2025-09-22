# Diplom/scripts/parser_full.py

import requests
import json
import time
import pandas as pd
from pathlib import Path

# === Пути ===
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_PATH = DATA_DIR / "raw"
CLEAN_PATH = DATA_DIR / "clean"

RAW_PATH.mkdir(parents=True, exist_ok=True)
CLEAN_PATH.mkdir(parents=True, exist_ok=True)

# === API HH ===
BASE_URL = "https://api.hh.ru/vacancies"

def fetch_vacancies(query="Аналитик данных", pages=5, per_page=50, area=1):
    """Скачиваем вакансии с HH API"""
    all_vacancies = []

    for page in range(pages):
        params = {
            "text": query,
            "area": area,
            "per_page": per_page,
            "page": page
        }
        r = requests.get(BASE_URL, params=params)
        r.raise_for_status()
        data = r.json()

        # сохраняем сырые json постранично
        with open(RAW_PATH / f"hh_page_{page}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        all_vacancies.extend(data.get("items", []))
        print(f"Страница {page+1} скачана, вакансий: {len(data.get('items', []))}")
        time.sleep(0.3)

    return all_vacancies

def fetch_vacancy_details(vacancy_id):
    """Скачиваем детали вакансии (skills, description, specializations)"""
    url = f"{BASE_URL}/{vacancy_id}"
    r = requests.get(url)
    if r.status_code != 200:
        return {}
    v = r.json()
    return {
        "key_skills": [s["name"] for s in v.get("key_skills", [])],
        "description": v.get("description"),
        "specializations": [s["name"] for s in v.get("specializations", [])]
    }

def transform_to_table(vacancies):
    """Превращаем json вакансий в таблицу"""
    rows = []
    for v in vacancies:
        salary = v.get("salary")
        details = fetch_vacancy_details(v.get("id"))
        row = {
            "id": v.get("id"),
            "employer_id": v.get("employer", {}).get("id"),
            "name": v.get("name"),
            "employer": v.get("employer", {}).get("name"),
            "area": v.get("area", {}).get("name"),
            "published_at": v.get("published_at"),
            "salary_from": salary.get("from") if salary else None,
            "salary_to": salary.get("to") if salary else None,
            "salary_currency": salary.get("currency") if salary else None,
            "experience": v.get("experience", {}).get("name"),
            "employment": v.get("employment", {}).get("name"),
            "schedule": v.get("schedule", {}).get("name"),
            "key_skills": details.get("key_skills"),
            "description": details.get("description"),
            "specializations": details.get("specializations"),
            "url": v.get("alternate_url"),
            "fetched_at": pd.Timestamp.now()
        }
        rows.append(row)
        time.sleep(0.1)  # маленькая пауза, чтобы API не заблокировал
    return pd.DataFrame(rows)

def main():
    vacancies = fetch_vacancies(query="Аналитик данных", pages=5, per_page=50, area=1)
    df = transform_to_table(vacancies)

    out_file = CLEAN_PATH / "vacancies_full.csv"
    df.to_csv(out_file, index=False, encoding="utf-8-sig")
    print(f"Файл сохранен: {out_file}, строк: {len(df)}")

if __name__ == "__main__":
    main()
