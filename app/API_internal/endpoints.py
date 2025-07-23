import json
import os
import aiofiles
import pandas as pd
import numpy as np
from fastapi import Depends, APIRouter, HTTPException, BackgroundTasks
from utils import get_db, format_datetime, get_db_connection
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
from scipy.sparse import csr_matrix
import asyncio
from concurrent.futures import ThreadPoolExecutor

router = APIRouter()
RULES_FILE = 'resources/flight_delay_rules.csv'

@router.get("/get_top3")
async def get_top_three(conn = Depends(get_db)):
    results = await conn.fetch("""
        WITH latest_ratings AS (
            SELECT DISTINCT ON (airline_iata_code) 
                airline_iata_code, rating_departure, rating_arrival, created_at
            FROM airline_ratings
            ORDER BY airline_iata_code, created_at DESC
        )
        SELECT 
            lr.airline_iata_code,
            a.name AS airline_name,
            lr.rating_departure,
            lr.rating_arrival,
            lr.created_at
        FROM latest_ratings lr
        JOIN airlines a ON lr.airline_iata_code = a.iata_code
        ORDER BY lr.rating_departure DESC, lr.rating_arrival DESC, lr.created_at DESC
        LIMIT 3;
                            """)
    return [
        {
            **dict(row),
            "created_at": format_datetime(row["created_at"])
        }
        for row in results
    ]
    
@router.get("/get_all_direction")
async def get_all_flight_direction():
    file_path = "data//flight_direction_stats.json"
    if not os.path.exists(file_path):
        return {"error": "File not found"}

    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    return data

@router.get("/get_airline_punctuality")
async def get_airline_punctuality():
    file_path = "data/airline_punctuality.json"
    if not os.path.exists(file_path):
        return {"error": "File not found"}

    async with aiofiles.open(file_path, 'r', encoding='utf-8') as file:
        data = await file.read()
        return json.loads(data)
    
@router.get("/get_airports")
async def get_airports(conn = Depends(get_db)):
    results = await conn.fetch("""
        SELECT 
            a.iata_code AS "IATA код",
            a.airport_name AS "Название аэропорта",
            a.longitude AS "Долгота",
            a.latitude AS "Широта",
            COALESCE(dep.departure_count, 0) AS "Кол-во вылетов",
            COALESCE(arr.arrival_count, 0) AS "Кол-во прилетов"
        FROM airports a
        LEFT JOIN (
            SELECT 
                departure_airport AS iata_code,
                COUNT(*) AS departure_count
            FROM flights
            GROUP BY departure_airport
        ) dep ON a.iata_code = dep.iata_code
        LEFT JOIN (
            SELECT 
                arrival_airport AS iata_code,
                COUNT(*) AS arrival_count
            FROM flights
            GROUP BY arrival_airport
        ) arr ON a.iata_code = arr.iata_code;
                               """)
    
    return [
        {
            **dict(row)
        }
        for row in results
    ]
    
    
@router.get("/delay_histogram")
async def delay_histogram(conn = Depends(get_db)):
    results = await conn.fetch("""
        SELECT
            COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (fact_departure - plan_departure)) <= 600) AS "0-10 минут",
            COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (fact_departure - plan_departure)) > 600 AND EXTRACT(EPOCH FROM ((fact_departure - plan_departure))) <= 1200) AS "11-20 минут",
            COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (fact_departure - plan_departure)) > 1200 AND EXTRACT(EPOCH FROM (fact_departure - plan_departure)) <= 1800) AS "21-30 минут",
            COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (fact_departure - plan_departure)) > 1800 AND EXTRACT(EPOCH FROM (fact_departure - plan_departure)) <= 7200) AS "31-120 минут",
            COUNT(*) FILTER (WHERE EXTRACT(EPOCH FROM (fact_departure - plan_departure)) > 7200) AS ">120 минут"
        FROM flights;
                        """)
    
    return [
        {
            **dict(row)
        }
        for row in results
    ]
    
    
@router.get("/cancellations_distribution")
async def get_cancellations_distribution(conn = Depends(get_db)):
    results = await conn.fetch("""
        SELECT 
            a.name AS airlines,
            COUNT(*) FILTER (WHERE f.fact_departure IS NULL) AS cancellations
        FROM flights f
        JOIN airlines a ON f.iata_code = a.iata_code
        GROUP BY a.name
        ORDER BY cancellations DESC;
                               """)
    
    return [
        {
            **dict(row)
        }
        for row in results
    ]

@router.get("/delay-rules/top")
async def get_top_delay_rules(top_n: int = 5):
    """
    Возвращает топ-N сложных правил о задержках рейсов
    """
    if not os.path.exists(RULES_FILE):
        raise HTTPException(
            status_code=404,
            detail="Файл с правилами не найден. Запустите анализ сначала."
        )
    
    try:
        rules_df = pd.read_csv(RULES_FILE)
        
        sorted_rules = rules_df.sort_values(
            by=['lift', 'confidence'], 
            ascending=False
        )
        
        top_rules = sorted_rules.head(top_n)
        
        results = []
        for _, row in top_rules.iterrows():
            results.append({
                "rule": row['formatted_rule'],
                "support": float(row['support']),
                "confidence": float(row['confidence']),
                "lift": float(row['lift'])
            })
        
        return results
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при обработке файла правил: {str(e)}"
        )

@router.post("/delay-rules/refresh")
async def refresh_delay_rules(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_analysis_task)
    return {"status": "started", "message": "Анализ запущен в фоновом режиме"}

async def async_load_data_from_db():
    """Асинхронная загрузка данных из БД"""
    async with get_db_connection() as conn:
        rows = await conn.fetch("SELECT * FROM flight_features")
        return pd.DataFrame([dict(r) for r in rows])

def prepare_transactions(df):
    transactions = []
    for _, row in df.iterrows():
        transaction = []
        for col in df.columns:
            transaction.append(f"{col}={row[col]}")
        transactions.append(transaction)
    return transactions

def format_rule(antecedents, consequents):
    condition_map = {
        'day_of_week': {
            'Понедельник': 'понедельник',
            'Вторник': 'вторник',
            'Среда': 'среда',
            'Четверг': 'четверг',
            'Пятница': 'пятница',
            'Суббота': 'суббота',
            'Воскресенье': 'воскресенье'
        },
        'time_of_day': {
            'Утро': 'утро',
            'День': 'день',
            'Вечер': 'вечер',
            'Ночь': 'ночь'
        },
        'season': {
            'Зима': 'зима',
            'Весна': 'весна',
            'Лето': 'лето',
            'Осень': 'осень'
        }
    }

    conditions = []
    delay = None

    for item in antecedents:
        col, val = item.split('=')
        if col in condition_map:
            conditions.append(condition_map[col].get(val, val))
        elif col == 'departure_airport':
            conditions.append(f'аэропорт вылета {val}')
        elif col == 'arrival_airport':
            conditions.append(f'аэропорт прилета {val}')
        elif col == 'airline_iata_code':
            conditions.append(f'авиакомпания {val}')

    for item in consequents:
        col, val = item.split('=')
        if col == 'delay_category':
            if val == 'Нет_задержки':
                delay = 'нет задержки'
            elif val == 'Короткая':
                delay = 'короткая задержка'
            elif val == 'Средняя':
                delay = 'средняя задержка'
            elif val == 'Длинная':
                delay = 'длинная задержка'
            elif val == 'Очень_длинная':
                delay = 'очень длинная задержка'

    return f"если {', '.join(conditions)}, то {delay}"

def find_delay_rules(transactions, min_support=0.05):
    te = TransactionEncoder()
    
    te_ary = te.fit(transactions).transform(transactions, sparse=True)
    
    sparse_matrix = csr_matrix(te_ary, dtype=bool)
    
    item_support = np.array(sparse_matrix.mean(axis=0)).flatten()
    
    mask = item_support >= min_support
    selected_columns = [te.columns_[i] for i in np.where(mask)[0]]
    
    filtered_matrix = sparse_matrix[:, mask]
    df_encoded = pd.DataFrame.sparse.from_spmatrix(
        filtered_matrix,
        columns=selected_columns
    )

    delay_columns = [col for col in df_encoded.columns if 'delay_category=' in col]
    other_columns = [col for col in df_encoded.columns if 'delay_category=' not in col]
    
    if len(other_columns) > 1000:
        other_columns = other_columns[:1000]
    
    df_encoded = df_encoded[delay_columns + other_columns]

    print(f"Используется {len(df_encoded.columns)} колонок после фильтрации")

    frequent_itemsets = apriori(
        df_encoded,
        min_support=min_support,
        use_colnames=True,
        low_memory=True,
        max_len=4
    )

    if not frequent_itemsets.empty:
        rules = association_rules(
            frequent_itemsets,
            metric="lift",
            min_threshold=1.5,
            support_only=False
        )

        delay_rules = rules[
            rules['consequents'].apply(
                lambda x: any('delay_category=' in item for item in x) and 
                not any('delay_category=Нет_задержки' in item for item in x)
            )
        ]

        delay_rules['formatted_rule'] = delay_rules.apply(
            lambda x: format_rule(x['antecedents'], x['consequents']),
            axis=1
        )

        return delay_rules.sort_values(by=['lift', 'confidence'], ascending=False)
    
    print("Не найдено частых наборов с заданным min_support.")
    return pd.DataFrame()

def run_analysis_task():
    """Синхронная обертка для асинхронной загрузки"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        df = loop.run_until_complete(async_load_data_from_db())
        print(f"Загружено {len(df)} строк")
        
        transactions = prepare_transactions(df)
        rules = find_delay_rules(transactions, min_support=0.001)
        
        if not rules.empty:
            rules.to_csv(RULES_FILE, index=False)
            print("Результаты сохранены в flight_delay_rules.csv")
        else:
            print("Не удалось найти правила")
    except Exception as e:
        print(f"Ошибка при выполнении анализа: {str(e)}")
    finally:
        loop.close()
