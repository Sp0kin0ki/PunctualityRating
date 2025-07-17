from decimal import Decimal
import json
import os
import aiofiles
import asyncpg
from DB.Database import db
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

async def get_db():
    async with db.connection() as conn:
        yield conn
        
def format_datetime(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%d')

async def get_db_pool():
    return await asyncpg.create_pool(os.getenv('DB_DSN'))

async def close_db_pool(pool):
    await pool.close()

async def calculate_flight_direction(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        try:
            results = await conn.fetch("""
                WITH DirectionStats AS (
                SELECT 
                    LEAST(f.departure_airport, f.arrival_airport) AS airport1,
                    GREATEST(f.departure_airport, f.arrival_airport) AS airport2,
                    COUNT(*) AS total_flights,
                    SUM(
                        CASE 
                            WHEN f.fact_arrival IS NOT NULL 
                                AND ABS(EXTRACT(EPOCH FROM (f.fact_arrival - f.plan_arrival))) < 900 
                            THEN 1 
                            ELSE 0 
                        END
                    ) AS on_time_arrivals,
                    ROUND(AVG(
                        CASE 
                            WHEN f.fact_arrival IS NOT NULL 
                            THEN EXTRACT(EPOCH FROM (f.fact_arrival - f.plan_arrival))/60 
                            ELSE NULL
                        END
                    )::numeric, 1) AS avg_delay_minutes,
                    SUM(
                        CASE 
                            WHEN f.fact_departure IS NULL 
                            THEN 1 
                            ELSE 0 
                        END
                    ) AS missing_departure_count
                FROM flights f
                GROUP BY 
                    LEAST(f.departure_airport, f.arrival_airport), 
                    GREATEST(f.departure_airport, f.arrival_airport)
            )
            SELECT 
                airport1,
                airport2,
                total_flights,
                on_time_arrivals,
                ROUND(
                    (on_time_arrivals * 100.0 / NULLIF(total_flights, 0))::numeric, 
                    1
                ) AS on_time_percentage,
                COALESCE(avg_delay_minutes, 0) AS avg_delay_minutes,
                missing_departure_count
            FROM DirectionStats
            ORDER BY airport1, airport2;
                                    """)
            
            def convert_value(value):
                    if isinstance(value, datetime):
                        return value.isoformat()
                    if isinstance(value, Decimal):
                        return float(value)
                    return value
                
            data = [
                {k: convert_value(v) for k, v in record.items()}
                for record in results
            ]
            
            async with aiofiles.open('data//flight_direction_stats.json', 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            return True
        except Exception as e:
            print(f"Error in calculate_flight_direction: {e}")
            return False

async def calculate_airline_punctuality(pool: asyncpg.pool.Pool):
    async with pool.acquire() as conn:
        try:
            os.makedirs('data', exist_ok=True)
            
            results = await conn.fetch("""
                WITH FlightStats AS (
                    SELECT
                        f.iata_code AS code,
                        a.name AS airline,
                        COUNT(*) AS total_flights,
                        COUNT(CASE 
                                WHEN f.fact_departure IS NOT NULL 
                                AND EXTRACT(EPOCH FROM (f.fact_departure - f.plan_departure)) < 900 
                                THEN 1 
                            END) AS on_time_departures,
                        COUNT(CASE 
                                WHEN f.fact_arrival IS NOT NULL 
                                AND EXTRACT(EPOCH FROM (f.fact_arrival - f.plan_arrival)) < 900 
                                THEN 1 
                            END) AS on_time_arrivals,
                        COUNT(CASE 
                                WHEN f.fact_departure IS NULL 
                                THEN 1 
                            END) AS cancellations
                    FROM flights f
                    LEFT JOIN airlines a ON f.iata_code = a.iata_code
                    GROUP BY f.iata_code, a.name
                )
                SELECT
                    code,
                    airline,
                    total_flights,
                    ROUND(
                        (on_time_departures * 100.0 / NULLIF(total_flights, 0))::numeric, 
                        1
                    )::FLOAT AS departure_percentage,
                    ROUND(
                        (on_time_arrivals * 100.0 / NULLIF(total_flights, 0))::numeric, 
                        1
                    )::FLOAT AS arrival_percentage,
                    ROUND(
                        (cancellations * 100.0 / NULLIF(total_flights, 0))::numeric, 
                        1
                    )::FLOAT AS cancellation_percentage
                FROM FlightStats
                ORDER BY (on_time_departures * 100.0 / NULLIF(total_flights, 0)) DESC, 
                         (on_time_arrivals * 100.0 / NULLIF(total_flights, 0)) DESC;
            """)
            
            data = [
                {
                    "Код": record["code"],
                    "Авиакомпания": record["airline"],
                    "Отправление": record["departure_percentage"],
                    "Прибытие": record["arrival_percentage"],
                    "Отмены": record["cancellation_percentage"],
                    "Количество рейсов": record["total_flights"]
                }
                for record in results
            ]
            
            async with aiofiles.open('data/airline_punctuality.json', 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))
            return True
        except Exception as e:
            print(f"Error in calculate_airline_punctuality: {e}")
            return False
