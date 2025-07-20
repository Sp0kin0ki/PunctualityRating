import json
import os
import aiofiles
from fastapi import Depends, APIRouter
from utils import get_db, format_datetime

router = APIRouter()

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