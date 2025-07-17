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
