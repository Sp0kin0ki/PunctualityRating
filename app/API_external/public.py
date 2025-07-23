from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import datetime, date
from typing import List, Optional
from utils import get_db

router = APIRouter()

@router.get("/airlines/top")
async def get_top_airlines(
    limit: int = 3, 
    conn = Depends(get_db)
):
    query = """
        SELECT 
            ar.airline_iata_code AS iata_code,
            al.name AS airline_name,
            ar.rating_departure,
            ar.rating_arrival,
            ar.created_at
        FROM (
            SELECT DISTINCT ON (airline_iata_code) *
            FROM airline_ratings
            ORDER BY airline_iata_code, created_at DESC
        ) ar
        JOIN airlines al ON ar.airline_iata_code = al.iata_code
        ORDER BY ar.rating_departure DESC
        LIMIT $1
    """
    results = await conn.fetch(query, limit)
    return [dict(row) for row in results]

@router.get("/airports/{iata_code}/stats")
async def airport_stats(
    iata_code: str,
    conn = Depends(get_db)
):
    airport = await conn.fetchrow(
        "SELECT 1 FROM airports WHERE iata_code = $1",
        iata_code
    )
    if not airport:
        raise HTTPException(status_code=404, detail="Airport not found")
    
    query = """
        SELECT 
            (SELECT COUNT(*) FROM flights 
             WHERE departure_airport = $1) AS departures,
            
            (SELECT COUNT(*) FROM flights 
             WHERE arrival_airport = $1) AS arrivals,
            
            (SELECT COUNT(*) FROM flights 
             WHERE departure_airport = $1 
             AND fact_departure IS NULL) AS missing_departures,
            
            (SELECT COUNT(*) FROM flights 
             WHERE arrival_airport = $1 
             AND fact_arrival IS NULL) AS missing_arrivals,
            
            (SELECT COUNT(*) FROM flight_features 
             WHERE departure_airport = $1 
             OR arrival_airport = $1) AS features_recorded
    """
    stats = await conn.fetchrow(query, iata_code)
    return dict(stats)

@router.get("/flights")
async def search_flights(
    airline: Optional[str] = None,
    departure_airport: Optional[str] = None,
    arrival_airport: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    min_delay: Optional[int] = None,
    max_delay: Optional[int] = None,
    limit: int = 100,
    conn = Depends(get_db)
):
    base_query = """
        SELECT 
            f.id, f.iata_code, f.flight,
            f.departure_airport, f.arrival_airport,
            f.plan_departure, f.plan_arrival,
            f.fact_departure, f.fact_arrival,
            ff.day_of_week, ff.time_of_day, 
            ff.season, ff.delay_category
        FROM flights f
        LEFT JOIN flight_features ff ON f.id = ff.flight_id
        WHERE 1=1
    """
    params = []
    count = 1
    
    conditions = []
    
    if airline:
        conditions.append(f"f.iata_code = ${count}")
        params.append(airline)
        count += 1
        
    if departure_airport:
        conditions.append(f"f.departure_airport = ${count}")
        params.append(departure_airport)
        count += 1
        
    if arrival_airport:
        conditions.append(f"f.arrival_airport = ${count}")
        params.append(arrival_airport)
        count += 1
        
    if date_from:
        conditions.append(f"DATE(f.plan_departure) >= ${count}")
        params.append(date_from)
        count += 1
        
    if date_to:
        conditions.append(f"DATE(f.plan_departure) <= ${count}")
        params.append(date_to)
        count += 1
        
    if min_delay is not None or max_delay is not None:
        delay_condition = """
            AND EXTRACT(EPOCH FROM (f.fact_arrival - f.plan_arrival)) 
            BETWEEN COALESCE(${min_delay}, -100000) 
            AND COALESCE(${max_delay}, 100000)
        """
        delay_condition = f"""
            AND EXTRACT(EPOCH FROM (f.fact_arrival - f.plan_arrival)) 
            BETWEEN COALESCE(${count}, -100000) 
            AND COALESCE(${count+1}, 100000)
        """
        conditions.append(delay_condition)
        params.append(min_delay)
        params.append(max_delay)
        count += 2
    
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    
    base_query += f" ORDER BY f.plan_departure DESC LIMIT ${count}"
    params.append(limit)
    
    results = await conn.fetch(base_query, *params)
    return [dict(row) for row in results]

@router.get("/flights/{flight_id}")
async def flight_details(
    flight_id: int,
    conn = Depends(get_db)
):
    query = """
        SELECT 
            f.*, 
            dep.airport_name AS departure_airport_name,
            dep.city AS departure_city,
            arr.airport_name AS arrival_airport_name,
            arr.city AS arrival_city,
            ff.day_of_week, ff.time_of_day, 
            ff.season, ff.delay_category
        FROM flights f
        JOIN airports dep ON f.departure_airport = dep.iata_code
        JOIN airports arr ON f.arrival_airport = arr.iata_code
        LEFT JOIN flight_features ff ON f.id = ff.flight_id
        WHERE f.id = $1
    """
    result = await conn.fetchrow(query, flight_id)
    
    if not result:
        raise HTTPException(status_code=404, detail="Flight not found")
    
    return dict(result)

@router.get("/airlines/{iata_code}/delay-stats")
async def airline_delay_stats(
    iata_code: str,
    conn = Depends(get_db)
):
    airline = await conn.fetchrow(
        "SELECT 1 FROM airlines WHERE iata_code = $1",
        iata_code
    )
    if not airline:
        raise HTTPException(status_code=404, detail="Airline not found")
    
    query = """
        SELECT 
            ff.delay_category,
            COUNT(*) AS count,
            ROUND(AVG(EXTRACT(EPOCH FROM (f.fact_arrival - f.plan_arrival)))) AS avg_delay_seconds
        FROM flights f
        JOIN flight_features ff ON f.id = ff.flight_id
        WHERE f.iata_code = $1
        GROUP BY ff.delay_category
    """
    results = await conn.fetch(query, iata_code)
    return [dict(row) for row in results]

@router.get("/airports")
async def search_airports(
    city: Optional[str] = None,
    country: Optional[str] = None,
    conn = Depends(get_db)
):
    base_query = """
        SELECT 
            iata_code, airport_name, city, timezone,
            longitude, latitude
        FROM airports
        WHERE 1=1
    """
    params = []
    conditions = []
    count = 1
    
    if city:
        conditions.append(f"LOWER(city) LIKE LOWER(${count})")
        params.append(f"%{city}%")
        count += 1
        
    if country:
        conditions.append(f"LOWER(country) LIKE LOWER(${count})")
        params.append(f"%{country}%")
        count += 1
    
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    
    results = await conn.fetch(base_query, *params)
    return [dict(row) for row in results]
