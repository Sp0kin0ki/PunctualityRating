from fastapi import APIRouter, Depends, HTTPException, status, Header
from pydantic import BaseModel, Field, validator, constr, ConfigDict, field_validator
from datetime import datetime
from typing import List, Optional
import secrets
import os
from utils import get_db

router = APIRouter()

ADMIN_SECRET = os.getenv("ADMIN_SECRET", "default-admin-secret")

class FlightData(BaseModel):
    model_config = ConfigDict(extra='forbid')
    
    flight: str = Field(..., max_length=10, description="Номер рейса (макс. 10 символов)")
    departure_airport: str = Field(..., min_length=3, max_length=3, description="Код аэропорта вылета (3 символа)")
    arrival_airport: str = Field(..., min_length=3, max_length=3, description="Код аэропорта прибытия (3 символа)")
    plan_departure: datetime = Field(..., description="Плановое время вылета (ISO 8601)")
    plan_arrival: datetime = Field(..., description="Плановое время прибытия (ISO 8601)")
    fact_departure: Optional[datetime] = Field(None, description="Фактическое время вылета (ISO 8601)")
    fact_arrival: Optional[datetime] = Field(None, description="Фактическое время прибытия (ISO 8601)")

    @field_validator('plan_arrival', 'fact_arrival')
    def check_arrival_after_departure(cls, v, info):
        """Проверяет, что время прибытия позже времени вылета"""
        if v is None:
            return v
            
        dep_field_name = info.field_name.replace('arrival', 'departure')
        dep_value = info.data.get(dep_field_name)
        
        if dep_value and v < dep_value:
            raise ValueError(f"{info.field_name} должно быть после {dep_field_name}")
        return v

async def get_airline_from_token(
    authorization: Optional[str] = Header(None),
    conn = Depends(get_db)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid authorization header"
        )
    
    token = authorization.split(" ")[1]
    
    result = await conn.fetchrow(
        "SELECT airline_iata_code FROM tokens WHERE token = $1 AND is_active",
        token
    )
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or inactive token"
        )
    
    return result["airline_iata_code"]

async def verify_admin(
    x_admin_secret: str = Header(..., alias="X-Admin-Secret")
):
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid admin credentials"
        )
    return True

@router.post("/generate-token/{airline_code}")
async def generate_token(
    airline_code: str, 
    conn = Depends(get_db),
    is_admin: bool = Depends(verify_admin)
):
    airline = await conn.fetchrow(
        "SELECT 1 FROM airlines WHERE iata_code = $1", 
        airline_code
    )
    if not airline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Airline not found"
        )
    
    token = secrets.token_urlsafe(48)
    
    await conn.execute(
        "INSERT INTO tokens (token, airline_iata_code) VALUES ($1, $2)",
        token, airline_code
    )
    
    return {"token": token, "airline": airline_code}

@router.post("/deactivate-token/{token}")
async def deactivate_token(
    token: str,
    conn = Depends(get_db),
    is_admin: bool = Depends(verify_admin)
):
    result = await conn.execute(
        "UPDATE tokens SET is_active = FALSE WHERE token = $1",
        token
    )
    
    if result == "UPDATE 0":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Token not found"
        )
    
    return {"status": "deactivated"}

@router.post("/upload")
async def upload_flights(
    flights_data: List[FlightData],
    airline_code: str = Depends(get_airline_from_token),
    conn = Depends(get_db)
):
    processed = 0
    errors = []
    
    for flight in flights_data:
        try:
            flight_dict = flight.model_dump()
            
            existing = await conn.fetchrow(
                """
                SELECT id FROM flights 
                WHERE iata_code = $1 
                AND flight = $2 
                AND plan_departure = $3
                """,
                airline_code, 
                flight.flight, 
                flight.plan_departure
            )
            
            if existing:
                await conn.execute(
                    """
                    UPDATE flights 
                    SET fact_departure = $1,
                        fact_arrival = $2
                    WHERE id = $3
                    """,
                    flight.fact_departure,
                    flight.fact_arrival,
                    existing["id"]
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO flights (
                        iata_code, flight, 
                        departure_airport, arrival_airport,
                        plan_departure, plan_arrival,
                        fact_departure, fact_arrival
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8
                    )
                    """,
                    airline_code,
                    flight.flight,
                    flight.departure_airport,
                    flight.arrival_airport,
                    flight.plan_departure,
                    flight.plan_arrival,
                    flight.fact_departure,
                    flight.fact_arrival
                )
            
            processed += 1
            
        except Exception as e:
            errors.append({
                "flight": flight.flight,
                "error": f"Ошибка: {str(e)}"
            })
    
    return {
        "status": "success" if not errors else "partial",
        "processed": processed,
        "errors": errors,
        "message": f"Обработано рейсов: {processed}, ошибок: {len(errors)}"
    }
