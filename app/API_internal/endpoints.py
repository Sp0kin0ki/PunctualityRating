from fastapi import Depends, APIRouter
from utils import get_db, format_datetime

router = APIRouter()

@router.get("/get_top3")
async def get_top_three(conn = Depends(get_db)):
    results = await conn.fetch("""
        WITH latest_ratings AS (
            SELECT DISTINCT ON (airline_iata_code) 
                airline_iata_code, rating, created_at
            FROM airline_ratings
            ORDER BY airline_iata_code, created_at DESC
        )
        SELECT 
            lr.airline_iata_code,
            a.name AS airline_name,
            lr.rating,
            lr.created_at
        FROM latest_ratings lr
        JOIN airlines a ON lr.airline_iata_code = a.iata_code
        ORDER BY lr.rating DESC, lr.created_at DESC
        LIMIT 3;
                            """)
    return [
        {
            **dict(row),
            "created_at": format_datetime(row["created_at"])
        }
        for row in results
    ]