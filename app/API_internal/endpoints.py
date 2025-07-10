from fastapi import Depends, APIRouter
from utils import get_db

router = APIRouter()

@router.get("/get_flights")
async def get_flights(conn = Depends(get_db)):
    return await conn.fetch("SELECT * FROM flights LIMIT 5")