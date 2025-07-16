from DB.Database import db
from fastapi import Depends
from datetime import datetime

async def get_db():
    async with db.connection() as conn:
        yield conn
        
def format_datetime(dt: datetime) -> str:
    return dt.strftime('%Y-%m-%d')