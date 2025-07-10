from DB.Database import db
from fastapi import Depends

async def get_db():
    async with db.connection() as conn:
        yield conn