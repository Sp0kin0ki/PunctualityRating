from contextlib import asynccontextmanager
from fastapi import FastAPI
from DB.Database import db
from dotenv import load_dotenv
import os
import uvicorn
from app.API_internal import endpoints
from utils import calculate_flight_direction, close_db_pool, get_db_pool

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""

    dsn = os.getenv('DB_DSN')
    await db.connect(dsn)
    pool = await get_db_pool()
    await calculate_flight_direction(pool)
    yield

    await db.disconnect()
    await close_db_pool(pool)

app = FastAPI(lifespan=lifespan)
app.include_router(endpoints.router)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)