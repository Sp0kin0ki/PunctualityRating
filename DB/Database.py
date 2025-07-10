import asyncpg
from contextlib import asynccontextmanager

class Database:
    def __init__(self):
        self.pool = None
    
    async def connect(self, dsn: str):
        self.pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=5,
            max_size=15,
            command_timeout=30
        )
    
    async def disconnect(self):
        if self.pool:
            await self.pool.close()
    
    @asynccontextmanager
    async def connection(self):
        """Контекстный менеджер для соединений"""
        if not self.pool:
            raise RuntimeError("Database not connected")
        
        async with self.pool.acquire() as conn:
            try:
                yield conn
            finally:
                pass
    
    async def execute(self, query: str, *args):
        async with self.connection() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args):
        async with self.connection() as conn:
            return await conn.fetch(query, *args)

db = Database()