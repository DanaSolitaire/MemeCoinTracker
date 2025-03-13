from fastapi import FastAPI, HTTPException
import aiohttp 
import aiosqlite 
import asyncio
import time
import schedule
import textblob
from datetime import datetime
from threading import Thread 
from decouple import config


app = FastAPI()
COINS = ['bitcoin']
DB_FILE = "memecoins.db"

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS memecoins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coin_name TEXT,
                price_usd REAL,
                sentiment_score REAL,
                timestamp TEXT)
        """)
        await db.close()
async def get_sentiment(coin: str) -> float:
    mock_posts = [
        f"I love {coin}, it’s going to the moon!",
        f"{coin} is crashing, sell now!",
        f"Just bought some {coin}, feeling good.",
        f"Can’t believe I missed the boat on {coin}.",
        f"Is {coin} the next big thing?",
        f"Lost so much money on {coin}, never again!"
    ]
    post_sentiments = []
    for post in mock_posts:
        post_sentiments.append(textblob.TextBlob(post).sentiment.polarity)
    avg_sentiment = sum(post_sentiments) / len(post_sentiments)
    return avg_sentiment

async def fetch_prices():
    async with aiohttp.ClientSession() as session:
        temp = ",".join(COINS)
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={temp}&vs_currencies=usd"
        async with session.get(url) as response:
            data = await response.json()
            return {coin: data[coin]['usd'] for coin in COINS}

async def update_db():
    prices = await fetch_prices()
    async with aiosqlite.connect(DB_FILE) as db:
        for coin in COINS:
            sentiment = await get_sentiment(coin)
            timestamp = datetime.now().isoformat()
            await db.execute("""
            INSERT INTO memecoins (coin_name, price_usd, sentiment_score, timestamp)
            VALUES (?, ?, ?, ?)""",
            (coin, prices[coin], sentiment, timestamp)
            )
            await db.commit()

def run_scheduler():
    schedule.every(1).minutes.do(lambda: app.state.loop.create_task(update_db()))
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
async def startup_event():
    await init_db()
    await update_db()
    app.state.loop = asyncio.get_event_loop()
    Thread(target=run_scheduler, daemon=True).start()

@app.get("/memecoins")
async def list_memecoins():
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute(
            """
            SELECT coin_name, price_usd, sentiment_score, timestamp
            FROM memecoins
            WHERE id IN (SELECT MAX(id)
            FROM memecoins GROUP BY coin_name)
            """
        )
        rows = await cursor.fetchall()
        return [
            {
                "coin": row[0],
                "price": row[1],
                "sentiment_score": row[2],
                "last_updated": row[3]
            }
            for row in rows
        ]
            
@app.get("/memecoins/{coin}")
async def get_memecoin(coin: str):
    if coin not in COINS:
        raise HTTPException (status_code=404, detail="Memecoin not tracked")
    async with aiosqlite.connect(DB_FILE) as db:
        cursor = await db.execute(
            """
            SELECT price_usd, sentiment_score, timestamp
            FROM memecoins
            WHERE coin_name = ?
            ORDER BY timestamp DESC
            LIMIT 10
            """,
            (coin,)
        )
        rows = await cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No data available")
        return {
            "coin": coin,
            "latest": {
                "price_usd": rows[0][0],
                "sentiment_score": rows[0][1],
                "timestamp": rows[0][2]
            },
            "history": [
                {
                    "price_usd": r[0],
                    "sentiment_score": r[1],
                    "timestamp": r[2]
                }
                for r in rows
            ]
        }

async def manual_update():
    await update_db()
    return {"message": "Data updated"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)