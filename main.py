from fastapi import FastAPI, HTTPException
import aiohttp 
import aiosqlite 
import asyncio
import textblob
from datetime import datetime
from threading import Thread 
from decouple import config
from fastapi.responses import RedirectResponse
import random

app = FastAPI()
COINS = ['bitcoin',"ethereum","dogecoin","shiba-inu"]
DB_FILE = "memecoins.db"
random.seed(42)


'''
function: Randomly generate sentiment score for each coin by picking 10 random posts
            from grok3_sim_posts.txt
input: file name
output: list of posts 
'''
def create_mock_posts(file_name):
    with open(file_name,'r') as file:
        sim_posts = file.readlines()
    file.close()
    return sim_posts

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS memecoins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coin_name TEXT,
                price_usd REAL,
                sentiment_score TEXT,
                sentiment_float REAL,
                timestamp TEXT)
        """)

async def get_sentiment(coin: str) -> float:
    random.shuffle(sim_posts)
    mock_posts = sim_posts[:10]
    post_sentiments = []
    for post in mock_posts:
        post_sentiments.append(textblob.TextBlob(post).sentiment.polarity)
    avg_sentiment = sum(post_sentiments) / len(post_sentiments) if post_sentiments else 0
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
            sentiment_fl = await get_sentiment(coin)
            if sentiment_fl > 0:
                sentiment = "Positive"
            elif sentiment_fl < 0:
                sentiment = "Negative"
            else:
                sentiment = "Neutral"
            sentiment = sentiment
            timestamp = datetime.now()
            readable_ts = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            await db.execute("""
            INSERT INTO memecoins (coin_name, price_usd, sentiment_score, sentiment_float, timestamp)
            VALUES (?, ?, ?, ?, ?)""",
            (coin, prices[coin], sentiment, sentiment_fl, readable_ts)
            )
            await db.commit()

async def run_update():
    while True:
        await update_db()
        await asyncio.sleep(3 * 60)

@app.on_event("startup")
async def startup_event():
    await init_db()
    await update_db()
    asyncio.create_task(run_update())

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
                f'''coin: {row[0]}, price: {row[1]}, sentiment_score: {row[2]}, last_updated: {row[3]}'''
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
    
@app.get("/")
async def redirect_to_memecoins():
    return RedirectResponse(url="/memecoins")

async def manual_update():
    await update_db()
    return {"message": "Data updated\n"}

if __name__ == "__main__":
    import uvicorn
    global sim_posts  # Declare sim_posts as global so we can access it inside main
    sim_posts = create_mock_posts("grok3_sim_posts.txt")  # Load the posts from the file
    uvicorn.run(app, host="0.0.0.0", port=8000)