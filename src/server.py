from typing import List
from fastapi import FastAPI, HTTPException, Query
from app.handler import AsyncTokenAnalyzer

app = FastAPI()
analyzer = AsyncTokenAnalyzer()

@app.on_event("startup")
async def startup_event():
    await analyzer.initialize()

@app.get("/get_balance")
async def get_balance(address: str):
    try:
        balance = await analyzer.get_balance(address)
        return {"balance": balance, "symbol": analyzer.symbol}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_balance_batch")
async def get_balance_batch(
    addresses: List[str] = Query(..., alias="addresses") 
):
    try:
        balances = await analyzer.get_balance_batch(addresses)
        return {
            "addresses": addresses,
            "balances": balances,
            "symbol": analyzer.symbol
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@app.get("/get_top_date")
async def get_balance_batch_with_date(n: int = 10):
    try:
        top = await analyzer.get_top_with_transactions(n)
        return {
            "top_holders": [
                {"address": addr, "balance": bal, "date": date}
                for addr, bal, date in top
            ],
            "symbol": analyzer.symbol
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_top")
async def get_top(n: int = 10):
    try:
        top = await analyzer.get_top_holders(n)
        return {
            "top_holders": [
                {"address": addr, "balance": bal}
                for addr, bal in top
            ],
            "symbol": analyzer.symbol
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/get_token_info")
async def get_token_info():
    try:
        return await analyzer.get_token_info()
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
