# /workspace/api/main.py — FastAPI app для TWA
"""Точка входа TWA API: uvicorn api.main:app"""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from api.payments import router as payments_router
from api.showcase import router as showcase_router
from api.twa import router as twa_router
from core.logging import setup_logging

@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    setup_logging(service_name="twa-api")
    logger.info("TWA API запущен")
    yield


app = FastAPI(title="GiftTrack TWA API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # TWA открывается внутри Telegram, CORS не критичен
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(twa_router)
app.include_router(showcase_router)
app.include_router(payments_router)



@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
