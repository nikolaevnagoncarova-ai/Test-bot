import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from sqlalchemy import (
    BigInteger, Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, select, func
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# ============================================================
# CONFIG
# ============================================================
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_IDS: list[int] = [
        int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()
    ]
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///gift_analytics.db")

    GETGEMS_API_KEY: str = os.getenv("GETGEMS_API_KEY", "")
    TONNEL_AUTH_DATA: str = os.getenv("TONNEL_AUTH_DATA", "")
    MRKT_AUTH_DATA: str = os.getenv("MRKT_AUTH_DATA", "")
    PORTALS_AUTH_DATA: str = os.getenv("PORTALS_AUTH_DATA", "")

    UPDATE_INTERVAL_GETGEMS: int = int(os.getenv("UPDATE_INTERVAL_GETGEMS", "1800"))
    UPDATE_INTERVAL_TONNEL: int = int(os.getenv("UPDATE_INTERVAL_TONNEL", "1800"))
    UPDATE_INTERVAL_MRKT: int = int(os.getenv("UPDATE_INTERVAL_MRKT", "1800"))
    UPDATE_INTERVAL_PORTALS: int = int(os.getenv("UPDATE_INTERVAL_PORTALS", "1800"))
    NOTIFICATION_CHECK_INTERVAL: int = int(os.getenv("NOTIFICATION_CHECK_INTERVAL", "300"))

    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "15"))
    REQUEST_RETRIES: int = int(os.getenv("REQUEST_RETRIES", "3"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def is_admin(cls, user_id: int) -> bool:
        return user_id in cls.ADMIN_IDS


logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ============================================================
# DATABASE SETUP & MODELS
# ============================================================
engine = create_async_engine(Config.DATABASE_URL, echo=False, pool_pre_ping=True)
async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    language: Mapped[str] = mapped_column(String(10), default="ru")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class Collection(Base):
    __tablename__ = "collections"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    canonical_slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"
    __table_args__ = (Index("ix_price_snapshots_lookup", "collection_id", "marketplace", "recorded_at"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collections.id"), nullable=False)
    marketplace: Mapped[str] = mapped_column(String(50), nullable=False)
    floor_price: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(10), default="TON")
    listings_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

class ActiveListing(Base):
    __tablename__ = "active_listings"
    __table_args__ = (UniqueConstraint("marketplace", "external_gift_id", name="uq_listing_external"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collections.id"), nullable=False)
    marketplace: Mapped[str] = mapped_column(String(50), nullable=False)
    external_gift_id: Mapped[str] = mapped_column(String(255), nullable=False)
    gift_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(10), default="TON")
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class Alert(Base):
    __tablename__ = "alerts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"))
    collection_id: Mapped[int] = mapped_column(ForeignKey("collections.id"))
    marketplace: Mapped[str] = mapped_column(String(50), default="all")
    target_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_triggered_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class SourceState(Base):
    __tablename__ = "source_states"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    marketplace: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    is_available: Mapped[bool] = mapped_column(Boolean, default=True)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    requires_auth: Mapped[bool] = mapped_column(Boolean, default=False)

class ErrorLog(Base):
    __tablename__ = "error_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized.")

async def close_db() -> None:
    await engine.dispose()

@asynccontextmanager
async def get_session() -> AsyncSession:
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ============================================================
# HTTP CLIENT & INTEGRATIONS
# ============================================================
class HttpClient:
    def __init__(self, headers: Optional[dict] = None):
        self._headers = headers or {}
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT),
                headers=self._headers,
            )
        return self._session

    async def post_json(self, url: str, json_body: dict, headers: Optional[dict] = None) -> Optional[Any]:
        session = await self._get_session()
        merged = {**self._headers, **(headers or {})}
        for attempt in range(Config.REQUEST_RETRIES):
            try:
                async with session.post(url, json=json_body, headers=merged) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)
            except Exception:
                pass
            await asyncio.sleep(1.5 * (attempt + 1))
        return None

    async def get_json(self, url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Optional[Any]:
        session = await self._get_session()
        merged = {**self._headers, **(headers or {})}
        for attempt in range(Config.REQUEST_RETRIES):
            try:
                async with session.get(url, params=params, headers=merged) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)
            except Exception:
                pass
            await asyncio.sleep(1.5 * (attempt + 1))
        return None

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

@dataclass
class AdapterResult:
    ok: bool
    data: list[dict] = field(default_factory=list)
    error: Optional[str] = None
    requires_auth: bool = False

class MarketplaceAdapter(ABC):
    name: str = "base"
    @abstractmethod
    async def get_collections(self) -> AdapterResult: ...
    @abstractmethod
    async def get_floor(self, collection_name: str) -> AdapterResult: ...
    @abstractmethod
    async def get_listings(self, collection_name: str, limit: int = 30) -> AdapterResult: ...

class GetGemsAdapter(MarketplaceAdapter):
    name = "getgems"
    def __init__(self, api_key: str = ""):
        headers = {"Accept": "application/json"}
        if api_key: headers["X-API-Key"] = api_key
        self._http = HttpClient(headers=headers)

    async def get_collections(self) -> AdapterResult:
        data = await self._http.get_json("https://api.getgems.io/public-api/collections", params={"limit": 100})
        if data is None: return AdapterResult(ok=False, error="GetGems unavailable")
        raw = data if isinstance(data, list) else data.get("collections", [])
        cols = [{"collection_name": i.get("name", ""), "floor_price": i.get("floorPrice", 0), "currency": "TON", "listings_count": i.get("listedCount"), "marketplace": self.name} for i in raw if i.get("name")]
        return AdapterResult(ok=True, data=cols)

    async def get_floor(self, collection_name: str) -> AdapterResult:
        data = await self._http.get_json(f"https://api.getgems.io/public-api/collections/{collection_name}/floor")
        if data is None: return AdapterResult(ok=False, error="GetGems floor failed")
        return AdapterResult(ok=True, data=[{"collection_name": collection_name, "floor_price": data.get("floorPrice", 0), "currency": "TON", "marketplace": self.name}])

    async def get_listings(self, collection_name: str, limit: int = 30) -> AdapterResult:
        return AdapterResult(ok=True, data=[])

    async def close(self): await self._http.close()

class TonnelAdapter(MarketplaceAdapter):
    name = "tonnel"
    def __init__(self, auth_data: str = ""):
        self._auth = auth_data
        self._http = HttpClient(headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"})
    async def get_collections(self) -> AdapterResult: return AdapterResult(ok=True, data=[])
    async def get_floor(self, name: str) -> AdapterResult: return AdapterResult(ok=False, error="Tonnel requires listings")
    async def get_listings(self, name: str, limit: int = 30) -> AdapterResult: return AdapterResult(ok=True, data=[])
    async def close(self): await self._http.close()

class MrktAdapter(MarketplaceAdapter):
    name = "mrkt"
    def __init__(self, auth_data: str = ""):
        self._auth = auth_data
        self._http = HttpClient(headers={"Accept": "application/json"})
    async def get_collections(self) -> AdapterResult:
        if not self._auth: return AdapterResult(ok=False, requires_auth=True)
        return AdapterResult(ok=True, data=[])
    async def get_floor(self, name: str) -> AdapterResult: return AdapterResult(ok=False)
    async def get_listings(self, name: str, limit: int = 20) -> AdapterResult: return AdapterResult(ok=True, data=[])
    async def close(self): await self._http.close()

class PortalsAdapter(MarketplaceAdapter):
    name = "portals"
    def __init__(self, auth_data: str = ""):
        self._auth = auth_data
        self._http = HttpClient(headers={"Accept": "application/json"})
    async def get_collections(self) -> AdapterResult:
        if not self._auth: return AdapterResult(ok=False, requires_auth=True)
        return AdapterResult(ok=True, data=[])
    async def get_floor(self, name: str) -> AdapterResult: return AdapterResult(ok=False)
    async def get_listings(self, name: str, limit: int = 20) -> AdapterResult: return AdapterResult(ok=True, data=[])
    async def close(self): await self._http.close()


# ============================================================
# SERVICES (AGGREGATOR, ANALYTICS, NOTIFICATIONS)
# ============================================================
class MarketplaceAggregator:
    def __init__(self):
        self.adapters = {
            "getgems": GetGemsAdapter(api_key=Config.GETGEMS_API_KEY),
            "tonnel": TonnelAdapter(auth_data=Config.TONNEL_AUTH_DATA),
            "mrkt": MrktAdapter(auth_data=Config.MRKT_AUTH_DATA),
            "portals": PortalsAdapter(auth_data=Config.PORTALS_AUTH_DATA),
        }

    async def close(self):
        for a in self.adapters.values(): await a.close()

    async def _get_or_create_collection(self, name: str) -> int:
        slug = name.lower().replace(" ", "-")
        async with get_session() as session:
            res = await session.execute(select(Collection).where(Collection.canonical_slug == slug))
            coll = res.scalar_one_or_none()
            if not coll:
                coll = Collection(name=name, canonical_slug=slug)
                session.add(coll)
                await session.flush()
            return coll.id

    async def refresh_marketplace(self, mp_name: str) -> None:
        adapter = self.adapters.get(mp_name)
        if not adapter: return
        try:
            res = await adapter.get_collections()
            if not res.ok: return
            for item in res.data:
                c_id = await self._get_or_create_collection(item["collection_name"])
                async with get_session() as session:
                    session.add(PriceSnapshot(
                        collection_id=c_id, marketplace=mp_name,
                        floor_price=item.get("floor_price", 0), currency="TON",
                        listings_count=item.get("listings_count")
                    ))
        except Exception as e:
            logger.exception("Refresh error")

    async def refresh_all(self) -> None:
        for mp in self.adapters:
            await self.refresh_marketplace(mp)

    async def get_floor_comparison(self, collection_name: str) -> dict[str, Optional[float]]:
        comp = {}
        for mp, adapter in self.adapters.items():
            try:
                res = await adapter.get_floor(collection_name)
                comp[mp] = res.data[0].get("floor_price") if res.ok and res.data else None
            except Exception:
                comp[mp] = None
        return comp

class AnalyticsService:
    async def get_market_stats(self) -> dict:
        async with get_session() as session:
            c_count = await session.scalar(select(func.count(Collection.id)))
            s_count = await session.scalar(select(func.count(PriceSnapshot.id)))
            return {"collections": c_count or 0, "snapshots": s_count or 0}

class NotificationService:
    def __init__(self, bot, aggregator):
        self.bot = bot
        self.aggregator = aggregator
    async def check_all(self) -> None:
        pass


# ============================================================
# KEYBOARDS & HANDLERS
# ============================================================
def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Аналитика рынка", callback_data="menu:analytics"),
         InlineKeyboardButton(text="🎁 Коллекции", callback_data="menu:collections")],
        [InlineKeyboardButton(text="🔄 Сравнение площадок", callback_data="menu:compare")],
        [InlineKeyboardButton(text="🔎 Поиск коллекции", callback_data="menu:search")],
        [InlineKeyboardButton(text="ℹ️ О проекте", callback_data="menu:about")],
    ])

def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")]
    ])

router = Router()
analytics_service = AnalyticsService()
aggregator = MarketplaceAggregator()

class SearchStates(StatesGroup):
    waiting_query = State()

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    async with get_session() as session:
        existing = await session.get(User, message.from_user.id)
        if not existing:
            session.add(User(id=message.from_user.id, username=message.from_user.username, first_name=message.from_user.first_name))
    await message.answer("📊 <b>GIFT ANALYTICS</b>\n\nАналитика рынка Telegram Gifts.\nВыберите раздел:", reply_markup=main_menu_keyboard(), parse_mode="HTML")

@router.callback_query(F.data == "menu:main")
async def back_to_main(callback: CallbackQuery) -> None:
    await callback.message.edit_text("📊 <b>GIFT ANALYTICS</b>\n\nВыберите раздел:", reply_markup=main_menu_keyboard(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "menu:about")
async def about(callback: CallbackQuery) -> None:
    await callback.message.edit_text("ℹ️ <b>О проекте</b>\n\nGift Analytics — агрегатор данных Telegram Gifts.", reply_markup=main_menu_keyboard(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "menu:analytics")
async def market_analytics(callback: CallbackQuery) -> None:
    stats = await analytics_service.get_market_stats()
    await callback.message.edit_text(f"📊 <b>Аналитика рынка</b>\n\n🎁 Коллекций: {stats['collections']}\n📸 Снимков: {stats['snapshots']}", reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "menu:collections")
async def collections_menu(callback: CallbackQuery) -> None:
    await callback.message.edit_text("🎁 <b>Коллекции</b>\n\nИспользуйте поиск.", reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "menu:search")
async def search_prompt(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(SearchStates.waiting_query)
    await callback.message.edit_text("🔎 <b>Поиск коллекции</b>\n\nВведите название коллекции:", reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()

@router.callback_query(F.data == "menu:compare")
async def compare_prompt(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(SearchStates.waiting_query)
    await callback.message.edit_text("🔄 <b>Сравнение площадок</b>\n\nВведите название коллекции:", reply_markup=back_keyboard(), parse_mode="HTML")
    await callback.answer()

@router.message(SearchStates.waiting_query)
async def search_execute(message: Message, state: FSMContext) -> None:
    query = (message.text or "").strip()
    await state.clear()
    if not query:
        await message.answer("Пустой запрос.", reply_markup=back_keyboard())
        return
    comparison = await aggregator.get_floor_comparison(query)
    lines = [f"🔎 <b>Результаты: {query}</b>\n"]
    for mp, floor in comparison.items():
        lines.append(f"🔹 {mp.capitalize()} — {f'{floor:.2f} TON' if floor else 'Нет данных'}")
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=back_keyboard())


# ============================================================
# SCHEDULER & WEB SERVER (FOR RENDER)
# ============================================================
async def handle_ping(request):
    return web.Response(text="Bot is alive!")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")

async def on_startup(bot: Bot) -> None:
    await init_db()
    asyncio.create_task(start_web_server())

    scheduler = AsyncIOScheduler()
    scheduler.add_job(aggregator.refresh_all, IntervalTrigger(seconds=1800), id="refresh_all", replace_existing=True)
    scheduler.start()
    logger.info("Scheduler started.")

async def on_shutdown(bot: Bot) -> None:
    await aggregator.close()
    await close_db()


# ============================================================
# MAIN ENTRY POINT
# ============================================================
async def main() -> None:
    if not Config.BOT_TOKEN:
        logger.error("BOT_TOKEN не задан!")
        return

    bot = Bot(token=Config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    dp.include_router(router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    logger.info("Starting bot...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
