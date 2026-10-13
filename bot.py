import asyncio
import html
import logging
import os
import time
from contextlib import suppress
from urllib.parse import quote

import aiohttp
import asyncpg
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("refbot")

# ───────────────────────── Настройки ─────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
PIARFLOW_API_KEY = os.environ["PIARFLOW_API_KEY"]  # API key бота (из /traffic_bot/add)
DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x}
PORT = int(os.getenv("PORT", "10000"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "piarflow")
BOT_TITLE = os.getenv("BOT_TITLE", "💎 Реферальный бот")

REF_L1 = round(float(os.getenv("REF_L1_RUB", "5")) * 100)         # копейки
REF_L2 = round(float(os.getenv("REF_L2_RUB", "0.5")) * 100)       # копейки
MIN_WITHDRAW = round(float(os.getenv("MIN_WITHDRAW_RUB", "100")) * 100)
MAX_SPONSORS = int(os.getenv("MAX_SPONSORS", "5"))
# если PiarFlow не вернул заданий — пускать пользователя (иначе все встанут)
ALLOW_WITHOUT_SPONSORS = os.getenv("ALLOW_WITHOUT_SPONSORS", "1") == "1"

PIARFLOW_API_URL = "https://piarflow.com/v1"
DONE_STATUSES = {"subscribed", "not_counted"}
# не дёргать PiarFlow чаще, чем раз в N секунд на одного пользователя (защита от 429 при быстрых кликах)
CHECK_TTL = float(os.getenv("CHECK_TTL", "3"))

bot = Bot(
    BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True),
)
router = Router()
http: aiohttp.ClientSession | None = None
pending_links: dict[int, list[str]] = {}
last_ok: dict[int, float] = {}  # когда пользователь последний раз прошёл проверку заданий


# ───────────────────────── Утилиты ─────────────────────────
def rub(kop: int) -> str:
    kop = int(kop)
    return f"{kop // 100} ₽" if kop % 100 == 0 else f"{kop // 100}.{kop % 100:02d} ₽"


def name_of(user) -> str:
    return html.escape(user.first_name or "друг")


def mask(name: str | None) -> str:
    name = (name or "Аноним").strip()
    return html.escape(name[:2]) + "•••"


def bar(value: int, total: int, size: int = 10) -> str:
    filled = min(size, value * size // total) if total else size
    return "▰" * filled + "▱" * (size - filled)


def kb(*rows: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[list(r) for r in rows])


def cb_btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


BACK = [cb_btn("⬅️ В меню", "menu:home")]
LINE = "━━━━━━━━━━━━━━━━━━"


async def show(target: Message | CallbackQuery, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
    """Один «экран»: редактируем сообщение при нажатии на кнопку, иначе отправляем новое."""
    if isinstance(target, CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=markup)
        except TelegramBadRequest as e:
            if "not modified" not in str(e).lower():
                await target.message.answer(text, reply_markup=markup)
    else:
        await target.answer(text, reply_markup=markup)


async def ref_link(user_id: int) -> str:
    me = await bot.me()
    return f"https://t.me/{me.username}?start={user_id}"


# ───────────────────────── База данных ─────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    referrer_id BIGINT,
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    ref_paid BOOLEAN NOT NULL DEFAULT FALSE,
    balance BIGINT NOT NULL DEFAULT 0,
    earned_total BIGINT NOT NULL DEFAULT 0,
    withdrawn_total BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS users_referrer_idx ON users(referrer_id);
CREATE TABLE IF NOT EXISTS earnings (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    from_user_id BIGINT NOT NULL,
    level SMALLINT NOT NULL,
    amount BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS earnings_user_idx ON earnings(user_id);
CREATE TABLE IF NOT EXISTS withdrawals (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount BIGINT NOT NULL,
    details TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ
);
"""


class DB:
    pool: asyncpg.Pool

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0)
        await self.pool.execute(SCHEMA)

    async def close(self) -> None:
        await self.pool.close()

    async def get_user(self, uid: int):
        return await self.pool.fetchrow("SELECT * FROM users WHERE id=$1", uid)

    async def register(self, user, referrer_id: int | None) -> None:
        if referrer_id == user.id:
            referrer_id = None
        if referrer_id and not await self.pool.fetchval("SELECT 1 FROM users WHERE id=$1", referrer_id):
            referrer_id = None
        # реферер фиксируется только при первой регистрации
        await self.pool.execute(
            """INSERT INTO users(id, username, first_name, referrer_id) VALUES($1,$2,$3,$4)
               ON CONFLICT (id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name""",
            user.id, user.username, user.first_name, referrer_id,
        )

    async def set_unverified(self, uid: int) -> None:
        await self.pool.execute("UPDATE users SET verified=FALSE WHERE id=$1", uid)

    @staticmethod
    async def _credit(c, user_id: int, from_id: int, level: int, amount: int) -> None:
        await c.execute(
            "UPDATE users SET balance=balance+$2, earned_total=earned_total+$2 WHERE id=$1", user_id, amount
        )
        await c.execute(
            "INSERT INTO earnings(user_id, from_user_id, level, amount) VALUES($1,$2,$3,$4)",
            user_id, from_id, level, amount,
        )

    async def verify(self, uid: int) -> list[tuple[int, int, int]]:
        """Подтверждает подписку. Награда рефереру начисляется РОВНО ОДИН РАЗ."""
        payouts: list[tuple[int, int, int]] = []
        async with self.pool.acquire() as c:
            async with c.transaction():
                await c.execute("UPDATE users SET verified=TRUE WHERE id=$1", uid)
                claim = await c.fetchrow(
                    "UPDATE users SET ref_paid=TRUE WHERE id=$1 AND ref_paid=FALSE RETURNING referrer_id", uid
                )
                if claim and claim["referrer_id"]:
                    r1 = claim["referrer_id"]
                    await self._credit(c, r1, uid, 1, REF_L1)
                    payouts.append((r1, 1, REF_L1))
                    r2 = await c.fetchval("SELECT referrer_id FROM users WHERE id=$1", r1)
                    if r2 and REF_L2 > 0:
                        await self._credit(c, r2, uid, 2, REF_L2)
                        payouts.append((r2, 2, REF_L2))
        return payouts

    async def ref_stats(self, uid: int):
        return await self.pool.fetchrow(
            """SELECT
                (SELECT count(*) FROM users WHERE referrer_id=$1 AND ref_paid) AS l1,
                (SELECT count(*) FROM users WHERE referrer_id=$1 AND NOT ref_paid) AS pending,
                (SELECT count(*) FROM users u JOIN users r ON u.referrer_id=r.id
                    WHERE r.referrer_id=$1 AND u.ref_paid) AS l2,
                COALESCE((SELECT sum(amount) FROM earnings WHERE user_id=$1 AND level=1),0) AS e1,
                COALESCE((SELECT sum(amount) FROM earnings WHERE user_id=$1 AND level=2),0) AS e2""",
            uid,
        )

    async def top(self):
        return await self.pool.fetch(
            """SELECT u.id, u.first_name, count(*) AS cnt
               FROM users c JOIN users u ON u.id=c.referrer_id
               WHERE c.ref_paid GROUP BY u.id, u.first_name ORDER BY cnt DESC LIMIT 10"""
        )

    async def create_withdrawal(self, uid: int, amount: int, details: str):
        async with self.pool.acquire() as c:
            async with c.transaction():
                left = await c.fetchval(
                    "UPDATE users SET balance=balance-$1 WHERE id=$2 AND balance>=$1 RETURNING balance", amount, uid
                )
                if left is None:
                    return None
                return await c.fetchval(
                    "INSERT INTO withdrawals(user_id, amount, details) VALUES($1,$2,$3) RETURNING id",
                    uid, amount, details,
                )

    async def resolve_withdrawal(self, wid: int, approve: bool):
        status = "paid" if approve else "rejected"
        async with self.pool.acquire() as c:
            async with c.transaction():
                row = await c.fetchrow(
                    """UPDATE withdrawals SET status=$2, processed_at=now()
                       WHERE id=$1 AND status='pending' RETURNING user_id, amount""",
                    wid, status,
                )
                if not row:
                    return None
                if approve:
                    await c.execute(
                        "UPDATE users SET withdrawn_total=withdrawn_total+$2 WHERE id=$1", row["user_id"], row["amount"]
                    )
                else:
                    await c.execute("UPDATE users SET balance=balance+$2 WHERE id=$1", row["user_id"], row["amount"])
                return row

    async def admin_stats(self):
        return await self.pool.fetchrow(
            """SELECT
                (SELECT count(*) FROM users) AS total,
                (SELECT count(*) FROM users WHERE verified) AS verified,
                COALESCE((SELECT sum(amount) FROM earnings),0) AS earned,
                COALESCE((SELECT sum(amount) FROM withdrawals WHERE status='paid'),0) AS paid,
                (SELECT count(*) FROM withdrawals WHERE status='pending') AS pending"""
        )

    async def all_user_ids(self) -> list[int]:
        return [r["id"] for r in await self.pool.fetch("SELECT id FROM users")]


db = DB()


# ───────────────────────── PiarFlow ─────────────────────────
async def piarflow(path: str, payload: dict) -> dict:
    async with http.post(
        f"{PIARFLOW_API_URL}{path}",
        json=payload,
        headers={"Authorization": f"Bearer {PIARFLOW_API_KEY}", "Content-Type": "application/json"},
    ) as resp:
        data = await resp.json(content_type=None)
        if resp.status >= 400:
            raise RuntimeError(data.get("message") or f"PiarFlow error {resp.status}")
        return data


def chat_id_of(target: Message | CallbackQuery) -> int:
    return target.message.chat.id if isinstance(target, CallbackQuery) else target.chat.id


async def fetch_sponsors(user_id: int, chat_id: int) -> list[dict]:
    data = await piarflow("/sponsors", {"user_id": user_id, "chat_id": chat_id, "max_sponsors": MAX_SPONSORS})
    return data.get("sponsors") or []


def not_done(sponsors: list[dict]) -> list[dict]:
    return [s for s in sponsors if s.get("status") not in DONE_STATUSES]


async def open_gate(target: Message | CallbackQuery, user, sponsors: list[dict] | None = None, new: bool = False) -> None:
    """Показывает задания PiarFlow. Если всё выполнено / заданий нет — открывает доступ."""
    retry = kb([cb_btn("🔄 Повторить", "gate:check")])
    if sponsors is None:
        try:
            sponsors = await fetch_sponsors(user.id, chat_id_of(target))
        except Exception as e:
            log.exception("PiarFlow /sponsors failed")
            extra = f"\n\n<code>{html.escape(str(e))[:300]}</code>" if user.id in ADMIN_IDS else ""
            await show(
                target,
                "⚠️ <b>Сервис заданий временно недоступен</b>\n\nПопробуй ещё раз через минуту." + extra,
                retry,
            )
            return

    todo = [s for s in sponsors if s.get("status") not in DONE_STATUSES]
    if not todo:
        if sponsors or ALLOW_WITHOUT_SPONSORS:
            pending_links.pop(user.id, None)
            await grant_access(target, user)
        else:
            await show(target, "😔 Сейчас нет доступных заданий.\nЗагляни чуть позже.", retry)
        return

    pending_links[user.id] = [s["link"] for s in todo]
    rows = [[InlineKeyboardButton(text=f"📢 Задание {i}", url=s["link"])] for i, s in enumerate(todo, 1)]
    rows.append([cb_btn("✅ Проверить подписки", "gate:check")])
    title = "🆕 <b>Появились новые задания</b>" if new else "🔐 <b>Открой доступ к боту</b>"
    text = (
        f"{title}\n{LINE}\n\n"
        "Подпишись на каналы ниже — это бесплатно и займёт минуту.\n"
        "Затем нажми <b>«Проверить подписки»</b>.\n\n"
        "✨ <i>Приглашённые тобой друзья засчитываются только после подписки на каналы.</i>"
    )
    await show(target, text, InlineKeyboardMarkup(inline_keyboard=rows))


async def grant_access(target: Message | CallbackQuery, user) -> None:
    last_ok[user.id] = time.monotonic()
    payouts = await db.verify(user.id)
    for referrer_id, level, amount in payouts:
        text = (
            f"🎉 <b>Новый реферал!</b>\n{LINE}\nДруг подписался на каналы.\n💰 Начислено: <b>+{rub(amount)}</b>"
            if level == 1
            else f"🔗 <b>Реферал 2 уровня!</b>\n{LINE}\nТвой друг пригласил нового участника.\n💰 Начислено: <b>+{rub(amount)}</b>"
        )
        with suppress(Exception):
            await bot.send_message(referrer_id, text)
    await show_menu(target, user)


# ───────────────────────── Экраны ─────────────────────────
async def show_menu(target: Message | CallbackQuery, user) -> None:
    row = await db.get_user(user.id)
    st = await db.ref_stats(user.id)
    text = (
        f"<b>{html.escape(BOT_TITLE)}</b>\n{LINE}\n\n"
        f"👋 Привет, <b>{name_of(user)}</b>!\n\n"
        f"💰 Баланс: <b>{rub(row['balance'])}</b>\n"
        f"👥 Приглашено: <b>{st['l1'] + st['pending']}</b> · засчитано: <b>{st['l1']}</b>\n\n"
        f"🎁 За друга — <b>+{rub(REF_L1)}</b>\n"
        f"🔗 За друга друга — <b>+{rub(REF_L2)}</b>\n"
        f"💸 Вывод от <b>{rub(MIN_WITHDRAW)}</b>"
    )
    markup = kb(
        [cb_btn("👥 Рефералы", "menu:refs"), cb_btn("💰 Баланс", "menu:balance")],
        [cb_btn("💸 Вывод", "menu:withdraw"), cb_btn("🏆 Топ", "menu:top")],
        [cb_btn("ℹ️ Как это работает", "menu:help")],
    )
    await show(target, text, markup)


@router.callback_query(F.data == "menu:home")
async def cb_home(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await cb.answer()
    await show_menu(cb, cb.from_user)


@router.callback_query(F.data == "menu:refs")
async def cb_refs(cb: CallbackQuery) -> None:
    await cb.answer()
    st = await db.ref_stats(cb.from_user.id)
    link = await ref_link(cb.from_user.id)
    text = (
        f"👥 <b>Мои рефералы</b>\n{LINE}\n\n"
        f"🔗 Твоя ссылка:\n<code>{link}</code>\n\n"
        f"1️⃣ <b>1 уровень</b> · +{rub(REF_L1)}\n"
        f"   ✅ Засчитано: <b>{st['l1']}</b>\n"
        f"   ⏳ Ждут подписки: <b>{st['pending']}</b>\n"
        f"   💵 Заработано: <b>{rub(st['e1'])}</b>\n\n"
        f"2️⃣ <b>2 уровень</b> · +{rub(REF_L2)}\n"
        f"   ✅ Засчитано: <b>{st['l2']}</b>\n"
        f"   💵 Заработано: <b>{rub(st['e2'])}</b>"
    )
    share = f"https://t.me/share/url?url={quote(link)}&text={quote('Зарабатывай деньги за приглашения 💰')}"
    await show(cb, text, kb([InlineKeyboardButton(text="📤 Поделиться ссылкой", url=share)], BACK))


@router.callback_query(F.data == "menu:balance")
async def cb_balance(cb: CallbackQuery) -> None:
    await cb.answer()
    row = await db.get_user(cb.from_user.id)
    pct = min(100, row["balance"] * 100 // MIN_WITHDRAW)
    text = (
        f"💰 <b>Мой баланс</b>\n{LINE}\n\n"
        f"💵 Доступно: <b>{rub(row['balance'])}</b>\n"
        f"📈 Всего заработано: <b>{rub(row['earned_total'])}</b>\n"
        f"💸 Выведено: <b>{rub(row['withdrawn_total'])}</b>\n\n"
        f"До вывода ({rub(MIN_WITHDRAW)}):\n{bar(row['balance'], MIN_WITHDRAW)} <b>{pct}%</b>"
    )
    await show(cb, text, kb([cb_btn("💸 Вывести", "menu:withdraw")], BACK))


@router.callback_query(F.data == "menu:top")
async def cb_top(cb: CallbackQuery) -> None:
    await cb.answer()
    rows = await db.top()
    medals = ["🥇", "🥈", "🥉"]
    if rows:
        lines = [
            f"{medals[i] if i < 3 else f'{i + 1}.'} {mask(r['first_name'])} — <b>{r['cnt']}</b>"
            for i, r in enumerate(rows)
        ]
        body = "\n".join(lines)
    else:
        body = "Пока никого нет. Стань первым! 🚀"
    await show(cb, f"🏆 <b>Топ рефоводов</b>\n{LINE}\n\n{body}", kb(BACK))


@router.callback_query(F.data == "menu:help")
async def cb_help(cb: CallbackQuery) -> None:
    await cb.answer()
    text = (
        f"ℹ️ <b>Как это работает</b>\n{LINE}\n\n"
        "1️⃣ Подпишись на каналы-спонсоры — доступ открыт\n"
        "2️⃣ Возьми свою ссылку в разделе «Рефералы»\n"
        f"3️⃣ Друг запускает бота и подписывается на каналы — тебе <b>+{rub(REF_L1)}</b>\n"
        f"4️⃣ Когда друг приглашает ещё одного — тебе <b>+{rub(REF_L2)}</b>\n"
        f"5️⃣ Вывод от <b>{rub(MIN_WITHDRAW)}</b>, заявки обрабатываются вручную\n\n"
        "⚠️ <i>Если отписаться от каналов, доступ к боту закроется до повторной подписки.</i>"
    )
    await show(cb, text, kb(BACK))


# ───────────────────────── Старт и проверка подписок ─────────────────────────
@router.message(CommandStart())
async def cmd_start(m: Message, command: CommandObject, state: FSMContext) -> None:
    await state.clear()
    ref = int(command.args) if command.args and command.args.isdigit() else None
    await db.register(m.from_user, ref)
    row = await db.get_user(m.from_user.id)
    if row["verified"]:
        await show_menu(m, m.from_user)
    else:
        await open_gate(m, m.from_user)


@router.message(Command("menu"))
async def cmd_menu(m: Message, state: FSMContext) -> None:
    await state.clear()
    await show_menu(m, m.from_user)


@router.callback_query(F.data == "gate:check")
async def cb_gate_check(cb: CallbackQuery) -> None:
    uid = cb.from_user.id
    links = pending_links.get(uid)
    if not links:
        await cb.answer()
        await open_gate(cb, cb.from_user)
        return
    try:
        result = await piarflow("/sponsors/check", {"user_id": uid, "links": links})
    except Exception:
        log.exception("PiarFlow /sponsors/check failed")
        await cb.answer("⚠️ Сервис проверки недоступен, попробуй через минуту", show_alert=True)
        return
    items = result.get("sponsors") or []
    left = sum(1 for i in items if i.get("status") not in DONE_STATUSES)
    if items and left == 0:
        pending_links.pop(uid, None)
        await cb.answer("✅ Подписки подтверждены!")
        await grant_access(cb, cb.from_user)
    else:
        await cb.answer(
            f"❌ Не выполнено заданий: {left or len(links)}. Подпишись и нажми «Проверить» снова.",
            show_alert=True,
        )


# ───────────────────────── Вывод средств ─────────────────────────
class Withdraw(StatesGroup):
    amount = State()
    details = State()


CANCEL = kb([cb_btn("✖️ Отмена", "wd:cancel")])


@router.callback_query(F.data == "menu:withdraw")
async def cb_withdraw(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await cb.answer()
    row = await db.get_user(cb.from_user.id)
    if row["balance"] < MIN_WITHDRAW:
        text = (
            f"💸 <b>Вывод средств</b>\n{LINE}\n\n"
            f"Минимальная сумма: <b>{rub(MIN_WITHDRAW)}</b>\n"
            f"Твой баланс: <b>{rub(row['balance'])}</b>\n"
            f"{bar(row['balance'], MIN_WITHDRAW)}\n\n"
            f"Нужно ещё <b>{rub(MIN_WITHDRAW - row['balance'])}</b> 💪"
        )
        await show(cb, text, kb([cb_btn("👥 Пригласить друзей", "menu:refs")], BACK))
        return
    text = f"💸 <b>Вывод средств</b>\n{LINE}\n\nДоступно к выводу: <b>{rub(row['balance'])}</b>"
    await show(
        cb,
        text,
        kb(
            [cb_btn(f"💸 Вывести всё ({rub(row['balance'])})", "wd:all")],
            [cb_btn("✏️ Указать сумму", "wd:amount")],
            BACK,
        ),
    )


@router.callback_query(F.data == "wd:cancel")
async def cb_wd_cancel(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await cb.answer("Отменено")
    await show_menu(cb, cb.from_user)


DETAILS_PROMPT = (
    "💳 <b>Реквизиты для выплаты</b>\n\n"
    "Отправь одним сообщением: номер карты / телефон (СБП) / кошелёк и название банка или платёжной системы."
)


@router.callback_query(F.data == "wd:all")
async def cb_wd_all(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    row = await db.get_user(cb.from_user.id)
    if row["balance"] < MIN_WITHDRAW:
        await show_menu(cb, cb.from_user)
        return
    await state.update_data(amount=row["balance"])
    await state.set_state(Withdraw.details)
    await show(cb, f"💸 Сумма: <b>{rub(row['balance'])}</b>\n\n{DETAILS_PROMPT}", CANCEL)


@router.callback_query(F.data == "wd:amount")
async def cb_wd_amount(cb: CallbackQuery, state: FSMContext) -> None:
    await cb.answer()
    row = await db.get_user(cb.from_user.id)
    await state.set_state(Withdraw.amount)
    await show(
        cb,
        f"✏️ Введи сумму в рублях: от <b>{rub(MIN_WITHDRAW)}</b> до <b>{rub(row['balance'])}</b>",
        CANCEL,
    )


@router.message(Withdraw.amount)
async def msg_wd_amount(m: Message, state: FSMContext) -> None:
    row = await db.get_user(m.from_user.id)
    try:
        amount = round(float((m.text or "").replace(",", ".").replace("₽", "").strip()) * 100)
    except ValueError:
        await m.answer("❌ Введи число, например <code>150</code>", reply_markup=CANCEL)
        return
    if amount < MIN_WITHDRAW or amount > row["balance"]:
        await m.answer(
            f"❌ Сумма должна быть от <b>{rub(MIN_WITHDRAW)}</b> до <b>{rub(row['balance'])}</b>", reply_markup=CANCEL
        )
        return
    await state.update_data(amount=amount)
    await state.set_state(Withdraw.details)
    await m.answer(f"💸 Сумма: <b>{rub(amount)}</b>\n\n{DETAILS_PROMPT}", reply_markup=CANCEL)


@router.message(Withdraw.details)
async def msg_wd_details(m: Message, state: FSMContext) -> None:
    details = (m.text or "").strip()
    if not 5 <= len(details) <= 200:
        await m.answer("❌ Реквизиты должны быть длиной от 5 до 200 символов.", reply_markup=CANCEL)
        return
    data = await state.get_data()
    await state.clear()
    amount = data.get("amount", 0)
    wid = await db.create_withdrawal(m.from_user.id, amount, details) if amount else None
    if wid is None:
        await m.answer("❌ Не удалось создать заявку: недостаточно средств.", reply_markup=kb(BACK))
        return
    await m.answer(
        f"✅ <b>Заявка #{wid} создана</b>\n{LINE}\n\n"
        f"💸 Сумма: <b>{rub(amount)}</b>\n⏳ Статус: на проверке\n\n"
        "Мы сообщим, когда выплата будет произведена.",
        reply_markup=kb(BACK),
    )
    u = m.from_user
    admin_text = (
        f"💸 <b>Заявка на вывод #{wid}</b>\n{LINE}\n"
        f"👤 <a href=\"tg://user?id={u.id}\">{name_of(u)}</a>"
        f"{' (@' + html.escape(u.username) + ')' if u.username else ''}\nID: <code>{u.id}</code>\n"
        f"💰 Сумма: <b>{rub(amount)}</b>\n💳 Реквизиты:\n<code>{html.escape(details)}</code>"
    )
    markup = kb([cb_btn("✅ Выплачено", f"adm:ok:{wid}"), cb_btn("❌ Отклонить", f"adm:no:{wid}")])
    for admin in ADMIN_IDS:
        with suppress(Exception):
            await bot.send_message(admin, admin_text, reply_markup=markup)


# ───────────────────────── Админка ─────────────────────────
@router.callback_query(F.data.startswith("adm:"))
async def cb_admin(cb: CallbackQuery) -> None:
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("Нет доступа", show_alert=True)
        return
    _, action, wid = cb.data.split(":")
    approve = action == "ok"
    row = await db.resolve_withdrawal(int(wid), approve)
    if not row:
        await cb.answer("Заявка уже обработана", show_alert=True)
        return
    await cb.answer("Готово")
    mark = "✅ ВЫПЛАЧЕНО" if approve else "❌ ОТКЛОНЕНО (баланс возвращён)"
    with suppress(Exception):
        await cb.message.edit_text(f"{cb.message.html_text}\n\n<b>{mark}</b>")
    user_text = (
        f"✅ <b>Выплата произведена!</b>\n{LINE}\nЗаявка #{wid} на <b>{rub(row['amount'])}</b> выполнена. Спасибо! 💙"
        if approve
        else f"❌ <b>Заявка #{wid} отклонена</b>\n{LINE}\n<b>{rub(row['amount'])}</b> возвращены на баланс."
    )
    with suppress(Exception):
        await bot.send_message(row["user_id"], user_text)


@router.message(Command("stats"), F.from_user.id.in_(ADMIN_IDS))
async def cmd_stats(m: Message) -> None:
    s = await db.admin_stats()
    await m.answer(
        f"📊 <b>Статистика</b>\n{LINE}\n"
        f"👥 Пользователей: <b>{s['total']}</b>\n✅ Подписаны: <b>{s['verified']}</b>\n"
        f"💰 Начислено всего: <b>{rub(s['earned'])}</b>\n💸 Выплачено: <b>{rub(s['paid'])}</b>\n"
        f"⏳ Заявок в ожидании: <b>{s['pending']}</b>"
    )


@router.message(Command("broadcast"), F.from_user.id.in_(ADMIN_IDS))
async def cmd_broadcast(m: Message, command: CommandObject) -> None:
    if not command.args:
        await m.answer("Использование: <code>/broadcast текст</code>")
        return
    ok = 0
    for uid in await db.all_user_ids():
        try:
            await bot.send_message(uid, command.args)
            ok += 1
        except Exception:
            pass
        await asyncio.sleep(0.05)
    await m.answer(f"📣 Отправлено: {ok}")


# Любое другое сообщение → меню (должен быть последним)
@router.message(F.chat.type == "private")
async def fallback(m: Message) -> None:
    await show_menu(m, m.from_user)


# ───────────────────────── Доступ только после подписки ─────────────────────────
class AccessMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is None:
            return await handler(event, data)
        if isinstance(event, Message):
            text = event.text or ""
            if text.startswith("/start"):
                return await handler(event, data)
            if user.id in ADMIN_IDS and text.startswith(("/stats", "/broadcast")):
                return await handler(event, data)
        if isinstance(event, CallbackQuery) and (event.data or "").startswith(("gate:", "adm:")):
            return await handler(event, data)

        row = await db.get_user(user.id)
        if row is None:
            await db.register(user, None)
            row = await db.get_user(user.id)
        if row["verified"]:
            # проверяем КАЖДОЕ действие: вдруг появились новые задания
            if time.monotonic() - last_ok.get(user.id, 0) < CHECK_TTL:
                return await handler(event, data)
            try:
                sponsors = await fetch_sponsors(user.id, chat_id_of(event))
            except Exception:
                log.exception("PiarFlow recheck failed")  # сервис лёг — не блокируем уже подтверждённых
                return await handler(event, data)
            if not_done(sponsors):
                if isinstance(event, CallbackQuery):
                    await event.answer()
                await open_gate(event, user, sponsors, new=True)
                return
            last_ok[user.id] = time.monotonic()
            return await handler(event, data)

        if isinstance(event, CallbackQuery):
            await event.answer()
        await open_gate(event, user)


router.message.outer_middleware(AccessMiddleware())
router.callback_query.outer_middleware(AccessMiddleware())


# ───────────────────────── Веб-сервер (Render + UptimeRobot) ─────────────────────────
async def health(_: web.Request) -> web.Response:
    return web.Response(text="ok")


async def piarflow_webhook(request: web.Request) -> web.Response:
    """Вебхук отписок PiarFlow: закрываем доступ до повторной подписки."""
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False}, status=400)
    if payload.get("status") == "unsubscribed" and not payload.get("test") and payload.get("tg_user_id"):
        await db.set_unverified(int(payload["tg_user_id"]))
    return web.json_response({"ok": True})


async def main() -> None:
    global http
    await db.connect()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    app.router.add_post(f"/piarflow/{WEBHOOK_SECRET}", piarflow_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Web server on port %s", PORT)

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=False)
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await http.close()
        await runner.cleanup()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
