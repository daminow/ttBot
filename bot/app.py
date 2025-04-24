import os
import asyncio
import uuid
import json
import logging
from itertools import combinations, cycle

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import (
    Column, Integer, BigInteger, String, TIMESTAMP, JSON, ForeignKey,
    func, select, and_, update as sa_update
)
from passlib.hash import bcrypt

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters
)

logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_URL = (
    f"postgresql+asyncpg://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_async_engine(DB_URL, future=True, echo=False)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


async def get_session():
    async with AsyncSessionLocal() as session:
        yield session


class Administrator(Base):
    __tablename__ = "administrators"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True)
    username = Column(String(64), nullable=False, unique=True)
    password = Column(String(128), nullable=False)
    role = Column(String(16), nullable=False, default="admin")
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


class RegCode(Base):
    __tablename__ = "reg_codes"
    code = Column(String(64), primary_key=True)
    role = Column(String(16), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


class Tournament(Base):
    __tablename__ = "tournaments"
    id = Column(Integer, primary_key=True)
    admin_id = Column(Integer, ForeignKey("administrators.id"), nullable=False)
    name = Column(String(255), nullable=False)
    tournament_type = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False, default="registration")
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    finished_at = Column(TIMESTAMP(timezone=True))
    data = Column(JSON, default={})
    players = relationship("Player", back_populates="tournament")
    rounds = relationship("Round", back_populates="tournament")


class Player(Base):
    __tablename__ = "players"
    id = Column(Integer, primary_key=True)
    tournament_id = Column(Integer, ForeignKey("tournaments.id"), nullable=False)
    name = Column(String(128), nullable=False)
    score = Column(Integer, default=0)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    tournament = relationship("Tournament", back_populates="players")


class Round(Base):
    __tablename__ = "rounds"
    id = Column(Integer, primary_key=True)
    tournament_id = Column(Integer, ForeignKey("tournaments.id"), nullable=False)
    round_type = Column(String(16), nullable=False)
    data = Column(JSON, nullable=False)
    status = Column(String(16), nullable=False, default="pending")
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    tournament = relationship("Tournament", back_populates="rounds")


class Match(Base):
    __tablename__ = "matches"
    id = Column(Integer, primary_key=True)
    round_id = Column(Integer, ForeignKey("rounds.id"), nullable=False)
    table_number = Column(Integer)
    player1_id = Column(Integer, ForeignKey("players.id"))
    player2_id = Column(Integer, ForeignKey("players.id"))
    result = Column(JSON, default={})
    status = Column(String(16), default="scheduled")
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


(
    AUTH_LOGIN, AUTH_PASS,
    CT_NAME, CT_TYPE, CT_TABLES, CT_PLAYERS,
    REG_NAME, REG_PASS,
    CHL_NEW, CHP_OLD, CHP_NEW
) = range(11)


def back_btn(cb="home"):
    return InlineKeyboardButton("⬅️ Назад", callback_data=cb)


async def require_login(update, ctx) -> bool:
    if "admin_id" in ctx.user_data:
        return False
    msg = "❌ Пожалуйста, авторизуйтесь (/start)."
    if update.callback_query:
        await update.callback_query.answer(msg, show_alert=True)
    else:
        await update.message.reply_text(msg)
    return True


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ Database initialized")


async def drop_forward(update, ctx):
    return


# ---------- AUTH ----------
async def start(update, ctx):
    uid = update.effective_user.id
    async for s in get_session():
        admin = (await s.execute(
            select(Administrator).where(Administrator.telegram_id == uid)
        )).scalar_one_or_none()
        if admin:
            ctx.user_data.update({"admin_id": admin.id, "role": admin.role})
            return await show_home(update, ctx)

    kb = [[InlineKeyboardButton("🔑 Войти", callback_data="auth_start")]]

    text = "👋 Добро пожаловать! Пожалуйста, авторизуйтесь:"
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(kb)
        )


async def auth_start(update, ctx):
    await update.callback_query.answer()
    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="auth_cancel")]]
    await update.callback_query.edit_message_text(
        "🔑 Введите логин:",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return AUTH_LOGIN


async def cancel_auth(update, ctx):
    if update.callback_query:
        await update.callback_query.answer()
    ctx.user_data.clear()
    await start(update, ctx)
    return ConversationHandler.END

async def auth_login(update, ctx):
    txt = update.message.text.strip()
    if txt.lower() == "назад":
        await start(update, ctx)
        return ConversationHandler.END

    ctx.user_data["login_try"] = txt
    kb = [[InlineKeyboardButton("⬅️ Назад", callback_data="auth_start")]]
    await update.message.reply_text(
        "💻 Введите пароль:",
        reply_markup=InlineKeyboardMarkup(kb)
    )
    return AUTH_PASS



async def auth_pass(update, ctx):
    pwd = update.message.text.strip()
    login = ctx.user_data.get("login_try")
    async for s in get_session():
        adm = (await s.execute(select(Administrator).where(Administrator.username == login))).scalar_one_or_none()
        if not adm:
            break
        try:
            valid = bcrypt.verify(pwd, adm.password)
        except ValueError:
            valid = pwd == adm.password
        if not valid:
            break
        adm.telegram_id = update.effective_user.id
        await s.commit()
        ctx.user_data.update({"admin_id": adm.id, "role": adm.role})
        await show_home(update, ctx)
        return ConversationHandler.END

    await update.message.reply_text("❌ Неверно. Попробуйте снова:", reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    return AUTH_LOGIN


# ---------- MAIN MENU ----------
async def show_home(update, ctx):
    kb = [
        [InlineKeyboardButton("➕ Новый турнир", callback_data="ct_start")],
        [InlineKeyboardButton("📜 История", callback_data="hist")],
        [InlineKeyboardButton("🎾 Активные", callback_data="act")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="settings")],
    ]
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("🏠 Главное меню:", reply_markup=InlineKeyboardMarkup(kb))
    else:
        await update.message.reply_text("🏠 Главное меню:", reply_markup=InlineKeyboardMarkup(kb))


# ---------- CREATE TOURNAMENT ----------
async def cancel_ct(update, ctx):
    if update.callback_query:
        await update.callback_query.answer()
    return ConversationHandler.END

async def ct_start(update, ctx):
    if await require_login(update, ctx):
        return ConversationHandler.END

    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        "🏆 Введите название турнира:",
        reply_markup=InlineKeyboardMarkup([[back_btn("home")]])
    )
    return CT_NAME


async def ct_name(update, ctx):
    txt = update.message.text.strip()
    if txt.lower() == "назад":
        return await show_home(update, ctx)
    ctx.user_data["ct_name"] = txt
    kb = [
        [InlineKeyboardButton("Beginner 🟢", callback_data="Beginner")],
        [InlineKeyboardButton("Advanced 🔵", callback_data="Advanced")],
        [back_btn()]
    ]
    await update.message.reply_text("🎯 Выберите тип:", reply_markup=InlineKeyboardMarkup(kb))
    return CT_TYPE


async def ct_type(update, ctx):
    ctx.user_data["ct_type"] = update.callback_query.data
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("🔢 Введите число столов (🏓):", reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    return CT_TABLES


async def ct_tables(update, ctx):
    txt = update.message.text.strip()
    if txt.lower() == "назад":
        return await show_home(update, ctx)
    if not txt.isdigit() or int(txt) < 1:
        return await update.message.reply_text("❌ Введите корректное число столов.")
    ctx.user_data["ct_tables"] = int(txt)
    await update.message.reply_text("👥 Введите игроков через запятую (по рейтингу):", reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    return CT_PLAYERS


async def ct_players(update, ctx):
    txt = update.message.text.strip()
    if txt.lower() == "назад":
        return await show_home(update, ctx)
    names = [n.strip() for n in txt.split(",") if n.strip()]
    async for s in get_session():
        tour = Tournament(
            admin_id=ctx.user_data["admin_id"],
            name=ctx.user_data["ct_name"],
            tournament_type=ctx.user_data["ct_type"],
            data={"tables": ctx.user_data["ct_tables"]}
        )
        s.add(tour)
        await s.flush()
        for nm in names:
            s.add(Player(tournament_id=tour.id, name=nm))
        await s.commit()
        tid = tour.id
    return await send_tournament_menu(update, ctx, tid)


# ---------- HISTORY & ACTIVE ----------
async def history_cb(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    async for s in get_session():
        tours = (await s.execute(select(Tournament).where(Tournament.status == "ended").order_by(Tournament.created_at.desc()).limit(4))).scalars().all()
    kb = [[InlineKeyboardButton(t.name, callback_data=f"show_{t.id}")] for t in tours]
    kb.append([back_btn()])
    await update.callback_query.edit_message_text("📜 История завершённых:", reply_markup=InlineKeyboardMarkup(kb))


async def active_cb(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    async for s in get_session():
        tours = (await s.execute(select(Tournament).where(Tournament.status != "ended"))).scalars().all()
    kb = [[InlineKeyboardButton(t.name, callback_data=f"show_{t.id}")] for t in tours]
    kb.append([back_btn()])
    await update.callback_query.edit_message_text("🎾 Активные турниры:", reply_markup=InlineKeyboardMarkup(kb))


# ---------- TOURNAMENT MENU ----------

async def send_tournament_menu(update, ctx, tid):
    ctx.user_data["tid"] = tid

    # Загружаем турнир, активный раунд и всех игроков
    async for s in get_session():
        tour = await s.get(Tournament, tid)
        active_rnd = (
            await s.execute(
                select(Round)
                .where(Round.tournament_id == tid, Round.status == "pending")
            )
        ).scalar_one_or_none()
        players = (
            await s.execute(
                select(Player).where(Player.tournament_id == tid)
            )
        ).scalars().all()

    # Базовый текст
    txt = (
        f"🏆 <b>{tour.name}</b>\n"
        f"📂 Тип: {tour.tournament_type}\n"
        f"🏓 Столов: {tour.data['tables']}\n"
        f"👥 Игроков: {len(players)}\n"
        f"📌 Статус: {tour.status}\n"
    )
    kb = []

    if not active_rnd:
        kb.append([InlineKeyboardButton("▶️ Начать простой", callback_data="round_simple")])
        kb.append([InlineKeyboardButton("▶️ Начать итоговый", callback_data="round_final")])
    else:
        # Все матчи этого раунда
        async for s in get_session():
            matches_all = (
                await s.execute(
                    select(Match).where(Match.round_id == active_rnd.id)
                )
            ).scalars().all()

        tables_count = tour.data["tables"]
        playing     = [m for m in matches_all if m.status == "playing"]
        done        = [m for m in matches_all if m.status == "done"]

        txt += (
            f"\n🔄 Раунд: {active_rnd.round_type}\n"
            f"Столов: {tables_count}, В процессе: {len(playing)}, Завершено: {len(done)}\n"
        )

        # Кнопки для каждой активной игры
        for m in playing:
            p1 = next(p for p in players if p.id == m.player1_id)
            p2 = next(p for p in players if p.id == m.player2_id)
            kb.append([InlineKeyboardButton(
                f"{p1.name} : {p2.name}",
                callback_data=f"match_{m.id}"
            )])

        # Если есть свободные столы — кнопка «▶️ Начать игру»
        if len(playing) < tables_count:
            kb.append([InlineKeyboardButton("▶️ Начать игру", callback_data="start_match")])

        # Если все игры завершены — кнопка «✅ Завершить раунд»
        if len(done) == len(matches_all):
            kb.append([InlineKeyboardButton("✅ Завершить раунд", callback_data="finish_round")])

    kb.append([back_btn("home")])

    # Отправляем или редактируем меню
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.edit_message_text(
                txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb)
            )
        except BadRequest as e:
            if "Message is not modified" in str(e):
                return
            raise
    else:
        await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text=txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb)
        )


async def start_match(update, ctx):
    logger.info("▶️ start_match called")
    if await require_login(update, ctx):
        logger.info("start_match: require_login blocked access")
        return

    await update.callback_query.answer()
    tid = ctx.user_data.get("tid")
    logger.info(f"start_match: tid from user_data = {tid!r}")
    if not tid:
        logger.warning("start_match: tid is None, aborting")
        return

    async for s in get_session():
        active_rnd = (await s.execute(
            select(Round).where(
                Round.tournament_id == tid,
                Round.status == "pending"
            )
        )).scalar_one_or_none()
        logger.info(f"start_match: active_rnd = {active_rnd!r}")

        if not active_rnd:
            logger.info("start_match: no pending round, returning to menu")
            return await send_tournament_menu(update, ctx, tid)

        pending = (await s.execute(
            select(Match).where(
                Match.round_id == active_rnd.id,
                Match.status == "scheduled"
            )
        )).scalars().all()
        logger.info(f"start_match: pending matches count = {len(pending)}")

        if not pending:
            logger.info("start_match: no scheduled matches to start")
            await update.callback_query.answer(
                "Нет доступных пар для старта.", show_alert=True
            )
            return await send_tournament_menu(update, ctx, tid)

        ids = {m.player1_id for m in pending} | {m.player2_id for m in pending}
        logger.info(f"start_match: loading players with ids = {ids}")
        players = (await s.execute(
            select(Player).where(Player.id.in_(ids))
        )).scalars().all()
        player_map = {p.id: p for p in players}
        logger.info(f"start_match: loaded players = {list(player_map.keys())}")

    kb = [
        [
            InlineKeyboardButton(
                f"{player_map[m.player1_id].name} : {player_map[m.player2_id].name}",
                callback_data=f"play_{m.id}"
            )
        ]
        for m in pending
    ]
    kb.append([ back_btn(f"show_{tid}") ])

    logger.info("start_match: sending keyboard with match options")
    await update.callback_query.edit_message_text(
        "Выберите пару для старта:",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def start_match(update, ctx):
    logger.info("▶️ start_match called")
    if await require_login(update, ctx):
        logger.info("start_match: require_login blocked access")
        return

    await update.callback_query.answer()
    tid = ctx.user_data.get("tid")
    logger.info(f"start_match: tid from user_data = {tid!r}")
    if not tid:
        logger.warning("start_match: tid is None, aborting")
        return

    async for s in get_session():
        # 1) Найти активный раунд
        active_rnd = (await s.execute(
            select(Round).where(
                Round.tournament_id == tid,
                Round.status == "pending"
            )
        )).scalar_one_or_none()
        logger.info(f"start_match: active_rnd = {active_rnd!r}")

        if not active_rnd:
            logger.info("start_match: no pending round, returning to menu")
            return await send_tournament_menu(update, ctx, tid)

        # 2) Собрать все scheduled-матчи
        pending = (await s.execute(
            select(Match).where(
                Match.round_id == active_rnd.id,
                Match.status == "scheduled"
            )
        )).scalars().all()
        logger.info(f"start_match: pending matches count = {len(pending)}")

        # 3) И все playing-матчи, чтобы узнать занятых игроков
        playing_matches = (await s.execute(
            select(Match).where(
                Match.round_id == active_rnd.id,
                Match.status == "playing"
            )
        )).scalars().all()
        logger.info(f"start_match: playing matches count = {len(playing_matches)}")
        playing_ids = {
            pid
            for m in playing_matches
            for pid in (m.player1_id, m.player2_id)
        }
        logger.info(f"start_match: currently playing player ids = {playing_ids}")

        # 4) Фильтруем pending — оставляем только пары без занятых игроков
        available = [
            m for m in pending
            if m.player1_id not in playing_ids and m.player2_id not in playing_ids
        ]
        logger.info(f"start_match: available matches count = {len(available)}")

        if not available:
            logger.info("start_match: no available matches due to players busy")
            await update.callback_query.answer(
                "Нет доступных пар для старта.", show_alert=True
            )
            return await send_tournament_menu(update, ctx, tid)

        # 5) Подгружаем данные игроков
        ids = {m.player1_id for m in available} | {m.player2_id for m in available}
        logger.info(f"start_match: loading players with ids = {ids}")
        players = (await s.execute(
            select(Player).where(Player.id.in_(ids))
        )).scalars().all()
        player_map = {p.id: p for p in players}
        logger.info(f"start_match: loaded players = {list(player_map.keys())}")

    # 6) Строим клавиатуру только из available-матчей
    kb = [
        [
            InlineKeyboardButton(
                f"{player_map[m.player1_id].name} : {player_map[m.player2_id].name}",
                callback_data=f"play_{m.id}"
            )
        ]
        for m in available
    ]
    kb.append([ back_btn(f"show_{tid}") ])

    logger.info("start_match: editing message with match list")
    await update.callback_query.edit_message_text(
        "Выберите пару для старта:",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def play_match(update, ctx):
    await update.callback_query.answer()
    mid = int(update.callback_query.data.split("_", 1)[1])
    async for s in get_session():
        m = await s.get(Match, mid)
        m.status = "playing"
        await s.commit()
    return await send_tournament_menu(update, ctx, ctx.user_data["tid"])


async def show_tournament(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    tid = int(update.callback_query.data.split("_")[1])
    return await send_tournament_menu(update, ctx, tid)


# ---------- EXPORT JSON ----------
async def export_json(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    tid = ctx.user_data["tid"]
    async for s in get_session():
        tour = await s.get(Tournament, tid)
        players = (await s.execute(select(Player).where(Player.tournament_id == tid))).scalars().all()
    data = {
        "id": tid,
        "name": tour.name,
        "type": tour.tournament_type,
        "tables": tour.data['tables'],
        "players": [{"id": p.id, "name": p.name, "score": p.score} for p in players]
    }
    fname = f"tour_{tid}.json"
    with open(fname, "w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    await ctx.bot.send_document(update.effective_chat.id, InputFile(fname), filename=fname)


# ---------- SIMPLE ROUND ----------
async def round_simple(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    tid = ctx.user_data["tid"]
    async for s in get_session():
        players = (await s.execute(select(Player).where(Player.tournament_id == tid).order_by(Player.score.desc()))).scalars().all()
        n = len(players)
        for tcount in range(2, n // 4 + 3, 2):
            size = -(-n // tcount)
            if 4 <= size <= 7:
                break
        tables = [[] for _ in range(tcount)]
        for p, idx in zip(players, cycle(range(tcount))):
            tables[idx].append(p)
        rnd = Round(tournament_id=tid, round_type="simple", data={"tables": [[p.id for p in tbl] for tbl in tables]})
        s.add(rnd)
        await s.flush()
        for idx, tbl in enumerate(tables, 1):
            for p1, p2 in combinations(tbl, 2):
                s.add(Match(round_id=rnd.id, table_number=idx, player1_id=p1.id, player2_id=p2.id))
        await s.execute(sa_update(Tournament).where(Tournament.id == tid).values(status="active"))
        await s.commit()
    return await send_tournament_menu(update, ctx, tid)


# ---------- FINAL ROUND ----------
async def round_final(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    tid = ctx.user_data["tid"]
    async for s in get_session():
        simple = (await s.execute(
            select(Round).where(
                Round.tournament_id == tid,
                Round.round_type == "simple",
                Round.status == "done"
            ).order_by(Round.created_at.desc())
        )).scalars().first()
        if not simple:
            return await update.callback_query.edit_message_text("❌ Сначала завершите простой раунд.")

        players = (await s.execute(select(Player).where(Player.tournament_id == tid).order_by(Player.score.desc()))).scalars().all()
        if len(players) != 8:
            return await update.callback_query.edit_message_text("❌ Итоговый только для 8 игроков.")

        bracket = [
            (players[0].id, players[7].id),
            (players[1].id, players[6].id),
            (players[4].id, players[3].id),
            (players[5].id, players[2].id)
        ]
        rnd = Round(tournament_id=tid, round_type="final", data={"matches": bracket})
        s.add(rnd)
        await s.flush()
        for idx, (a, b) in enumerate(bracket, 1):
            s.add(Match(round_id=rnd.id, table_number=idx, player1_id=a, player2_id=b))
        await s.execute(sa_update(Tournament).where(Tournament.id == tid).values(status="active"))
        await s.commit()
    return await send_tournament_menu(update, ctx, tid)


# ---------- MATCH ----------
async def match_cb(update, ctx):
    await update.callback_query.answer()
    mid = int(update.callback_query.data.split("_")[1])
    async for s in get_session():
        m  = await s.get(Match, mid)
        p1 = await s.get(Player, m.player1_id)
        p2 = await s.get(Player, m.player2_id)

    kb = [
        [InlineKeyboardButton(f"{p1.name} победил", callback_data=f"res_{mid}_1")],
        [InlineKeyboardButton(f"{p2.name} победил", callback_data=f"res_{mid}_2")],
        [back_btn(f"show_{ctx.user_data['tid']}")]
    ]
    await update.callback_query.edit_message_text(
        "Выберите победителя:", reply_markup=InlineKeyboardMarkup(kb)
    )



async def match_res(update, ctx):
    await update.callback_query.answer()
    _, mid, who = update.callback_query.data.split("_")
    mid, who = int(mid), int(who)
    async for s in get_session():
        m      = await s.get(Match, mid)
        winner = await s.get(Player, m.player1_id if who == 1 else m.player2_id)

    kb = [
        [InlineKeyboardButton("✅ Да", callback_data=f"confirm_{mid}_{who}")],
        [InlineKeyboardButton("❌ Нет", callback_data=f"match_{mid}")]
    ]
    await update.callback_query.edit_message_text(
        f"Подтвердить: <b>{winner.name}</b> победил?",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb)
    )

async def confirm_res(update, ctx):
    logger.info(f"confirm_res called with data={update.callback_query.data}")
    await update.callback_query.answer()
    _, mid, who = update.callback_query.data.split("_")
    mid, who = int(mid), int(who)

    async for s in get_session():
        m = await s.get(Match, mid)
        if m.status != "playing":
            return await send_tournament_menu(update, ctx, ctx.user_data["tid"])

        winner_id = m.player1_id if who == 1 else m.player2_id
        loser_id  = m.player2_id if who == 1 else m.player1_id

        m.result = {"winner": winner_id, "loser": loser_id}
        m.status = "done"

        rnd = await s.get(Round, m.round_id)
        if rnd.round_type == "simple":
            pw = await s.get(Player, winner_id)
            pl = await s.get(Player, loser_id)
            pw.score += 2
            pl.score += 1

        await s.commit()

        # если все матчи закрыты — завершаем раунд
        allm = (await s.execute(select(Match).where(Match.round_id == rnd.id))).scalars().all()
        if all(x.status == "done" for x in allm):
            rnd.status = "done"
            await s.commit()

    return await send_tournament_menu(update, ctx, ctx.user_data["tid"])

async def finish_round(update, ctx):
    return await send_tournament_menu(update, ctx, ctx.user_data["tid"])


# ---------- SETTINGS ----------
async def settings_cb(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    async for s in get_session():
        adm = await s.get(Administrator, ctx.user_data["admin_id"])
    kb = []
    if ctx.user_data.get("role") == "main":
        kb.append([InlineKeyboardButton("➕ Рег. код", callback_data="gen_code")])
        kb.append([InlineKeyboardButton("👥 Список админов", callback_data="list_admins")])
    kb.append([InlineKeyboardButton("✏️ Сменить логин", callback_data="change_login")])
    kb.append([InlineKeyboardButton("🔒 Сменить пароль", callback_data="change_pass")])
    kb.append([InlineKeyboardButton("🚪 Выйти", callback_data="logout")])
    kb.append([back_btn("home")])
    text = f"⚙️ Настройки\nТекущий логин: <b>{adm.username}</b>"
    await update.callback_query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))


from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import ConversationHandler

async def logout(update, ctx):
    query = update.callback_query
    await query.answer()

    try:
        async with AsyncSessionLocal() as session:
            admin = await session.get(Administrator, ctx.user_data.get("admin_id"))
            if admin:
                admin.telegram_id = None
                await session.commit()
    except Exception as e:
        logger.error(f"Logout DB error: {e}")
    ctx.user_data.clear()

    kb = [[InlineKeyboardButton("🔑 Войти", callback_data="auth_start")]]
    text = "👋 Вы успешно вышли из системы. Для входа нажмите кнопку:"

    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))
    except BadRequest:
        await ctx.bot.send_message(chat_id=update.effective_chat.id,
                                   text=text,
                                   reply_markup=InlineKeyboardMarkup(kb))

    return ConversationHandler.END


async def gen_code(update, ctx):
    if await require_login(update, ctx):
        return
    if ctx.user_data.get("role") != "main":
        return await update.callback_query.answer("Доступно только главному админу.", show_alert=True)
    await update.callback_query.answer()
    code = uuid.uuid4().hex[:8]
    async for s in get_session():
        s.add(RegCode(code=code, role="admin"))
        await s.commit()
    await update.callback_query.edit_message_text(f"🗝 Код: <code>{code}</code>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[back_btn("settings")]]))


async def list_admins(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    async for s in get_session():
        lst = (await s.execute(select(Administrator))).scalars().all()
    txt = "\n".join(f"{a.username} ({a.role})" for a in lst)
    await update.callback_query.edit_message_text("👥 Администраторы:\n" + txt, reply_markup=InlineKeyboardMarkup([[back_btn("settings")]]))


# ---------- REG FLOW ----------
async def reg_start(update, ctx):
    parts = update.message.text.split()
    if len(parts) != 2:
        await update.message.reply_text("❌ Используйте /reg <код>")
        return ConversationHandler.END
    code = parts[1]
    async for s in get_session():
        rc = await s.get(RegCode, code)
        if not rc:
            await update.message.reply_text("❌ Неверный код.")
            return ConversationHandler.END
        ctx.user_data["reg_role"] = rc.role
        await s.delete(rc)
        await s.commit()
    await update.message.reply_text("🔑 Введите логин нового админа:", reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    return REG_NAME


async def reg_name(update, ctx):
    txt = update.message.text.strip()
    if txt.lower() == "назад":
        return await show_home(update, ctx)
    ctx.user_data["reg_login"] = txt
    await update.message.reply_text("💻 Введите пароль нового админа:", reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    return REG_PASS


async def reg_pass(update, ctx):
    pwd = update.message.text.strip()
    if pwd.lower() == "назад":
        return await show_home(update, ctx)
    hashed = bcrypt.hash(pwd)
    async for s in get_session():
        s.add(Administrator(username=ctx.user_data["reg_login"], password=hashed, role=ctx.user_data["reg_role"]))
        await s.commit()
    await update.message.reply_text("✅ Администратор создан.")
    return await settings_cb(update, ctx)


# ---------- CHANGE LOGIN ----------
async def change_login_start(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("✏️ Введите новый логин:", reply_markup=InlineKeyboardMarkup([[back_btn("settings")]]))
    return CHL_NEW


async def change_login_new(update, ctx):
    new = update.message.text.strip()
    if new.lower() == "назад":
        return await settings_cb(update, ctx)
    async for s in get_session():
        if (await s.execute(select(Administrator).where(Administrator.username == new))).scalar_one_or_none():
            return await update.message.reply_text("❌ Логин занят.", reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    ctx.user_data["new_login"] = new
    kb = [[InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_login")], [back_btn("settings")]]
    await update.message.reply_text(f"Подтвердить новый логин <b>{new}</b>?", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END


async def confirm_login(update, ctx):
    await update.callback_query.answer()
    new = ctx.user_data["new_login"]
    async for s in get_session():
        await s.execute(sa_update(Administrator).where(Administrator.id == ctx.user_data["admin_id"]).values(username=new))
        await s.commit()
    await update.callback_query.edit_message_text("✅ Логин изменён.")
    return await settings_cb(update, ctx)


# ---------- CHANGE PASS ----------
async def change_pass_start(update, ctx):
    if await require_login(update, ctx):
        return
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("🔒 Введите старый пароль:", reply_markup=InlineKeyboardMarkup([[back_btn("settings")]]))
    return CHP_OLD


async def change_pass_old(update, ctx):
    old = update.message.text.strip()
    if old.lower() == "назад":
        return await settings_cb(update, ctx)
    async for s in get_session():
        adm = await s.get(Administrator, ctx.user_data["admin_id"])
        try:
            valid = bcrypt.verify(old, adm.password)
        except ValueError:
            valid = old == adm.password
        if not valid:
            return await update.message.reply_text("❌ Неверный пароль.", reply_markup=InlineKeyboardMarkup([[back_btn()]]))
    await update.message.reply_text("🔒 Введите новый пароль:", reply_markup=InlineKeyboardMarkup([[back_btn("settings")]]))
    return CHP_NEW


async def change_pass_new(update, ctx):
    new = update.message.text.strip()
    if new.lower() == "назад":
        return await settings_cb(update, ctx)
    ctx.user_data["new_pass"] = new
    kb = [[InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_pass")], [back_btn("settings")]]
    await update.message.reply_text("Подтвердить новый пароль?", reply_markup=InlineKeyboardMarkup(kb))
    return ConversationHandler.END


async def confirm_pass(update, ctx):
    await update.callback_query.answer()
    hashed = bcrypt.hash(ctx.user_data["new_pass"])
    async for s in get_session():
        await s.execute(sa_update(Administrator).where(Administrator.id == ctx.user_data["admin_id"]).values(password=hashed))
        await s.commit()
    await update.callback_query.edit_message_text("✅ Пароль изменён.")
    return await settings_cb(update, ctx)


# ---------- MAIN ----------
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(MessageHandler(filters.FORWARDED, drop_forward), group=0)

    async def drop_wh(app):
        await app.bot.delete_webhook(drop_pending_updates=True)

    app.post_init = drop_wh

    auth_conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CallbackQueryHandler(auth_start, pattern="^auth_start$")
        ],
        states={
            AUTH_LOGIN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, auth_login),
                CallbackQueryHandler(cancel_auth, pattern="^auth_cancel$")
            ],
            AUTH_PASS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, auth_pass),
                CallbackQueryHandler(cancel_auth, pattern="^auth_cancel$")
            ],
        },
        fallbacks=[
            CommandHandler("cancel", start),
            CallbackQueryHandler(cancel_auth, pattern="^auth_cancel$")
        ]
    )
    app.add_handler(auth_conv)

    ct_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(ct_start, pattern="^ct_start$")
        ],
        states={
            CT_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ct_name)
            ],
            CT_TYPE: [
                CallbackQueryHandler(ct_type, pattern="^(Beginner|Advanced)$")
            ],
            CT_TABLES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ct_tables)
            ],
            CT_PLAYERS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, ct_players)
            ],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_ct, pattern="^home$"),
            CommandHandler("cancel", start)
        ],
        allow_reentry=True
    )

    app.add_handler(ct_conv)

    app.add_handler(CallbackQueryHandler(show_home, pattern="^home$"))
    app.add_handler(CommandHandler("home", show_home))
    app.add_handler(CommandHandler("back", show_home))
    app.add_handler(CallbackQueryHandler(history_cb, pattern="^hist$"))
    app.add_handler(CallbackQueryHandler(active_cb, pattern="^act$"))

    app.add_handler(CallbackQueryHandler(show_tournament, pattern="^show_\\d+$"))
    app.add_handler(CallbackQueryHandler(export_json, pattern="^exp_json$"))

    app.add_handler(CallbackQueryHandler(round_simple, pattern="^round_simple$"))
    app.add_handler(CallbackQueryHandler(start_match, pattern="^start_match$"))
    app.add_handler(CallbackQueryHandler(round_final, pattern="^round_final$"))
    app.add_handler(CallbackQueryHandler(play_match,  pattern="^play_\\d+$"))
    app.add_handler(CallbackQueryHandler(confirm_res, pattern=r"^confirm_\d+_[12]$"))
    app.add_handler(CallbackQueryHandler(match_cb, pattern="^match_\\d+$"))
    app.add_handler(CallbackQueryHandler(match_res, pattern="^res_\\d+_[12]$"))
    app.add_handler(CallbackQueryHandler(finish_round, pattern="^finish_round$"))

    app.add_handler(CallbackQueryHandler(settings_cb, pattern="^settings$"))
    app.add_handler(CallbackQueryHandler(gen_code, pattern="^gen_code$"))
    app.add_handler(CallbackQueryHandler(list_admins, pattern="^list_admins$"))
    app.add_handler(CallbackQueryHandler(logout, pattern="^logout$"))

    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("reg", reg_start)],
        states={
            REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            REG_PASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_pass)],
        },
        fallbacks=[CommandHandler("cancel", start)]
    )
    app.add_handler(reg_conv)

    chlogin_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(change_login_start, pattern="^change_login$")],
        states={CHL_NEW: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_login_new)]},
        fallbacks=[CommandHandler("cancel", start)]
    )
    app.add_handler(chlogin_conv)
    app.add_handler(CallbackQueryHandler(confirm_login, pattern="^confirm_login$"))

    chpass_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(change_pass_start, pattern="^change_pass$")],
        states={
            CHP_OLD: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_pass_old)],
            CHP_NEW: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_pass_new)]
        },
        fallbacks=[CommandHandler("cancel", start)]
    )
    app.add_handler(chpass_conv)
    app.add_handler(CallbackQueryHandler(confirm_pass, pattern="^confirm_pass$"))

    logger.info("🚀 Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
