import os
import asyncio
import uuid
import json
import logging
from itertools import combinations

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import (
    Column, Integer, BigInteger, String, TIMESTAMP, JSON, ForeignKey,
    func, select, update
)
from passlib.hash import bcrypt

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters, ContextTypes
)

# — Logging —
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# — Config from ENV —
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_URL = (
    f"postgresql+asyncpg://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# — Database setup —
engine = create_async_engine(DB_URL, future=True, echo=False)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_session():
    async with AsyncSessionLocal() as session:
        yield session

# — Models —
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

# — Conversation states —
(
    AUTH_LOGIN, AUTH_PASS,
    REG_NAME, REG_PASS,
    CT_NAME, CT_TYPE, CT_TABLES, CT_PLAYERS,
    CHL_NEW, CHP_OLD, CHP_NEW
) = range(11)

# — Helpers —
def back_button(data="home"):
    return InlineKeyboardButton("⬅️ Назад", callback_data=data)

def require_login(update: Update, ctx):
    if "admin_id" not in ctx.user_data:
        # если нет авторизации — отправляем подсказку
        if update.callback_query:
            return update.callback_query.answer("❌ Пожалуйста, авторизуйтесь: /start", show_alert=True)
        else:
            return update.message.reply_text("❌ Пожалуйста, авторизуйтесь: /start")
    return None

# — Initialize DB —
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ Database initialized")

# — /start handler —
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    async for s in get_session():
        admin = (await s.execute(
            select(Administrator).where(Administrator.telegram_id == uid)
        )).scalar_one_or_none()
        if admin:
            ctx.user_data.update({"admin_id": admin.id, "role": admin.role})
            return await show_home(update, ctx)
    # иначе — попросим авторизовать
    kb = [[InlineKeyboardButton("🔑 Войти", callback_data="auth_start")]]
    await update.message.reply_text("👋 Добро пожаловать! Авторизуйтесь:", 
        reply_markup=InlineKeyboardMarkup(kb))

# — Auth flow —
async def auth_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        "🔑 Введите логин:", reply_markup=InlineKeyboardMarkup([[back_button()]])
    )
    return AUTH_LOGIN

async def auth_login(update: Update, ctx):
    text = update.message.text.strip()
    if text.lower() == "назад":
        return await show_home(update, ctx)
    ctx.user_data["auth_login"] = text
    await update.message.reply_text(
        "💻 Введите пароль:", 
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="auth_start")]])
    )
    return AUTH_PASS

async def auth_pass(update: Update, ctx):
    pwd = update.message.text.strip()
    login = ctx.user_data.get("auth_login")
    async for s in get_session():
        admin = (await s.execute(
            select(Administrator).where(Administrator.username == login)
        )).scalar_one_or_none()
        if admin and bcrypt.verify(pwd, admin.password):
            admin.telegram_id = update.effective_user.id
            await s.commit()
            ctx.user_data.update({"admin_id": admin.id, "role": admin.role})
            return await show_home(update, ctx)
    await update.message.reply_text(
        "❌ Неверно. Попробуйте снова:", 
        reply_markup=InlineKeyboardMarkup([[back_button()]])
    )
    return AUTH_LOGIN

# — Cancel fallback —
async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    return await show_home(update, ctx)

# — Main menu —
async def show_home(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("➕ Новый турнир", callback_data="ct_start")],
        [InlineKeyboardButton("📜 История", callback_data="hist")],
        [InlineKeyboardButton("🎾 Активные", callback_data="act")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="settings")],
    ]
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "🏠 Главное меню:", reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        await update.message.reply_text("🏠 Главное меню:", reply_markup=InlineKeyboardMarkup(kb))

# — Create Tournament flow —
async def ct_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        "🏆 Введите название турнира:", reply_markup=InlineKeyboardMarkup([[back_button()]])
    )
    return CT_NAME

async def ct_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    if txt.lower() == "назад":
        return await show_home(update, ctx)
    ctx.user_data["ct_name"] = txt
    kb = [
        [InlineKeyboardButton("Beginner 🟢", callback_data="Beginner")],
        [InlineKeyboardButton("Advanced 🔵", callback_data="Advanced")],
        [back_button()]
    ]
    await update.message.reply_text("🎯 Выберите тип турнира:", reply_markup=InlineKeyboardMarkup(kb))
    return CT_TYPE

async def ct_type(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    tp = update.callback_query.data
    ctx.user_data["ct_type"] = tp
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        "🔢 Введите число столов:", reply_markup=InlineKeyboardMarkup([[back_button()]])
    )
    return CT_TABLES

async def ct_tables(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    if txt.lower() == "назад":
        return await show_home(update, ctx)
    if not txt.isdigit() or int(txt) < 1:
        return await update.message.reply_text("❌ Введите корректное число столов.")
    ctx.user_data["ct_tables"] = int(txt)
    await update.message.reply_text(
        "👥 Введите игроков через запятую (по рейтингу):",
        reply_markup=InlineKeyboardMarkup([[back_button()]])
    )
    return CT_PLAYERS

async def ct_players(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
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
        s.add(tour); await s.flush()
        for nm in names:
            s.add(Player(tournament_id=tour.id, name=nm))
        await s.commit()
    await update.message.reply_text(f"✅ Турнир «{ctx.user_data['ct_name']}» создан.")
    return await show_tournament_placeholder(update, ctx, tour.id)

# Заглушка: сразу переходим в меню турнира после создания
async def show_tournament_placeholder(update, ctx, tid):
    ctx.user_data["tid"] = tid
    # симулируем callback для открытия меню турнира
    class C: data=f"show_{tid}"
    update_cb = type("U",(object,),{"data":f"show_{tid}","message":update.effective_message,"answer":update.callback_query.answer if update.callback_query else lambda **_:None})
    return await show_tournament(update_cb, ctx)

# — History & Active —
async def history_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    async for s in get_session():
        tours = (await s.execute(
            select(Tournament).order_by(Tournament.created_at.desc()).limit(4)
        )).scalars().all()
    kb = [[InlineKeyboardButton(t.name, callback_data=f"show_{t.id}")] for t in tours]
    kb.append([back_button()])
    await update.callback_query.edit_message_text(
        "📜 Последние турниры:", reply_markup=InlineKeyboardMarkup(kb)
    )

async def active_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    async for s in get_session():
        tours = (await s.execute(
            select(Tournament).where(Tournament.status != "ended")
        )).scalars().all()
    kb = [[InlineKeyboardButton(t.name, callback_data=f"show_{t.id}")] for t in tours]
    kb.append([back_button()])
    await update.callback_query.edit_message_text(
        "🎾 Активные турниры:", reply_markup=InlineKeyboardMarkup(kb)
    )

# — Show tournament & JSON & Round Menu in tournament context —
async def show_tournament(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    tid = int(update.callback_query.data.split("_")[1])
    ctx.user_data["tid"] = tid
    async for s in get_session():
        tour = await s.get(Tournament, tid)
        players = (await s.execute(
            select(Player).where(Player.tournament_id == tid)
        )).scalars().all()
    txt = (
        f"🏆 {tour.name}\n"
        f"📂 Тип: {tour.tournament_type}\n"
        f"🏓 Столов: {tour.data['tables']}\n"
        f"👥 Игроков: {len(players)}\n"
        f"📌 Статус: {tour.status}"
    )
    kb = [
        [InlineKeyboardButton("🔄 Раунд", callback_data="round_menu")],
        [InlineKeyboardButton("⬇️ JSON", callback_data="exp_json")],
        [back_button()]
    ]
    await update.callback_query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def export_json(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    tid = ctx.user_data["tid"]
    async for s in get_session():
        tour = await s.get(Tournament, tid)
        players = (await s.execute(
            select(Player).where(Player.tournament_id == tid)
        )).scalars().all()
    data = {
        "id": tid,
        "name": tour.name,
        "type": tour.tournament_type,
        "tables": tour.data["tables"],
        "players": [{"id": p.id, "name": p.name, "score": p.score} for p in players]
    }
    fname = f"tournament_{tid}.json"
    with open(fname, "w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    await ctx.bot.send_document(update.effective_chat.id, InputFile(fname), filename=fname)

# — Settings —
async def settings_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    async for s in get_session():
        admin = await s.get(Administrator, ctx.user_data["admin_id"])
    kb = []
    if is_main(ctx):
        kb.append([InlineKeyboardButton("➕ Рег. код", callback_data="gen_code")])
        kb.append([InlineKeyboardButton("👥 Список админов", callback_data="list_admins")])
    kb.append([InlineKeyboardButton(f"✏️ Логин: {admin.username}", callback_data="change_login")])
    kb.append([InlineKeyboardButton("🔒 Пароль: ******", callback_data="change_pass")])
    kb.append([back_button()])
    await update.callback_query.edit_message_text("⚙️ Настройки:", reply_markup=InlineKeyboardMarkup(kb))

async def list_admins(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    async for s in get_session():
        admins = (await s.execute(select(Administrator))).scalars().all()
    txt = "\n".join(f"{a.username} ({a.role})" for a in admins)
    await update.callback_query.edit_message_text("👥 Администраторы:\n"+txt, reply_markup=InlineKeyboardMarkup([[back_button()]]))

async def gen_code(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    code = uuid.uuid4().hex[:8]
    async for s in get_session():
        s.add(RegCode(code=code, role="admin")); await s.commit()
    await update.callback_query.edit_message_text(
        f"🗝 Код регистрации: `{code}`", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[back_button()]])
    )

# — Registration —
async def reg_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    parts = update.message.text.split()
    if len(parts) != 2:
        return await update.message.reply_text("❌ Используйте /reg <код>")
    code = parts[1]
    async for s in get_session():
        rc = await s.get(RegCode, code)
        if not rc:
            return await update.message.reply_text("❌ Неверный код.")
        ctx.user_data["reg_role"] = rc.role
        await s.delete(rc); await s.commit()
    await update.message.reply_text("🔑 Введите логин нового админа:", reply_markup=InlineKeyboardMarkup([[back_button()]]))
    return REG_NAME

async def reg_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    if txt.lower() == "назад":
        return await show_home(update, ctx)
    ctx.user_data["reg_login"] = txt
    await update.message.reply_text("💻 Введите пароль нового админа:", reply_markup=InlineKeyboardMarkup([[back_button()]]))
    return REG_PASS

async def reg_pass(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    pwd = update.message.text.strip()
    if pwd.lower() == "назад":
        return await show_home(update, ctx)
    hashed = bcrypt.hash(pwd)
    async for s in get_session():
        s.add(Administrator(
            username=ctx.user_data["reg_login"],
            password=hashed,
            role=ctx.user_data["reg_role"]
        ))
        await s.commit()
    await update.message.reply_text("✅ Новый админ создан.")
    return await show_home(update, ctx)

# — Change login —
async def change_login_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        "✏️ Введите новый логин:", reply_markup=InlineKeyboardMarkup([[back_button("⬅️ Назад","settings")]])
    )
    return CHL_NEW

async def change_login_new(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    new = update.message.text.strip()
    if new.lower() == "назад":
        return await settings_cb(update, ctx)
    async for s in get_session():
        exists = (await s.execute(select(Administrator).where(Administrator.username==new))).scalar_one_or_none()
        if exists:
            return await update.message.reply_text("❌ Логин уже занят.")
    ctx.user_data["new_login"] = new
    await update.message.reply_text(
        f"Подтвердить логин `{new}`?", parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да", callback_data="confirm_login")],
            [back_button("❌ Отмена","settings")]
        ])
    )

async def confirm_login(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    new = ctx.user_data["new_login"]
    async for s in get_session():
        await s.execute(update(Administrator).where(Administrator.id==ctx.user_data["admin_id"]).values(username=new))
        await s.commit()
    await update.callback_query.edit_message_text("✅ Логин изменён.")
    return await settings_cb(update, ctx)

# — Change password —
async def change_pass_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        "🔒 Введите старый пароль:", reply_markup=InlineKeyboardMarkup([[back_button("⬅️ Назад","settings")]])
    )
    return CHP_OLD

async def change_pass_old(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    old = update.message.text.strip()
    if old.lower() == "назад":
        return await settings_cb(update, ctx)
    async for s in get_session():
        admin = await s.get(Administrator, ctx.user_data["admin_id"])
        if not bcrypt.verify(old, admin.password):
            return await update.message.reply_text("❌ Неверный пароль.")
    await update.message.reply_text(
        "🔒 Введите новый пароль:", reply_markup=InlineKeyboardMarkup([[back_button("⬅️ Назад","settings")]])
    )
    return CHP_NEW

async def change_pass_new(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    new = update.message.text.strip()
    if new.lower() == "назад":
        return await settings_cb(update, ctx)
    ctx.user_data["new_pass"] = new
    await update.message.reply_text(
        "Подтвердить новый пароль?", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да", callback_data="confirm_pass")],
            [back_button("❌ Отмена","settings")]
        ])
    )

async def confirm_pass(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    new = ctx.user_data["new_pass"]
    hashed = bcrypt.hash(new)
    async for s in get_session():
        await s.execute(update(Administrator).where(Administrator.id==ctx.user_data["admin_id"]).values(password=hashed))
        await s.commit()
    await update.callback_query.edit_message_text("✅ Пароль изменён.")
    return await settings_cb(update, ctx)

# — Rounds menu & creation (в контексте турнира!) —
async def round_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    tid = ctx.user_data.get("tid")
    if not tid:
        return await update.callback_query.edit_message_text("❌ Сначала выберите турнир.")
    await update.callback_query.edit_message_text(
        "🔄 Создать раунд:", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Простой ▶️", callback_data="round_simple")],
            [InlineKeyboardButton("Итоговый ▶️", callback_data="round_final")],
            [back_button()]
        ])
    )

async def round_simple(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    tid = ctx.user_data["tid"]
    async for s in get_session():
        players = (await s.execute(
            select(Player).where(Player.tournament_id==tid).order_by(Player.score.desc())
        )).scalars().all()
        if len(players) <= 8:
            t1, t2 = players[0::2], players[1::2]
            tables = [t1, t2]
        else:
            half = len(players)//2
            tables = [players[:half], players[half:]]
        rnd = Round(tournament_id=tid, round_type="simple",
                    data={"tables":[[p.id for p in tbl] for tbl in tables]})
        s.add(rnd); await s.flush()
        for idx, tbl in enumerate(tables, start=1):
            for p1,p2 in combinations(tbl,2):
                s.add(Match(round_id=rnd.id, table_number=idx, player1_id=p1.id, player2_id=p2.id))
        await s.commit()
    await update.callback_query.edit_message_text("✅ Простой раунд создан.")
    return await show_tournament(update, ctx)

async def round_final(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if require_login(update, ctx): return
    await update.callback_query.answer()
    tid = ctx.user_data["tid"]
    async for s in get_session():
        simple = (await s.execute(
            select(Round).where(Round.tournament_id==tid, Round.round_type=="simple")
        )).scalar_one_or_none()
        if not simple:
            return await update.callback_query.edit_message_text("❌ Завершите простой раунд.")
        players = (await s.execute(
            select(Player).where(Player.tournament_id==tid).order_by(Player.score.desc())
        )).scalars().all()
        if len(players) != 8:
            return await update.callback_query.edit_message_text("❌ Итоговый только для 8 игроков.")
        p1,p2,p3,p4 = players[:4]
        q1,q2,q3,q4 = players[4:]
        bracket = [(p1.id,q4.id),(p2.id,q3.id),(q1.id,p4.id),(q2.id,p3.id)]
        rnd = Round(tournament_id=tid, round_type="final", data={"matches": bracket})
        s.add(rnd); await s.flush()
        for idx,(a,b) in enumerate(bracket, start=1):
            s.add(Match(round_id=rnd.id, table_number=idx, player1_id=a, player2_id=b))
        await s.commit()
    await update.callback_query.edit_message_text("✅ Итоговый раунд создан.")
    return await show_tournament(update, ctx)

# — Main entrypoint —
def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_db())

    app = ApplicationBuilder().token(TOKEN).build()

    # Auth conv
    auth_conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CallbackQueryHandler(auth_start, pattern="^auth_start$")
        ],
        states={
            AUTH_LOGIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, auth_login)],
            AUTH_PASS:  [MessageHandler(filters.TEXT & ~filters.COMMAND, auth_pass)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    app.add_handler(auth_conv)

    # /reg conv
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("reg", reg_start)],
        states={
            REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            REG_PASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_pass)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    app.add_handler(reg_conv)

    # Change login conv
    chlogin_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(change_login_start, pattern="^change_login$")],
        states={CHL_NEW: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_login_new)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    app.add_handler(chlogin_conv)
    app.add_handler(CallbackQueryHandler(confirm_login, pattern="^confirm_login$"))

    # Change pass conv
    chpass_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(change_pass_start, pattern="^change_pass$")],
        states={
            CHP_OLD: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_pass_old)],
            CHP_NEW: [MessageHandler(filters.TEXT & ~filters.COMMAND, change_pass_new)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    app.add_handler(chpass_conv)
    app.add_handler(CallbackQueryHandler(confirm_pass, pattern="^confirm_pass$"))

    # Create tournament conv
    ct_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(ct_start, pattern="^ct_start$")],
        states={
            CT_NAME:    [MessageHandler(filters.TEXT & ~filters.COMMAND, ct_name)],
            CT_TYPE:    [CallbackQueryHandler(ct_type, pattern="^(Beginner|Advanced)$")],
            CT_TABLES:  [MessageHandler(filters.TEXT & ~filters.COMMAND, ct_tables)],
            CT_PLAYERS: [MessageHandler(filters.TEXT & ~filters.COMMAND, ct_players)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )
    app.add_handler(ct_conv)

    # Navigation
    app.add_handler(CommandHandler("home", show_home))
    app.add_handler(CommandHandler("back", show_home))
    app.add_handler(CallbackQueryHandler(show_home, pattern="^home$"))
    app.add_handler(CallbackQueryHandler(history_cb, pattern="^hist$"))
    app.add_handler(CallbackQueryHandler(active_cb, pattern="^act$"))
    app.add_handler(CallbackQueryHandler(show_tournament, pattern="^show_\\d+$"))
    app.add_handler(CallbackQueryHandler(export_json, pattern="^exp_json$"))

    # Settings
    app.add_handler(CallbackQueryHandler(settings_cb, pattern="^settings$"))
    app.add_handler(CallbackQueryHandler(list_admins, pattern="^list_admins$"))
    app.add_handler(CallbackQueryHandler(gen_code, pattern="^gen_code$"))

    # Rounds
    app.add_handler(CallbackQueryHandler(round_menu, pattern="^round_menu$"))
    app.add_handler(CallbackQueryHandler(round_simple, pattern="^round_simple$"))
    app.add_handler(CallbackQueryHandler(round_final, pattern="^round_final$"))

    logger.info("🚀 Bot started")
    app.run_polling()

if __name__ == "__main__":
    main()