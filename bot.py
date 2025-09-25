# pip install python-telegram-bot==20.6
import json, os
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo, ForceReply
from urllib.parse import urlencode, quote_plus
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import datetime, calendar
import asyncio
from telegram.error import Forbidden, BadRequest, TimedOut

TOKEN = "8413119831:AAHBTOc50t2jshcVTEXUMoNSWABw80VT408"

# Список блоков (как прислал)
BLOCKS = [
    "IOPL", "LSE", "TPU Logger", "xBM", "NCCM", "CMF", "RM", "HDMA",
    "STM", "CSS", "CIM", "CMM", "FIM", "WLD", "LLHCMQ", "LLHMCTPQ",
    "QM", "ATSM"
]

# Файлы-хранилища (простая персистентность)
VOTES_PATH = "vp4_votes.json"     # {"<user_id>": {"<block>": score}}
POSTS_PATH = "vp4_posts.json"     # {"<chat_id>": {"main": {"message_id": int}}}
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://aivanou86.github.io/tg_bot/index.html")  # Укажите ваш HTTPS URL
WEBAPP_ENABLED = "1"  # Включать WebApp-кнопки только после /setdomain
STATUS_KEY_PREFIX = "status:"

# Константы фаз и причин задержки
PHASES = ["VP1", "VP2", "VP3", "VP4", "VP5"]
REASONS = [
    ("mas_doc", "Incomplete or missing MAS documentation"),
    ("spec_not_final", "Specifications not finalized or lack details about new features."),
    ("design_slow", "Slow response from design team"),
    ("design_wait", "Long waiting time for answers on JIRA/e-mail/messenger."),
    ("high_complex", "High block complexity"),
    ("tb_complex", "Testbench complexity"),
    ("immature_rtl", "Immature RTL design"),
    ("late_features", "Late introduction of new features"),
    ("regression_unstable", "Regression instability"),
    ("coverage_gaps", "Coverage gaps"),
    ("xblock_deps", "Cross-block dependencies"),
    ("resource_constraints", "Resource constraints"),
    ("other", "✍️"),
]

def _load(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default

def _save(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _get_query_chat_id(query):
    try:
        return int(getattr(query, "_chat_id"))
    except Exception:
        pass
    try:
        return int(query.message.chat.id)
    except Exception:
        return None

def _get_thread_id_from_query(query):
    try:
        return getattr(query.message, "message_thread_id", None)
    except Exception:
        return None

def get_votes(ctx):
    if "votes" not in ctx.bot_data:
        ctx.bot_data["votes"] = _load(VOTES_PATH, {})
    return ctx.bot_data["votes"]

def get_posts(ctx):
    if "posts" not in ctx.bot_data:
        ctx.bot_data["posts"] = _load(POSTS_PATH, {})
    return ctx.bot_data["posts"]

def get_status_from_votes(ctx, chat_id_str: str):
    votes = get_votes(ctx)
    key = STATUS_KEY_PREFIX + chat_id_str
    if key not in votes or not isinstance(votes[key], dict):
        votes[key] = {}
    return votes[key]

def get_latest_per_chat_from_votes(ctx, chat_id_str: str):
    # legacy: не используется (история latest удалена)
    return {}

def set_latest_vote(ctx, chat_id_str: str, block: str, score: int, user_id: int):
    # legacy: не используется (история latest удалена)
    return

def save_all(ctx):
    # Всегда подгружаем текущее состояние с диска, чтобы не перезаписать пустым
    votes = get_votes(ctx)
    posts = get_posts(ctx)
    _save(VOTES_PATH, votes)
    _save(POSTS_PATH, posts)

def _ensure_block_state(state_by_chat, chat_id: str, block: str):
    # legacy helper: не используется
    state_by_chat.setdefault(chat_id, {})
    state_by_chat[chat_id].setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
    return state_by_chat[chat_id][block]

def _format_status_summary(state):
    phase = state.get("phase") or "—"
    progress = state.get("progress")
    progress_str = f"{progress}%" if isinstance(progress, int) else "—"
    eta = state.get("eta") or "—"
    return f"{phase} {progress_str} • {eta}"

def _format_reasons_summary(state, max_items: int = 2):
    keys = state.get("reasons") or []
    if not keys:
        return "—"
    titles = []
    for key, title in REASONS:
        if key in keys:
            titles.append(title)
    if not titles:
        return "—"
    if len(titles) <= max_items:
        return ", ".join(titles)
    return ", ".join(titles[:max_items]) + f" …(+{len(titles)-max_items})"

def _md_escape(text: str) -> str:
    if text is None:
        return ""
    # Экранируем минимально необходимые символы для Markdown
    text = text.replace("\\", "\\\\")
    for ch in ("*", "_", "`"):
        text = text.replace(ch, "\\" + ch)
    return text

def _format_comment_summary(state, max_len: int = 80) -> str:
    comment = (state.get("comment") or "").strip()
    if not comment:
        return "—"
    one_line = " ".join(comment.split())
    esc = _md_escape(one_line)
    if len(esc) > max_len:
        esc = esc[: max_len - 1] + "…"
    return f"💬 {esc}"

def _format_reasons_with_comment(state) -> str:
    reasons = _format_reasons_summary(state)
    comment = _format_comment_summary(state)
    if comment != "—":
        if reasons == "—":
            return comment
        return f"{reasons} • {comment}"
    return reasons

def _format_block_button_text(block: str, risk: int | None, arrow: str, phase: str | None = None, progress: int | None = None) -> str:
    # Компактная строка: минимальный отступ между названием и оценкой
    block_padded = (block or "")
    phase_part = ""
    if isinstance(progress, int) or (phase and phase != "—"):
        prog_str = f":{progress}%" if isinstance(progress, int) else ""
        phase_val = phase or "—"
        phase_part = f" | {phase_val}{prog_str}"
    if isinstance(risk, int):
        color, _ = get_risk_info(risk)
        return f"{color} {arrow} {block_padded} | {risk}/10{phase_part}"
    return f"⚪ {arrow} {block_padded} | —{phase_part}"

def _reason_titles_list(state) -> list:
    # Возвращает только названия причин (без 'other' и без комментария)
    keys = state.get("reasons") or []
    titles_map = {k: t for k, t in REASONS}
    titles = []
    for k in keys:
        if k == "other":
            continue
        t = titles_map.get(k)
        if isinstance(t, str):
            titles.append(t)
    return titles

def _get_other_comment(state):
    # Показываем комментарий только если выбрана причина 'other'
    reasons = state.get("reasons") or []
    if "other" not in reasons:
        return None
    text = (state.get("comment") or "").strip()
    return text if text else None

def make_keyboard_compact(block: str, current_vote=None) -> InlineKeyboardMarkup:
    # legacy: не используется
    return InlineKeyboardMarkup([])

def get_risk_info(score):
    """Возвращает цвет и описание риска"""
    risk_levels = {
        1: ("🟢", "все шикарно"),
        2: ("🟢", "отличное состояние"),
        3: ("🟡", "небольшие риски"),
        4: ("🟡", "умеренный риск"),
        5: ("🟠", "средний риск"),
        6: ("🟠", "заметный риск"),
        7: ("🔴", "высокий риск"),
        8: ("🔴", "очень высокий риск"),
        9: ("🔴", "критический риск"),
        10: ("🔴", "огромный высокий риск")
    }
    return risk_levels.get(score, ("⚪", "неизвестно"))

def block_text_compact(block: str, votes) -> str:
    # legacy: не используется, т.к. теперь берём только последний статус
    return f"{block}"

async def is_admin(update, context):
    chat = update.effective_chat
    user = update.effective_user
    try:
        admins = await context.bot.get_chat_administrators(chat.id)
    except Forbidden:
        return False
    return user.id in {a.user.id for a in admins}

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Привет! Я бот для оценки рисков VP4.\n"
        "Команды:\n"
        "• /survey — опубликовать голосование по всем блокам (только админы)\n"
        "• /results — общая сводка по голосам\n\n"
        "• /reset — сбросить все голоса (только админы)\n\n"
        "Голосовать может любой участник. Можно переголосовать — засчитается последнее значение."
    )

    # Обработка deep-link payload: /start block=<BLOCK>&chat_id=<GROUP_ID>
    try:
        payload = (update.message.text or "").split(" ", 1)
        payload = payload[1] if len(payload) > 1 else ""
        if payload:
            # Простая разборка пары ключ=значение через &
            params = {}
            for part in payload.split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params[k] = v
            block = params.get("block")
            group_chat_id = params.get("chat_id")
            if block in BLOCKS and group_chat_id:
                context.user_data["awaiting"] = {"type": "comment", "chat_id": int(group_chat_id), "block": block}
                await update.message.reply_text(
                    f"Откройте комментарий к блоку {block}. Отправьте текст — он будет сохранён для группы {group_chat_id}."
                )
                return
    except Exception:
        pass

    await update.message.reply_text(text)

async def cmd_survey(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Только админ в группе — чтобы не спамили
    if update.effective_chat.type in ("group", "supergroup"):
        if not await is_admin(update, context):
            await update.message.reply_text("⛔ Только администраторы могут запускать /survey в группе.")
            return

    posts = get_posts(context)
    votes = get_votes(context)
    chat_id = str(update.effective_chat.id)
    posts.setdefault(chat_id, {})

    # Создаем/обновляем одно единственное сообщение-опрос в этом чате
    lines = ["🎯 *Оценка рисков VP4*\n"]

    chat_id_str = str(update.effective_chat.id)
    status_all = get_status_from_votes(context, chat_id_str)
    posts = get_posts(context)
    chat_posts = posts.setdefault(chat_id_str, {})
    header = chat_posts.get("header")
    if header:
        lines.append(header)
    else:
        lines.append("Оцените вероятность не уложиться в срок (1-10):")
        lines.append("1 все шикарно • 10 огромный риск\n")
    expanded = set(chat_posts.get("expanded", []))
    
    # Текст таблицы не содержит строк по блокам — вся таблица рендерится как кнопки
    
    lines.append("\n_Тап по кнопке блока: ▸/▾ — развернуть, ⚙ — настройки_")

    # Клавиатура: построчно по блоку — [▸/▾ block]; при развороте добавляем ряд действий
    keyboard = []
    for block in BLOCKS:
        is_expanded = block in expanded
        arrow = "▾" if is_expanded else "▸"
        bs = status_all.get(block, {}) if isinstance(status_all, dict) else {}
        risk = bs.get("risk") if isinstance(bs, dict) else None
        # Малая кнопка настроек + широкая кнопка блока (с фазой/процентом прямо в строке)
        phase_val = (bs.get("phase") or "—") if isinstance(bs, dict) else None
        prog_val = bs.get("progress") if isinstance(bs, dict) else None
        left_btn = InlineKeyboardButton("⚙", callback_data=f"vote:{block}")
        right_btn = InlineKeyboardButton(_format_block_button_text(block, risk, arrow, phase_val, prog_val), callback_data=f"toggle:{block}")
        keyboard.append([left_btn, right_btn])
        if is_expanded:
            phase = bs.get("phase") or "—"
            prog = bs.get("progress")
            prog_str = f"{prog}%" if isinstance(prog, int) else "—"
            eta = bs.get("eta") or "—"
            # В развороте показываем только ETA (фаза/проценты уже в основной кнопке)
            info_text = f"ETA {eta}"
            keyboard.append([
                InlineKeyboardButton("VP4 ETA", callback_data="noop"),
                InlineKeyboardButton(info_text, callback_data="noop")
            ])
            # Разворачиваем причины построчно; первая строка с меткой, остальные — без левой кнопки
            reason_titles = _reason_titles_list(bs)
            other_comment = _get_other_comment(bs)
            if reason_titles:
                for idx, title in enumerate(reason_titles):
                    if idx == 0:
                        keyboard.append([
                            InlineKeyboardButton("Причины", callback_data=f"show:reasons:{block}"),
                            InlineKeyboardButton(title, callback_data=f"show:reasons:{block}")
                        ])
                    else:
                        keyboard.append([
                            InlineKeyboardButton(" ", callback_data=f"show:reasons:{block}"),
                            InlineKeyboardButton(title, callback_data=f"show:reasons:{block}")
                        ])
            # 'Иное' (Others) — отдельной строкой, если есть текст комментария (без активного callback)
            if other_comment:
                keyboard.append([
                    InlineKeyboardButton("Others", callback_data="noop"),
                    InlineKeyboardButton(other_comment, callback_data="noop")
                ])

    main_post = posts[chat_id].get("main")
    text = "\n".join(lines)
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Если основное сообщение уже существует — обновим его, без создания новых
    if main_post and isinstance(main_post, dict) and "message_id" in main_post:
        try:
            await context.bot.edit_message_text(
                chat_id=int(chat_id),
                message_id=main_post["message_id"],
                text=text,
                parse_mode="Markdown",
                reply_markup=reply_markup
            )
        except Exception:
            # Если не получилось (удалено/нет доступа) — создаем заново
            msg = await update.message.reply_text(
                text,
                parse_mode="Markdown",
                reply_markup=reply_markup
            )
            posts[chat_id]["main"] = {"message_id": msg.message_id}
            save_all(context)
    else:
        # Еще не создавали — создадим
        msg = await update.message.reply_text(
            text,
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
        posts[chat_id]["main"] = {"message_id": msg.message_id}
        save_all(context)

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user

    # Разбор callback
    if query.data.startswith("toggle:"):
        # Развернуть/свернуть подробности блока в основной таблице
        block = query.data.split(":", 1)[1]
        chat_id_str = str(update.effective_chat.id)
        posts = get_posts(context)
        chat_posts = posts.setdefault(chat_id_str, {})
        expanded = set(chat_posts.get("expanded", []))
        if block in expanded:
            expanded.remove(block)
        else:
            expanded.add(block)
        chat_posts["expanded"] = list(expanded)
        # Сохраняем только posts, не трогая votes
        _save(POSTS_PATH, posts)
        await update_main_survey(query, context)
        return
    
    if query.data.startswith("vote:"):
        # Пользователь выбрал блок для голосования/управления — меняем разметку в том же сообщении
        block = query.data[5:]  # убираем "vote:"
        if block not in BLOCKS:
            await query.answer("Неверный блок.", show_alert=True)
            return

        votes = get_votes(context)
        user_vote = votes.get(str(user.id), {}).get(block)

        # Вариант WebApp: единая форма управления блоком (включаем только если разрешено)
        chat_id_str = str(update.effective_chat.id)
        params = f"block={quote_plus(block)}&chat_id={chat_id_str}"
        webapp_row = [InlineKeyboardButton("🧰 Открыть форму (WebApp)", web_app=WebAppInfo(url=f"{WEBAPP_URL}?{params}"))] if WEBAPP_ENABLED else None
        # Сохраним старые разделённые пункты как альтернативу
        rate_row = [InlineKeyboardButton("🎯 Оценка рисков", callback_data=f"rate:{block}")]
        status_row = [InlineKeyboardButton("📊 Текущий статус", callback_data=f"status:{block}")]
        reasons_row = [InlineKeyboardButton("⏱ Причины рисков", callback_data=f"reasons:{block}:0")]
        eta_row = [InlineKeyboardButton("🗓 ETA", callback_data=f"eta:{block}")]
        back_btn = [InlineKeyboardButton("⬅️ Назад", callback_data="back:main")]
        keyboard = []
        if webapp_row:
            keyboard.append(webapp_row)
        keyboard += [rate_row, status_row, reasons_row, eta_row, back_btn]

        # Подпишем меню: меняем текст сообщения на заголовок настроек и показываем клавиатуру действий
        menu_text = f"⚙️ Настройки: `{block}`\nВыберите действие:"
        try:
            await query.edit_message_text(menu_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        except BadRequest as e:
            if "Button_type_invalid" in str(e) and WEBAPP_ENABLED:
                keyboard_no_webapp = []
                for row in keyboard:
                    if not row:
                        continue
                    btn = row[0]
                    if isinstance(btn, InlineKeyboardButton) and getattr(btn, "web_app", None):
                        continue
                    keyboard_no_webapp.append(row)
                await query.edit_message_text(menu_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard_no_webapp))
            else:
                raise
        except TimedOut:
            # Повторим один раз с паузой и увеличенными таймаутами
            await asyncio.sleep(1.0)
            await query.edit_message_text(menu_text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        if user_vote:
            await query.answer(f"Текущая: {user_vote}")
        else:
            await query.answer("Откройте меню оценки")
        return
    elif query.data.startswith("rate:"):
        # Показать кнопки 1-10 для оценки (каждая на своей строке с описанием)
        block = query.data.split(":", 1)[1]
        if block not in BLOCKS:
            await query.answer("Неверный блок.", show_alert=True)
            return
        rows = []
        for i in range(1, 11):
            _, desc = get_risk_info(i)
            text = f"{i} ({desc})"
            rows.append([InlineKeyboardButton(text, callback_data=f"{block}:{i}")])
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"vote:{block}")])
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(rows))
        await query.answer("Выберите оценку")
        return
        
    elif query.data.startswith("back:"):
        # Возврат к главному меню — обновляем всё в этом же сообщении
        await update_main_survey(query, context)
        return
    
    elif query.data.startswith("status:"):
        block = query.data.split(":", 1)[1]
        if update.effective_chat.type in ("group", "supergroup") and not await is_admin(update, context):
            await query.answer("Только админ может менять статус.", show_alert=True)
            return
        # Показать только кнопки фаз VP1..VP5 (вертикально), затем проценты
        context.user_data["status_only_block"] = block
        await open_status_menu_only_phase(query, context, block, show_progress=False)
        return

    elif query.data.startswith("phase:"):
        _b, phase = query.data.split(":", 1)[1].split("|", 1)
        block = _b
        chat_id_str = str(update.effective_chat.id)
        status_all = get_status_from_votes(context, chat_id_str)
        st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
        if phase in PHASES:
            st["phase"] = phase
            save_all(context)
        # Если пользователь пришёл из меню статуса, после выбора фазы показываем проценты
        if context.user_data.get("status_only_block") == block:
            await open_status_menu_only_phase(query, context, block, show_progress=True)
        else:
            await open_status_menu(query, context, block)
        return

    elif query.data.startswith("progress:"):
        # progress:<block>:<val or txt>
        parts = query.data.split(":")
        block = parts[1]
        action = parts[2] if len(parts) > 2 else ""
        if action == "txt":
            # Просим процент ответом на сообщение (ForceReply), как для ETA/Иного
            prompt = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"Введите процент прогресса для {block} (0-100) и отправьте ответом на это сообщение.",
                reply_markup=ForceReply(input_field_placeholder="0..100", selective=True)
            )
            context.user_data["awaiting"] = {"type": "progress", "chat_id": update.effective_chat.id, "block": block, "reply_to": prompt.message_id, "user_id": query.from_user.id}
        else:
            try:
                val = int(action)
                if 0 <= val <= 100:
                    chat_id_str = str(update.effective_chat.id)
                    status_all = get_status_from_votes(context, chat_id_str)
                    st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
                    st["progress"] = val
                    save_all(context)
            except Exception:
                pass
        # Возвращаемся в основное меню с таблицей
        await update_main_survey(query, context)
        return

    elif query.data.startswith("reasons:"):
        # Открываем меню причин (как раньше)
        _, block, page_str = query.data.split(":", 2)
        page = int(page_str)
        if update.effective_chat.type in ("group", "supergroup") and not await is_admin(update, context):
            await query.answer("Только админ может менять причины.", show_alert=True)
            return
        await open_reasons_menu(query, context, block, page)
        return

    elif query.data.startswith("reason_toggle:"):
        # reason_toggle:<block>:<key>:<page>
        _, block, key, page_str = query.data.split(":", 3)
        chat_id_str = str(update.effective_chat.id)
        status_all = get_status_from_votes(context, chat_id_str)
        st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
        st.setdefault("reasons", [])
        if key in st["reasons"]:
            st["reasons"].remove(key)
        else:
            st["reasons"].append(key)
            # Если включили 'other' — сразу запросим комментарий реплаем
            if key == "other":
                prompt = await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"Опишите 'Иное' для {block} ответом на это сообщение.",
                    reply_markup=ForceReply(input_field_placeholder="Ваш комментарий…", selective=True)
                )
                context.user_data["awaiting"] = {"type": "comment", "chat_id": update.effective_chat.id, "block": block, "reply_to": prompt.message_id, "user_id": query.from_user.id}
        save_all(context)
        await open_reasons_menu(query, context, block, int(page_str))
        return

    elif query.data.startswith("eta:"):
        block = query.data.split(":", 1)[1]
        if update.effective_chat.type in ("group", "supergroup") and not await is_admin(update, context):
            await query.answer("Только админ может менять ETA.", show_alert=True)
            return
        today = datetime.date.today()
        await open_calendar(query, context, block, today.year, today.month)
        return

    elif query.data.startswith("caleta:"):
        # caleta:<block>:YYYY:MM — открыть календарь на месяце
        _, block, y, m = query.data.split(":", 3)
        await open_calendar(query, context, block, int(y), int(m))
        return

    elif query.data.startswith("caltype:"):
        # Переключиться на ручной ввод даты
        block = query.data.split(":", 1)[1]
        # Как и для комментария — просим ответом на сообщение бота (ForceReply)
        prompt = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"Введите дату ETA для {block} в формате YYYY-MM-DD и отправьте ответом на это сообщение.",
            reply_markup=ForceReply(input_field_placeholder="YYYY-MM-DD", selective=True)
        )
        context.user_data["awaiting"] = {"type": "eta", "chat_id": update.effective_chat.id, "block": block, "reply_to": prompt.message_id, "user_id": query.from_user.id}
        return

    elif query.data.startswith("calpick:"):
        # calpick:<block>:YYYY-MM-DD — выбран день
        _, block, ds = query.data.split(":", 2)
        chat_id_str = str(update.effective_chat.id)
        status_all = get_status_from_votes(context, chat_id_str)
        st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
        st["eta"] = ds
        save_all(context)
        # Покажем главную таблицу в том же сообщении
        await update_main_survey(query, context)
        return

    elif query.data.startswith("comment:"):
        block = query.data.split(":", 1)[1]
        if update.effective_chat.type in ("group", "supergroup") and not await is_admin(update, context):
            await query.answer("Только админ может менять комментарий.", show_alert=True)
            return
        # Всегда просим ответом на сообщение бота (ForceReply), без всплывающих алертов
        prompt = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"Напишите комментарий для {block} и отправьте ответом на это сообщение.",
            reply_markup=ForceReply(input_field_placeholder="Комментарий…", selective=True)
        )
        context.user_data["awaiting"] = {"type": "comment", "chat_id": update.effective_chat.id, "block": block, "reply_to": prompt.message_id, "user_id": query.from_user.id}
        return

    elif query.data.startswith("show:" ):
        # show:status:<block> или show:reasons:<block>
        _, kind, block = query.data.split(":", 2)
        chat_id_str = str(update.effective_chat.id)
        status_all = get_status_from_votes(context, chat_id_str)
        bs = status_all.get(block, {}) if isinstance(status_all, dict) else {}
        if kind == "status":
            phase = bs.get("phase") or "—"
            prog = bs.get("progress")
            prog_str = f"{prog}%" if isinstance(prog, int) else "—"
            eta = bs.get("eta") or "—"
            header = f"📊 Статус: {phase} {prog_str} • ETA {eta}"
        else:
            titles = _reason_titles_list(bs)
            header = "⏱ Причины: " + (", ".join(titles) if titles else "—")
        # Сохраняем заголовок в posts и перерисовываем сообщение через update_main_survey
        posts = get_posts(context)
        posts.setdefault(chat_id_str, {})["header"] = header
        _save(POSTS_PATH, posts)
        await update_main_survey(query, context)
        return

    elif ":" in query.data:
        # Пользователь поставил оценку
        try:
            block, score_str = query.data.split(":")
            score = int(score_str)
            assert block in BLOCKS and 1 <= score <= 10
        except Exception:
            await query.answer("Неверный формат ответа.", show_alert=True)
            return

        # Обновляем текущую оценку риска в status:<chat_id>
        chat_key = str(_get_query_chat_id(query) or update.effective_chat.id)
        status_map = get_status_from_votes(context, chat_key)
        bs = status_map.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None, "risk": None, "updated_by": None})
        bs["risk"] = score
        bs["updated_by"] = int(user.id)
        save_all(context)

        await query.answer(f"✅ {block}: {score}/10")

        # После оценки возвращаем основное меню (то же сообщение), сохранив правильный chat_id
        q_chat_id = _get_query_chat_id(query)
        if q_chat_id is not None:
            try:
                class DummyQuery:
                    def __init__(self, chat_id, message_id):
                        self._chat_id = chat_id
                        self._message_id = message_id
                    async def edit_message_text(self, text, parse_mode=None, reply_markup=None):
                        await context.bot.edit_message_text(chat_id=self._chat_id, message_id=self._message_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
                # Ищем основное сообщение этого чата
                posts = get_posts(context)
                main_post = posts.get(str(q_chat_id), {}).get("main")
                if main_post and isinstance(main_post, dict) and "message_id" in main_post:
                    dummy_query = DummyQuery(q_chat_id, main_post["message_id"])
                    await update_main_survey(dummy_query, context)
                else:
                    await update_main_survey(query, context)
            except Exception:
                await update_main_survey(query, context)
        else:
            await update_main_survey(query, context)
        return

async def update_main_survey(query, context):
    """Обновляет главное сообщение с таблицей"""
    votes = get_votes(context)
    chat_id_str = str(query._chat_id) if hasattr(query, "_chat_id") else str(query.message.chat_id)
    status_all = get_status_from_votes(context, chat_id_str)
    
    lines = ["🎯 *Оценка рисков VP4*\n"]
    lines.append("Оцените вероятность не уложиться в срок (1-10):")
    lines.append("1 все шикарно • 10 огромный риск\n")
    
    # Состояние разворота строк
    posts = get_posts(context)
    chat_posts = posts.setdefault(chat_id_str, {})
    expanded = set(chat_posts.get("expanded", []))

    # Таблица полностью рендерится кнопками ниже
    lines.append("\n_Тап по кнопке блока: ▸/▾ — развернуть, ⚙ — настройки_")
    
    # Клавиатура-таблица: слева ⚙, справа кнопка блока с риском и фазой/прогрессом; при развороте — ETA и причины
    keyboard = []
    for block in BLOCKS:
        is_expanded = block in expanded
        arrow = "▾" if is_expanded else "▸"
        bs = status_all.get(block, {}) if isinstance(status_all, dict) else {}
        risk = bs.get("risk") if isinstance(bs, dict) else None
        phase_val = (bs.get("phase") or "—") if isinstance(bs, dict) else None
        prog_val = bs.get("progress") if isinstance(bs, dict) else None
        left_btn = InlineKeyboardButton("⚙", callback_data=f"vote:{block}")
        right_btn = InlineKeyboardButton(_format_block_button_text(block, risk, arrow, phase_val, prog_val), callback_data=f"toggle:{block}")
        keyboard.append([left_btn, right_btn])
        if is_expanded:
            phase = bs.get("phase") or "—"
            prog = bs.get("progress")
            prog_str = f"{prog}%" if isinstance(prog, int) else "—"
            eta = bs.get("eta") or "—"
            info_text = f"ETA {eta}"
            keyboard.append([
                InlineKeyboardButton("VP4 ETA", callback_data="noop"),
                InlineKeyboardButton(info_text, callback_data="noop")
            ])
            # Причины построчно: первая с меткой, остальные с пустой левой кнопкой
            reason_titles = _reason_titles_list(bs)
            other_comment = _get_other_comment(bs)
            if reason_titles:
                for idx, title in enumerate(reason_titles):
                    if idx == 0:
                        keyboard.append([
                            InlineKeyboardButton("Причины", callback_data="noop"),
                            InlineKeyboardButton(title, callback_data="noop")
                        ])
                    else:
                        keyboard.append([
                            InlineKeyboardButton(" ", callback_data="noop"),
                            InlineKeyboardButton(title, callback_data="noop")
                        ])
            if other_comment:
                keyboard.append([
                    InlineKeyboardButton("Others", callback_data="noop"),
                    InlineKeyboardButton(other_comment, callback_data="noop")
                ])
    
    await query.edit_message_text(
        "\n".join(lines), 
        parse_mode="Markdown", 
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def open_status_menu(query, context, block: str):
    chat_id_str = str(query.message.chat_id) if hasattr(query, "message") and query.message else str(query._chat_id)
    status_all = get_status_from_votes(context, chat_id_str)
    st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
    phase = st.get("phase") or "—"
    progress = st.get("progress")
    progress_str = f"{progress}%" if isinstance(progress, int) else "—"
    eta = st.get("eta") or "—"
    comment = st.get("comment") or "—"

    text_lines = [f"⚙️ *{block}* — управление статусом\n"]
    text_lines.append(f"Фаза: `{phase}` • Прогресс: `{progress_str}` • ETA: `{eta}`")
    text_lines.append(f"Комментарий: {comment}\n")

    # Ряд кнопок фаз VP1..VP5 (отдельная строка)
    phase_buttons = [InlineKeyboardButton(p, callback_data=f"phase:{block}|{p}") for p in PHASES]
    # Кнопки быстрой установки прогресса и ввода своего варианта
    prog_buttons = [
        InlineKeyboardButton("0%", callback_data=f"progress:{block}:0"),
        InlineKeyboardButton("25%", callback_data=f"progress:{block}:25"),
        InlineKeyboardButton("50%", callback_data=f"progress:{block}:50"),
        InlineKeyboardButton("75%", callback_data=f"progress:{block}:75"),
        InlineKeyboardButton("100%", callback_data=f"progress:{block}:100"),
        InlineKeyboardButton("✍️", callback_data=f"progress:{block}:txt"),
    ]
    # Нижние кнопки
    bottom = [
        InlineKeyboardButton("⏱ Причины", callback_data=f"reasons:{block}:0"),
        InlineKeyboardButton("🗓 ETA", callback_data=f"eta:{block}"),
        InlineKeyboardButton("💬 Коммент", callback_data=f"comment:{block}"),
    ]
    # Кнопка WebApp-формы (если указан URL)
    try:
        chat_id_str = str(query.message.chat_id) if hasattr(query, "message") and query.message else str(query._chat_id)
    except Exception:
        chat_id_str = ""
    if WEBAPP_ENABLED and WEBAPP_URL and "<your-pages-domain>" not in WEBAPP_URL:
        webapp_button = InlineKeyboardButton("📝 Коммент (форма)", web_app=WebAppInfo(url=f"{WEBAPP_URL}?block={block}&chat_id={chat_id_str}"))
        bottom.append(webapp_button)
    back_btn = [InlineKeyboardButton("⬅️ Назад", callback_data=f"vote:{block}")]

    keyboard = [phase_buttons, prog_buttons, bottom, back_btn]
    try:
        await query.edit_message_text("\n".join(text_lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
    except BadRequest as e:
        if "Button_type_invalid" in str(e):
            bottom2 = [btn for btn in bottom if not (isinstance(btn, InlineKeyboardButton) and getattr(btn, "web_app", None))]
            keyboard2 = [phase_buttons, prog_buttons, bottom2, back_btn]
            await query.edit_message_text("\n".join(text_lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard2))
        else:
            raise

async def open_status_menu_only_phase(query, context, block: str, show_progress: bool = False):
    chat_id_str = str(query.message.chat_id) if hasattr(query, "message") and query.message else str(query._chat_id)
    status_all = get_status_from_votes(context, chat_id_str)
    st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
    phase = st.get("phase") or "—"
    progress = st.get("progress")
    progress_str = f"{progress}%" if isinstance(progress, int) else "—"
    eta = st.get("eta") or "—"

    text_lines = [f"⚙️ *{block}* — текущий статус\n"]
    text_lines.append(f"Фаза: `{phase}` • Прогресс: `{progress_str}` • ETA: `{eta}`\n")

    rows = []
    if not show_progress:
        # Фазы вертикально по одной в строке
        rows = [[InlineKeyboardButton(p, callback_data=f"phase:{block}|{p}")] for p in PHASES]
    else:
        # Только проценты и свой вариант
        rows.append([InlineKeyboardButton("0%", callback_data=f"progress:{block}:0")])
        rows.append([InlineKeyboardButton("25%", callback_data=f"progress:{block}:25")])
        rows.append([InlineKeyboardButton("50%", callback_data=f"progress:{block}:50")])
        rows.append([InlineKeyboardButton("75%", callback_data=f"progress:{block}:75")])
        rows.append([InlineKeyboardButton("100%", callback_data=f"progress:{block}:100")])
        rows.append([InlineKeyboardButton("✍️", callback_data=f"progress:{block}:txt")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"vote:{block}")])
    await query.edit_message_text("\n".join(text_lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(rows))

async def open_reasons_menu(query, context, block: str, page: int = 0):
    chat_id_str = str(query.message.chat_id) if hasattr(query, "message") and query.message else str(query._chat_id)
    status_all = get_status_from_votes(context, chat_id_str)
    st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
    chat_id_str = str(query.message.chat_id) if hasattr(query, "message") and query.message else str(query._chat_id)
    status_all = get_status_from_votes(context, chat_id_str)
    st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
    selected = set(st.get("reasons") or [])

    # Показываем все причины в одном меню без пагинации
    page_reasons = REASONS

    lines = [f"⏱ Причины задержки для *{block}*\n"]
    lines.append("Отметьте соответствующие пункты и нажмите Готово")
    lines.append("\nЕсли выбрали ‘✍️’ — бот попросит ввести пояснение ответом на сообщение.")

    kb_rows = []
    for key, title in page_reasons:
        mark = "☑️" if key in selected else "⬜"
        # Отметка слева, затем название — без доп. выравнивания
        kb_rows.append([InlineKeyboardButton(f"{mark} {title}", callback_data=f"reason_toggle:{block}:{key}:{page}")])

    kb_rows.append([InlineKeyboardButton("✅ Готово", callback_data="back:main")])

    await query.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb_rows))

async def open_calendar(query, context, block: str, year: int, month: int):
    # Заголовок
    title = datetime.date(year, month, 1).strftime("%B %Y")
    lines = [f"🗓 Выберите дату ETA для *{block}*\n", f"`{title}`"]

    # Строка дней недели (Пн-Вс)
    week_days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    kb_rows = [[InlineKeyboardButton(d, callback_data="noop")] for d in []]  # placeholder not used
    kb_rows.append([InlineKeyboardButton(d, callback_data="noop") for d in week_days])

    cal = calendar.Calendar(firstweekday=0)  # Пн=0
    weeks = cal.monthdayscalendar(year, month)
    for w in weeks:
        row = []
        for day in w:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="noop"))
            else:
                ds = f"{year:04d}-{month:02d}-{day:02d}"
                row.append(InlineKeyboardButton(str(day), callback_data=f"calpick:{block}:{ds}"))
        kb_rows.append(row)

    # Навигация по месяцам
    prev_year, prev_month = (year, month - 1)
    if prev_month == 0:
        prev_year -= 1
        prev_month = 12
    next_year, next_month = (year, month + 1)
    if next_month == 13:
        next_year += 1
        next_month = 1
    nav = [
        InlineKeyboardButton("⬅️", callback_data=f"caleta:{block}:{prev_year}:{prev_month}"),
        InlineKeyboardButton(title, callback_data="noop"),
        InlineKeyboardButton("➡️", callback_data=f"caleta:{block}:{next_year}:{next_month}"),
    ]
    kb_rows.append(nav)

    # Доп. кнопки
    kb_rows.append([
        InlineKeyboardButton("✍️ Ввести", callback_data=f"caltype:{block}"),
        InlineKeyboardButton("⬅️ Назад", callback_data=f"vote:{block}"),
    ])

    await query.edit_message_text("\n".join(lines), parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb_rows))

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wait = context.user_data.get("awaiting")
    if not wait:
        return
    # Для прогресса/ETA ожидаем ввод в том же чате; для комментария разрешаем из любого
    if wait.get("type") in ("progress", "eta") and wait.get("chat_id") != update.effective_chat.id:
        return
    # Если ожидали комментарий — принимаем ответ на наш prompt ИЛИ на любое сообщение бота
    if wait.get("type") == "comment":
        if not update.message or not update.message.reply_to_message:
            return
        replied = update.message.reply_to_message
        is_reply_to_prompt = wait.get("reply_to") and replied.message_id == wait.get("reply_to")
        is_reply_to_bot = getattr(replied.from_user, "is_bot", False)
        if not (is_reply_to_prompt or is_reply_to_bot):
            return
        # Разрешим только пользователю, кто нажал кнопку
        if wait.get("user_id") and update.effective_user and update.effective_user.id != wait.get("user_id"):
            return
    if wait.get("type") == "progress" and wait.get("reply_to"):
        if not update.message or not update.message.reply_to_message or update.message.reply_to_message.message_id != wait.get("reply_to"):
            return
        if wait.get("user_id") and update.effective_user and update.effective_user.id != wait.get("user_id"):
            return
    if wait.get("type") == "eta" and wait.get("reply_to"):
        if not update.message or not update.message.reply_to_message or update.message.reply_to_message.message_id != wait.get("reply_to"):
            return
        if wait.get("user_id") and update.effective_user and update.effective_user.id != wait.get("user_id"):
            return
    block = wait.get("block")
    target_chat_id = wait.get("chat_id") or update.effective_chat.id
    chat_id_str = str(target_chat_id)
    status_all = get_status_from_votes(context, chat_id_str)
    st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})

    t = wait.get("type")
    text = (update.message.text or "").strip()
    if t == "progress":
        try:
            val = int(text)
            if 0 <= val <= 100:
                st["progress"] = val
                save_all(context)
                # Удалим ответ и prompt
                try:
                    await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
                except Exception:
                    pass
                if wait.get("reply_to"):
                    try:
                        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=wait.get("reply_to"))
                    except Exception:
                        pass
                try:
                    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ Прогресс для {block}: {val}%")
                except Exception:
                    pass
            else:
                await update.message.reply_text("Процент должен быть 0..100. Попробуйте ещё раз.")
                return
        except Exception:
            await update.message.reply_text("Введите число 0..100.")
            return
    elif t == "eta":
        # Простая проверка формата YYYY-MM-DD
        if len(text) == 10 and text[4] == '-' and text[7] == '-':
            st["eta"] = text
            save_all(context)
            # Удалим ответ и prompt
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
            except Exception:
                pass
            if wait.get("reply_to"):
                try:
                    await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=wait.get("reply_to"))
                except Exception:
                    pass
            try:
                await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ ETA для {block}: {text}")
            except Exception:
                pass
        else:
            await update.message.reply_text("Неверный формат. Ожидается YYYY-MM-DD.")
            return
    elif t == "comment":
        st["comment"] = text
        save_all(context)
        # Удалим сообщение пользователя и prompt, чтобы не засорять чат
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
        except Exception:
            pass
        if wait.get("reply_to") and update.message.reply_to_message and update.message.reply_to_message.message_id == wait.get("reply_to"):
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=wait.get("reply_to"))
            except Exception:
                pass
        # Отправим короткое уведомление в виде ephemeral-алерта нельзя; пришлём компактное сообщение и удалим через пару секунд нельзя без scheduler.
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=f"✅ Комментарий для {block} сохранён.")
        except Exception:
            pass
    else:
        return

    # Сброс ожидания и обновление главного сообщения
    context.user_data["awaiting"] = None
    posts = get_posts(context)
    main_post = posts.get(chat_id_str, {}).get("main")
    if main_post and isinstance(main_post, dict) and "message_id" in main_post:
        try:
            class DummyQuery:
                def __init__(self, chat_id, message_id):
                    self._chat_id = chat_id
                    self._message_id = message_id
                async def edit_message_text(self, text, parse_mode=None, reply_markup=None):
                    await context.bot.edit_message_text(chat_id=self._chat_id, message_id=self._message_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
            dummy_query = DummyQuery(update.effective_chat.id, main_post["message_id"])
            await update_main_survey(dummy_query, context)
        except Exception:
            pass

async def on_webapp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not getattr(msg, "web_app_data", None):
        return
    try:
        payload = msg.web_app_data.data or "{}"
        data = json.loads(payload)
    except Exception:
        await msg.reply_text("Не удалось прочитать данные формы.")
        return
    block = data.get("block")
    comment = data.get("comment")
    phase = data.get("phase")
    progress = data.get("progress")
    eta = data.get("eta")
    reasons = data.get("reasons") or []
    chat_id = str(data.get("chat_id") or msg.chat.id)
    if not block or block not in BLOCKS or not isinstance(comment, str):
        await msg.reply_text("Некорректные данные формы.")
        return
    status_all = get_status_from_votes(context, chat_id)
    st = status_all.setdefault(block, {"phase": None, "progress": None, "eta": None, "reasons": [], "comment": None})
    if isinstance(comment, str):
        st["comment"] = comment.strip()
    if isinstance(phase, str) and phase in PHASES:
        st["phase"] = phase
    try:
        p = int(progress)
        if 0 <= p <= 100:
            st["progress"] = p
    except Exception:
        pass
    if isinstance(eta, str) and len(eta) == 10 and eta[4] == '-' and eta[7] == '-':
        st["eta"] = eta
    if isinstance(reasons, list):
        st["reasons"] = [r for r in reasons if isinstance(r, str)]
    save_all(context)
    await msg.reply_text(f"✅ Обновлено: {block}")
    # Обновим основное сообщение, если есть
    posts = get_posts(context)
    main_post = posts.get(chat_id, {}).get("main")
    if main_post and isinstance(main_post, dict) and "message_id" in main_post:
        try:
            class DummyQuery:
                def __init__(self, chat_id, message_id):
                    self._chat_id = chat_id
                    self._message_id = message_id
                async def edit_message_text(self, text, parse_mode=None, reply_markup=None):
                    await context.bot.edit_message_text(chat_id=self._chat_id, message_id=self._message_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
            dummy_query = DummyQuery(int(chat_id), main_post["message_id"])
            await update_main_survey(dummy_query, context)
        except Exception:
            pass

## inline handlers удалены

async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Только админ в группе
    if update.effective_chat.type in ("group", "supergroup"):
        if not await is_admin(update, context):
            await update.message.reply_text("⛔ Только администраторы могут выполнить /reset в группе.")
            return
    
    # Очистка голосов (в памяти и в файле)
    votes = get_votes(context)
    votes.clear()
    save_all(context)

    # Обновим главное сообщение, если оно есть
    posts = get_posts(context)
    chat_id = str(update.effective_chat.id)
    main_post = posts.get(chat_id, {}).get("main")
    if main_post and isinstance(main_post, dict) and "message_id" in main_post:
        try:
            class DummyQuery:
                def __init__(self, chat_id, message_id):
                    self._chat_id = chat_id
                    self._message_id = message_id
                async def edit_message_text(self, text, parse_mode=None, reply_markup=None):
                    await context.bot.edit_message_text(chat_id=self._chat_id, message_id=self._message_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
            dummy_query = DummyQuery(int(chat_id), main_post["message_id"])
            await update_main_survey(dummy_query, context)
            await update.message.reply_text("✅ Статистика сброшена.")
            return
        except Exception:
            pass
    
    # Если главного сообщения нет — просто подтвердим сброс
    await update.message.reply_text("✅ Статистика сброшена.")

async def cmd_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    # Используем только текущую оценку из status:<chat_id>
    status_map = get_status_from_votes(context, chat_id)
    
    lines = ["📊 *Сводка по всем блокам VP4:*\n"]
    
    # Сортируем по убыванию риска для наглядности
    for b in BLOCKS:
        bs = status_map.get(b, {})
        score = bs.get("risk") if isinstance(bs, dict) else None
        if isinstance(score, int):
            color, description = get_risk_info(score)
            lines.append(f"{color} *{b}*: {score}/10 ({description})")
        else:
            lines.append(f"⚪ *{b}*: — (нет оценок)")
    
    lines.append("\n📈 Шкала: 1 — все ок; 10 — критично")
    lines.append("🟢 Низкий риск • 🟡 Умеренный • 🟠 Средний • 🔴 Высокий")
    
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_clearvote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Очистить голос конкретного пользователя по конкретному блоку в текущем чате
    # Синтаксис: /clearvote <user_id> <block>
    if update.effective_chat.type in ("group", "supergroup"):
        if not await is_admin(update, context):
            await update.message.reply_text("⛔ Только администратор может использовать /clearvote в группе.")
            return
    text = (update.message.text or "").strip()
    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        await update.message.reply_text("Использование: /clearvote <user_id> <block>")
        return
    try:
        target_user_id = int(parts[1])
    except Exception:
        await update.message.reply_text("user_id должен быть числом.")
        return
    block = parts[2].strip()
    if block not in BLOCKS:
        await update.message.reply_text("Неизвестный блок.")
        return

    votes = get_votes(context)
    uid_key = str(target_user_id)
    changed = False
    if uid_key in votes and isinstance(votes[uid_key], dict) and block in votes[uid_key]:
        del votes[uid_key][block]
        if not votes[uid_key]:
            del votes[uid_key]
        changed = True

    # Если "последняя" оценка (внутри votes под спец-ключом) принадлежала этому пользователю — уберём её
    chat_key = str(update.effective_chat.id)
    # legacy latest map удалён — больше ничего не чистим здесь

    if not changed:
        await update.message.reply_text("Нечего удалять: записей не найдено.")
        return

    save_all(context)
    await update.message.reply_text(f"✅ Удалено: user {target_user_id}, блок {block}")

    # Перерисуем основную таблицу, если есть
    posts = get_posts(context)
    main_post = posts.get(chat_key, {}).get("main")
    if main_post and isinstance(main_post, dict) and "message_id" in main_post:
        try:
            class DummyQuery:
                def __init__(self, chat_id, message_id):
                    self._chat_id = chat_id
                    self._message_id = message_id
                async def edit_message_text(self, text, parse_mode=None, reply_markup=None):
                    await context.bot.edit_message_text(chat_id=self._chat_id, message_id=self._message_id, text=text, parse_mode=parse_mode, reply_markup=reply_markup)
            dummy_query = DummyQuery(update.effective_chat.id, main_post["message_id"])
            await update_main_survey(dummy_query, context)
        except Exception:
            pass

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("survey", cmd_survey))
    app.add_handler(CommandHandler("results", cmd_results))
    app.add_handler(CommandHandler("clearvote", cmd_clearvote))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CallbackQueryHandler(on_button))
    # Сначала обработчик текстового ввода (комментарий/ETA/прогресс)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    # Затем обработчик данных из WebApp
    app.add_handler(MessageHandler(filters.ALL, on_webapp))
    app.run_polling()

if __name__ == "__main__":
    main()
