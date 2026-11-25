#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["python-telegram-bot>=21.0", "websockets>=14.0"]
# ///
"""herdr-remote Telegram bot — monitor and approve agents from Telegram."""
import asyncio, hashlib, html, json, os, logging, re, secrets
from collections import Counter, OrderedDict

from telegram import BotCommand, ForceReply, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, ContextTypes, filters

from agent_state import apply_agent_message

logging.basicConfig(level=logging.INFO)
# httpx logs every request URL at INFO; that URL contains the bot token. Silence it.
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("herdr-tg")

TOKEN = os.environ.get("HERDR_TG_TOKEN", "")
CHAT_ID = os.environ.get("HERDR_TG_CHAT_ID", "")
RELAY_WS = os.environ.get("HERDR_RELAY", "ws://127.0.0.1:8375")
# Optional cold-start hook: an executable run once per `/run <args>` or `/start <args>`
# as `<cmd> --chat-id <id> --text "/<verb> <args>"`; its stdout is the chat reply.
# Unset = no /run command and /start stays the dashboard.
SPAWN_CMD = os.environ.get("HERDR_TG_SPAWN_CMD", "")
# Verbs forwarded to SPAWN_CMD as `/<verb> <args>`; anything else is treated as /run.
SPAWN_VERBS = frozenset({"run", "start", "job"})
RELAY_WS_SAFE = RELAY_WS.split("?", 1)[0]  # token-free variant for display and logging; never leak the token
_RELAY_TOKEN = RELAY_WS.split("token=", 1)[1] if "token=" in RELAY_WS else ""


def scrub(value) -> str:
    """Strip secrets from any string before it is logged or sent to Telegram.

    WebSocket exceptions (e.g. InvalidURI) embed the full relay URL incl. the
    ?token= query, so raw exception text must never be surfaced unredacted.
    """
    s = str(value)
    for secret in (_RELAY_TOKEN, TOKEN):
        if secret:
            s = s.replace(secret, "<redacted>")
    return s

if not TOKEN:
    print("Set HERDR_TG_TOKEN (from @BotFather)")
    exit(1)

# State
pending: OrderedDict[tuple[int, int], str] = OrderedDict()  # (chat_id, message_id) -> pane_id
approval_tokens: dict[str, str] = {}  # pane_id -> current blocked-notification generation
blocked_prompt_ids: dict[str, str] = {}  # pane_id -> current relay prompt identity
agents: list[dict] = []       # current agent list from relay
prev_statuses: dict[str, str] = {}  # pane_id -> last known status
relay_connected = False
daily_stats: dict[str, dict] = {}  # pane_id -> {agent, project, blocked_count, working_mins, last_change}

AGENT_PAGE_SIZE = 20
PENDING_LIMIT = 500
COMMAND_ACK_TIMEOUT = 20
# python-telegram-bot's 5s default connect timeout includes the DNS lookup, and glibc
# waits 5s before retrying a dropped DNS query, so one lost query fails a send outright.
TELEGRAM_CONNECT_TIMEOUT = 15
# Covers the spawn command's own 120s agent-start timeout plus worktree creation.
SPAWN_TIMEOUT = 180
SPAWN_ERROR_TAIL = 500
# herdr-routines' success reply: "Spawned <name> on <where>, pane <id>. Reply to this
# message to steer it." The pane id is what makes that reply steerable.
SPAWNED_PANE_RE = re.compile(r", pane ([A-Za-z0-9:_-]+)\. Reply to this message")
PANE_READ_TIMEOUT = 20
# The relay shells out to opencode for this (a status call, then an api call), and falling back to
# the screen is cheap, so give up sooner than a pane read.
LAST_MESSAGE_TIMEOUT = 12
# Telegram rejects a message over 4096 characters, counted in UTF-16 units after entities are
# parsed (so `&lt;` counts 1, an emoji 2). Chunks stay well under it; heading and footer come out
# of the same budget.
TELEGRAM_MESSAGE_LIMIT = 4096
TELEGRAM_LIMIT_MARGIN = 96
# How many rows of the pane to ask the relay for, and how many non-blank lines of them to show.
# A TUI's input box and footer are tall (opencode 2's is ~8 rows, with blank rows around it), so
# the old 15 raw rows held only chrome and pushed the agent's last answer out of view.
PANE_READ_WINDOW = 160
PANE_SHOW_LINES = 40
STATUS_ORDER = {"blocked": 0, "working": 1, "done": 2, "idle": 3, "unknown": 3}
STATUS_LABELS = {"blocked": "BLOCKED", "working": "WORKING", "done": "DONE", "idle": "IDLE", "unknown": "IDLE"}
ACTION_CODES = {
    "read": "r",
    "interrupt": "i",
    "select_send": "s",
    "select_reply": "q",
    "trust": "t",
    "approval": "k",
}
CODE_ACTIONS = {code: action for action, code in ACTION_CODES.items()}


# --- Relay communication ---

async def await_command_result(ws, request_id: str, command: str):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + COMMAND_ACK_TIMEOUT
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise RuntimeError(f"relay did not acknowledge {command}")
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise RuntimeError(f"relay did not acknowledge {command}") from exc
        response = json.loads(raw)
        response_request_id = response.get("request_id")
        if response.get("type") == "command_result" and response_request_id != request_id:
            continue
        if response_request_id not in (None, request_id):
            continue
        if response.get("type") == "error":
            raise RuntimeError(response.get("message", f"relay rejected {command}"))
        if response.get("type") == "command_result" and response.get("command") == command:
            if not response.get("ok"):
                raise RuntimeError(response.get("message", f"relay rejected {command}"))
            return


async def send_to_relay(pane_id: str, text: str, prompt_id: str | None = None):
    """Send a response to the relay via WebSocket."""
    import websockets
    async with websockets.connect(RELAY_WS) as ws:
        request_id = secrets.token_hex(8)
        await ws.send(json.dumps({
            "type": "respond",
            "pane_id": pane_id,
            "prompt_id": prompt_id if prompt_id is not None else blocked_prompt_ids.get(pane_id, ""),
            "text": text,
            "request_id": request_id,
        }))
        await await_command_result(ws, request_id, "respond")


async def send_keys_to_relay(pane_id: str, keys: list[str], prompt_id: str | None = None):
    """Send raw key presses to the relay via WebSocket (e.g. ["1"] to pick a prompt option)."""
    import websockets
    async with websockets.connect(RELAY_WS) as ws:
        request_id = secrets.token_hex(8)
        message = {
            "type": "send_keys",
            "pane_id": pane_id,
            "keys": keys,
            "request_id": request_id,
        }
        if prompt_id is not None:
            message["prompt_id"] = prompt_id
        await ws.send(json.dumps(message))
        await await_command_result(ws, request_id, "send_keys")


def interrupt_keys(agent: dict) -> tuple[list[str], str]:
    """The keys that interrupt `agent`'s running turn, and their label for the reply.

    opencode 2 takes Escape twice (`esc interrupt` -> `esc again to interrupt`; verified on the Pi,
    2026-11-24: the agent stays alive and settles `done`). Ctrl+C quits its TUI instead, which
    deletes the agent. Everything else keeps Ctrl+C."""
    if agent.get("agent") == "opencode":
        return ["Escape", "Escape"], "Esc Esc"
    return ["C-c"], "Ctrl+C"


def tidy_pane(content: str, keep: int = PANE_SHOW_LINES) -> str:
    """The last `keep` non-blank lines of a pane read, trailing spaces trimmed."""
    shown = [line.rstrip() for line in content.splitlines() if line.strip()]
    return "\n".join(shown[-keep:]) or "(empty)"


def pane_html(content: str, project: str | None = None, footer: str | None = None) -> str:
    """Telegram HTML for an agent's screen: the text in a code block so the TUI's spacing and
    box drawing survive, with an optional project heading and footer line."""
    parts = []
    if project:
        parts.append(f"{html.escape(project)}:")
    parts.append(f"<pre>{html.escape(content)}</pre>")
    if footer:
        parts.append(html.escape(footer))
    return "\n\n".join(parts)


def utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def split_for_telegram(text: str, budget: int) -> list[str]:
    """Split `text` into chunks of at most `budget` UTF-16 units, on line boundaries where it can
    and inside a line only when that one line alone is over budget. Nothing is dropped:
    "\n".join(chunks) is `text`, except a hard cut inside a line which joins with no separator."""
    chunks: list[str] = []
    current: list[str] = []
    used = 0

    def flush():
        nonlocal current, used
        if current:
            chunks.append("\n".join(current))
        current, used = [], 0

    for line in text.split("\n"):
        while utf16_len(line) > budget:
            flush()
            cut, units = 0, 0
            for char in line:
                width = utf16_len(char)
                if units + width > budget:
                    break
                units += width
                cut += 1
            chunks.append(line[:cut])
            line = line[cut:]
        cost = utf16_len(line) + 1
        if current and used + cost > budget:
            flush()
        current.append(line)
        used += cost
    flush()
    return chunks or [""]


def pane_messages(content: str, project: str | None = None, footer: str | None = None) -> list[str]:
    """`pane_html` for content of any length: one message per chunk, the project heading on the
    first and the footer on the last."""
    overhead = utf16_len(project or "") + utf16_len(footer or "") + TELEGRAM_LIMIT_MARGIN
    chunks = split_for_telegram(content, TELEGRAM_MESSAGE_LIMIT - overhead)
    last = len(chunks) - 1
    return [
        pane_html(chunk, project if index == 0 else None, footer if index == last else None)
        for index, chunk in enumerate(chunks)
    ]


async def fetch_last_message(agent: dict) -> str | None:
    """An opencode agent's whole last answer, from the relay, or None when the screen should be
    shown instead: not opencode, mid-turn or waiting on a prompt (a permission question lives on
    the screen, not in the message list), an older relay that ignores the request, or any failure.
    Never raises and never surfaces why."""
    if agent.get("agent") != "opencode" or agent.get("status") in ("blocked", "working"):
        return None
    import websockets
    request_id = secrets.token_hex(8)
    try:
        async with websockets.connect(RELAY_WS) as ws:
            await ws.send(json.dumps({
                "type": "last_message", "pane_id": agent["pane_id"], "request_id": request_id,
            }))
            loop = asyncio.get_running_loop()
            deadline = loop.time() + LAST_MESSAGE_TIMEOUT
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return None
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining))
                if msg.get("type") == "error" and msg.get("request_id") in (None, request_id):
                    return None
                if msg.get("type") == "last_message" and msg.get("request_id") == request_id:
                    text = msg.get("text")
                    return text if isinstance(text, str) and text.strip() and not msg.get("unavailable") else None
    except Exception as exc:
        log.info("last message unavailable for %s: %s", agent.get("pane_id"), scrub(exc))
        return None


async def agent_output(agent: dict, screen_limit: int) -> str:
    """What to show of an agent: its whole last answer when the relay can give it, else the
    screen tail (its last `screen_limit` characters, as before)."""
    message = await fetch_last_message(agent)
    if message is not None:
        return message
    content = await read_pane(agent["pane_id"])
    return content[-screen_limit:] if len(content) > screen_limit else content


async def send_pane_output(message, chat_id: int, pane_id: str, content: str, project: str | None = None):
    """Reply with `content` as one or more code-block messages, each one steerable by replying."""
    for text in pane_messages(content, project):
        sent = await message.reply_text(text, parse_mode="HTML")
        register_pending(chat_id, sent.message_id, pane_id)


async def read_pane(pane_id: str, lines: int = PANE_READ_WINDOW) -> str:
    """Read pane content from relay: the last PANE_SHOW_LINES non-blank lines of `lines` rows."""
    import websockets
    try:
        async with websockets.connect(RELAY_WS) as ws:
            await ws.send(json.dumps({"type": "read_pane", "pane_id": pane_id, "lines": lines}))
            loop = asyncio.get_running_loop()
            deadline = loop.time() + PANE_READ_TIMEOUT
            # The connect preamble (sessions/agents/one-blocked-per-blocked-
            # agent) can put any number of unrelated messages ahead of
            # pane_content. Skip by deadline, not by a fixed message count.
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return "(no response)"
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                msg = json.loads(raw)
                if msg.get("type") == "pane_content":
                    return tidy_pane(msg.get("content", ""))
    except Exception as e:
        return f"(error reading pane: {scrub(e)})"


async def send_text_to_relay(pane_id: str, text: str):
    """Send text to an agent using 'herdr agent prompt' for proper submission."""
    if not text or len(text) > 10000:
        raise ValueError("text must contain 1-10000 characters")
    import websockets
    async with websockets.connect(RELAY_WS) as ws:
        request_id = secrets.token_hex(8)
        await ws.send(json.dumps({
            "type": "agent_prompt",
            "pane_id": pane_id,
            "text": text,
            "request_id": request_id,
        }))
        await await_command_result(ws, request_id, "agent_prompt")


# --- Auth guard ---

def authorized(update: Update) -> bool:
    """Reject messages from unauthorized users."""
    if not CHAT_ID:
        return True  # No restriction if CHAT_ID not set (first-run discovery mode)
    return str(update.effective_chat.id) == CHAT_ID


def register_pending(chat_id: int, message_id: int, pane_id: str):
    key = (int(chat_id), int(message_id))
    pending[key] = pane_id
    pending.move_to_end(key)
    while len(pending) > PENDING_LIMIT:
        pending.popitem(last=False)


def pending_pane(chat_id: int, message_id: int) -> str | None:
    return pending.get((int(chat_id), int(message_id)))


def find_agent(pane_id: str) -> dict | None:
    matches = [agent for agent in agents if agent.get("pane_id") == pane_id]
    return matches[0] if len(matches) == 1 else None


def clear_relay_connection_state():
    global agents, relay_connected
    relay_connected = False
    agents = []
    approval_tokens.clear()
    blocked_prompt_ids.clear()


async def send_reply_prompt(message, chat, agent: dict, content: str | None = None):
    project = agent.get("project") or agent.get("agent") or "agent"
    footer = "Reply to this message to send your response."
    if content is None:
        texts = [f"{html.escape(project)}:\n\n{html.escape(footer)}"]
    else:
        texts = pane_messages(content, project, footer)
    kwargs = {"parse_mode": "HTML"}
    sent = None
    for index, text in enumerate(texts):
        extra = {}
        # The input box follows the LAST message, which is also the one with the footer.
        if index == len(texts) - 1 and getattr(chat, "type", None) == "private":
            extra["reply_markup"] = ForceReply(
                selective=True,
                input_field_placeholder=f"Reply to {project}"[:64],
            )
        sent = await message.reply_text(text, **kwargs, **extra)
        register_pending(chat.id, sent.message_id, agent["pane_id"])
    return sent


def agents_for_action(action: str) -> list[dict]:
    if action == "interrupt":
        return [agent for agent in agents if agent.get("status") in ("working", "blocked")]
    if action == "trust":
        return [agent for agent in agents if agent.get("status") == "blocked"]
    return list(agents)


def sorted_agents(agent_list: list[dict]) -> list[dict]:
    return sorted(agent_list, key=lambda agent: (
        STATUS_ORDER.get(agent.get("status", "unknown"), 3),
        agent.get("project", "").lower(),
        agent.get("agent", "").lower(),
        agent.get("host", "local").lower(),
        agent.get("pane_id", ""),
    ))


def compact_identifier(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    digest = hashlib.sha1(value.encode()).hexdigest()[:12]
    return value[:limit - 15] + "..." + digest


def pane_callback_token(pane_id: str) -> str:
    return hashlib.sha256(pane_id.encode()).hexdigest()[:16]


def pane_callback_data(action: str, pane_id: str, **extra) -> str:
    data = {"a": ACTION_CODES[action], "p": pane_callback_token(pane_id), **extra}
    return json.dumps(data, separators=(",", ":"))


def resolve_pane_token(token: str) -> str | None:
    agent_matches = [
        agent.get("pane_id") for agent in agents
        if agent.get("pane_id") and pane_callback_token(agent["pane_id"]) == token
    ]
    if len(agent_matches) == 1:
        return agent_matches[0]
    if len(agent_matches) > 1:
        return None
    pending_matches = {
        pane_id for pane_id in pending.values()
        if pane_id and pane_callback_token(pane_id) == token
    }
    return next(iter(pending_matches)) if len(pending_matches) == 1 else None


def parse_callback_data(raw: str) -> dict:
    data = json.loads(raw)
    if "a" in data:
        data["action"] = CODE_ACTIONS.get(data["a"], "invalid")
    if "p" in data:
        data["pane_id"] = resolve_pane_token(data["p"])
    return data


def agent_button_labels(agent_list: list[dict]) -> list[str]:
    bases = []
    contexts = []
    for agent in agent_list:
        status = STATUS_LABELS.get(agent.get("status", "unknown"), "UNKNOWN")
        project = agent.get("project") or agent.get("cwd") or "unknown"
        name = agent.get("agent") or "agent"
        host = agent.get("host", "local")
        bases.append(f"[{status}] {project} ({name})")
        contexts.append(f" @{compact_identifier(host, 24)}" if host != "local" else "")

    provisional = [base[:max(1, 64 - len(context))] + context for base, context in zip(bases, contexts)]
    counts = Counter(provisional)
    labels = []
    for agent, base, context, candidate in zip(agent_list, bases, contexts, provisional):
        if counts[candidate] == 1:
            labels.append(candidate)
            continue
        pane_id = compact_identifier(agent.get("pane_id", "?"), 18)
        suffix = context + f" [{pane_id}]"
        labels.append(base[:max(1, 64 - len(suffix))] + suffix)
    unique_labels = []
    used = set()
    for label in labels:
        candidate = label
        ordinal = 2
        while candidate in used:
            marker = f" #{ordinal}"
            candidate = label[:64 - len(marker)] + marker
            ordinal += 1
        used.add(candidate)
        unique_labels.append(candidate)
    return unique_labels


def build_agent_keyboard(action: str, page: int = 0, agent_list: list[dict] | None = None) -> InlineKeyboardMarkup:
    ordered = sorted_agents(agents_for_action(action) if agent_list is None else agent_list)
    page_count = max(1, (len(ordered) + AGENT_PAGE_SIZE - 1) // AGENT_PAGE_SIZE)
    page = min(max(page, 0), page_count - 1)
    start = page * AGENT_PAGE_SIZE
    visible = ordered[start:start + AGENT_PAGE_SIZE]
    labels = agent_button_labels(ordered)[start:start + AGENT_PAGE_SIZE]
    keyboard = [[InlineKeyboardButton(
        label,
        callback_data=pane_callback_data(action, agent["pane_id"]),
    )] for agent, label in zip(visible, labels)]

    if page_count > 1:
        navigation = []
        if page > 0:
            navigation.append(InlineKeyboardButton(
                "Previous",
                callback_data=json.dumps(
                    {"action": "page", "menu": action, "page": page - 1}, separators=(",", ":")
                ),
            ))
        if page + 1 < page_count:
            navigation.append(InlineKeyboardButton(
                "Next",
                callback_data=json.dumps(
                    {"action": "page", "menu": action, "page": page + 1}, separators=(",", ":")
                ),
            ))
        keyboard.append(navigation)
    return InlineKeyboardMarkup(keyboard)


def refresh_keyboard(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "Refresh agents",
            callback_data=json.dumps({"action": "page", "menu": action, "page": 0}, separators=(",", ":")),
        )
    ]])


def dashboard_text() -> str:
    if not relay_connected:
        text = "herdr-remote bot\n\nRelay disconnected. Use /status for connection details."
    elif not agents:
        text = "herdr-remote bot\n\nConnected to relay. No agents are running."
    else:
        blocked = sum(agent.get("status") == "blocked" for agent in agents)
        working = sum(agent.get("status") == "working" for agent in agents)
        done = sum(agent.get("status") == "done" for agent in agents)
        idle = len(agents) - blocked - working - done
        text = f"herdr-remote bot\n\nAgents: {len(agents)} ({blocked} blocked, {working} working, {done} done, {idle} idle)\nSelect an agent to read and reply."
    if not CHAT_ID:
        text += "\n\nChat ID: {chat_id}"
    return text


async def run_spawn_command(chat_id: int, text: str) -> str:
    """Run SPAWN_CMD for one cold-start message and return the reply to send back.

    The message reaches the command only as argv, never through a shell."""
    proc = await asyncio.create_subprocess_exec(
        SPAWN_CMD, "--chat-id", str(chat_id), "--text", text,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), SPAWN_TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return f"Spawn timed out after {SPAWN_TIMEOUT}s."
    reply = out.decode(errors="replace").strip()
    if proc.returncode != 0 and not reply:
        tail = err.decode(errors="replace").strip()[-SPAWN_ERROR_TAIL:]
        return f"Spawn failed (exit {proc.returncode}): {tail}"
    return reply or "Nothing to spawn: send /run <alias>."


# --- Bot commands ---

async def cmd_run(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /run <alias> (and /start <alias>): cold-start a session via SPAWN_CMD.

    Also /job <name>: herdr-routines runs a `request: true` job through its scheduled
    path (herdr-routines issue 075). SPAWN_CMD decides what each verb does."""
    if not authorized(update):
        return
    verb = update.message.text.split()[0].lstrip("/").split("@")[0]
    if verb not in SPAWN_VERBS:
        verb = "run"
    # The verb is rebuilt so a `/run@bot_name` suffix never reaches the command. The rest
    # of the message is kept as typed: the lines after the command are the agent's first
    # prompt, and ctx.args would flatten them into one line.
    parts = update.message.text.split(None, 1)
    rest = parts[1].strip() if len(parts) > 1 else ""
    text = f"/{verb} {rest}" if rest else f"/{verb}"
    try:
        reply = await run_spawn_command(update.effective_chat.id, text)
    except OSError as e:
        reply = f"Spawn failed: {e}"
    sent = await update.message.reply_text(scrub(reply))
    if match := SPAWNED_PANE_RE.search(reply):
        register_pending(update.effective_chat.id, sent.message_id, match.group(1))


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    if not authorized(update):
        return
    if SPAWN_CMD and ctx.args:
        await cmd_run(update, ctx)
        return
    text = dashboard_text()
    if not CHAT_ID:
        text = text.format(chat_id=update.effective_chat.id)
    if relay_connected and agents:
        await update.message.reply_text(text, reply_markup=build_agent_keyboard("select_reply"))
        return
    await update.message.reply_text(text)


HELP_TEXT = """Replying
- Reply to any bot message about an agent to send it text.
- Blocked agents get approval buttons. For opencode permission prompts you can also reply \
"allow once", "allow always" or "reject" (or once / always / no).
- /agents or /reply opens a picker; choose an agent to see its output and reply.

Commands
/agents - list agents and pick one
/reply [name] - show an agent's output and reply
/send <name> <text> - send text to an agent
/read [name] - recent terminal output
/esc [name] - interrupt an agent (Esc Esc for opencode, else Ctrl+C)
/trust [name] - always-allow a blocked agent's prompt
/status - relay connection
/digest - today's activity"""

SPAWN_HELP = """

Starting agents
/run <alias> [harness] [model] - start a new agent
  Put a first prompt on the next lines, or after -- on the same line:
  /run myrepo opencode -- review the auth module
/job <name> - run a configured job"""


def bot_commands() -> list[BotCommand]:
    commands = [
        BotCommand("agents", "List agents and pick one"),
        BotCommand("reply", "Read an agent's output and reply"),
        BotCommand("send", "Send text to an agent"),
        BotCommand("read", "Recent terminal output"),
        BotCommand("esc", "Interrupt an agent"),
        BotCommand("trust", "Always-allow a blocked agent"),
    ]
    if SPAWN_CMD:
        commands += [
            BotCommand("run", "Start an agent, optionally with a prompt"),
            BotCommand("job", "Run a configured job"),
        ]
    return commands + [
        BotCommand("status", "Relay connection"),
        BotCommand("digest", "Today's activity"),
        BotCommand("help", "How replies work"),
    ]


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /help — how replies and commands work."""
    if not authorized(update):
        return
    await update.message.reply_text(HELP_TEXT + (SPAWN_HELP if SPAWN_CMD else ""))


async def cmd_agents(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /agents — list current agents with status."""
    if not agents:
        await update.message.reply_text("No agents connected." if relay_connected else "Not connected to relay.")
        return

    blocked = [a for a in agents if a.get("status") == "blocked"]
    working = [a for a in agents if a.get("status") == "working"]
    done = [a for a in agents if a.get("status") == "done"]
    idle = [a for a in agents if a.get("status") in ("idle", "unknown")]

    lines = []
    if blocked:
        lines.append("BLOCKED:")
        for a in blocked:
            host = f" @{a['host']}" if a.get('host', 'local') != 'local' else ''
            lines.append(f"  {a['project']} ({a['agent']}){host}")
    if working:
        lines.append("WORKING:")
        for a in working:
            host = f" @{a['host']}" if a.get('host', 'local') != 'local' else ''
            lines.append(f"  {a['project']} ({a['agent']}){host}")
    if done:
        lines.append("DONE:")
        for a in done:
            host = f" @{a['host']}" if a.get('host', 'local') != 'local' else ''
            lines.append(f"  {a['project']} ({a['agent']}){host}")
    if idle:
        lines.append("IDLE:")
        for a in idle:
            host = f" @{a['host']}" if a.get('host', 'local') != 'local' else ''
            lines.append(f"  {a['project']} ({a['agent']}){host}")

    lines.extend(["", "Pick an agent to read its output and reply."])
    await update.message.reply_text("\n".join(lines), reply_markup=build_agent_keyboard("select_reply"))


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /status — show connection info."""
    b = len([a for a in agents if a.get("status") == "blocked"])
    w = len([a for a in agents if a.get("status") == "working"])
    d = len([a for a in agents if a.get("status") == "done"])
    i = len([a for a in agents if a.get("status") in ("idle", "unknown")])

    status = "Connected" if relay_connected else "Disconnected"
    text = (
        f"Relay: {RELAY_WS_SAFE}\n"
        f"Status: {status}\n"
        f"Agents: {len(agents)} ({b} blocked, {w} working, {d} done, {i} idle)"
    )
    await update.message.reply_text(text)


async def cmd_read(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /read [project] — read pane output."""
    args = ctx.args
    if not args:
        # Show agent picker
        if not agents:
            await update.message.reply_text("No agents. Use /agents to check.")
            return
        await update.message.reply_text("Read which agent?", reply_markup=build_agent_keyboard("read"))
        return

    # Find agent by project name
    query = " ".join(args).lower()
    match = next((a for a in agents if query in a.get("project", "").lower() or query in a.get("agent", "").lower()), None)
    if not match:
        await update.message.reply_text(f"No agent matching '{query}'. Use /agents to see list.")
        return

    content = await agent_output(match, 3500)
    await send_pane_output(update.message, update.effective_chat.id, match["pane_id"], content, match["project"])


async def cmd_interrupt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /interrupt [project] — interrupt the agent (see interrupt_keys)."""
    args = ctx.args
    if not args:
        if not agents:
            await update.message.reply_text("No agents.")
            return
        working = [a for a in agents if a.get("status") in ("working", "blocked")]
        if not working:
            await update.message.reply_text("No active agents to interrupt.")
            return
        await update.message.reply_text(
            "Interrupt which agent?",
            reply_markup=build_agent_keyboard("interrupt", agent_list=working),
        )
        return

    query = " ".join(args).lower()
    match = next((a for a in agents if query in a.get("project", "").lower() or query in a.get("agent", "").lower()), None)
    if not match:
        await update.message.reply_text(f"No agent matching '{query}'.")
        return

    try:
        keys, label = interrupt_keys(match)
        await send_keys_to_relay(match["pane_id"], keys)
        await update.message.reply_text(f"Sent {label} to {match['project']}")
    except Exception as e:
        await update.message.reply_text(f"Failed: {scrub(e)}")


async def cmd_send(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /send [project] [text] — send text + Enter to a pane."""
    args = ctx.args
    if not args:
        if not agents:
            await update.message.reply_text("No agents.")
            return
        await update.message.reply_text(
            "Send to which agent?\n(After selecting, reply with your text)",
            reply_markup=build_agent_keyboard("select_send"),
        )
        return

    query = args[0].lower()
    match = next((a for a in agents if query in a.get("project", "").lower() or query in a.get("agent", "").lower()), None)
    if not match:
        await update.message.reply_text(f"No agent matching '{query}'. Use /agents to see list.")
        return

    text = " ".join(args[1:])
    if not text:
        await send_reply_prompt(update.message, update.effective_chat, match)
        return

    try:
        await send_text_to_relay(match["pane_id"], text)
        await update.message.reply_text(f"Sent to {match['project']}: {text}")
    except Exception as e:
        await update.message.reply_text(f"Failed: {scrub(e)}")


async def cmd_reply(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /reply [project] — show agent output then accept input."""
    args = ctx.args

    if not agents:
        await update.message.reply_text("No agents.")
        return

    if not args:
        await update.message.reply_text("Reply to which agent?", reply_markup=build_agent_keyboard("select_reply"))
        return

    query = " ".join(args).lower()
    match = next((a for a in agents if query in a.get("project", "").lower() or query in a.get("agent", "").lower()), None)
    if not match:
        await update.message.reply_text(f"No agent matching '{query}'.")
        return

    content = await agent_output(match, 3000)
    await send_reply_prompt(update.message, update.effective_chat, match, content)


async def cmd_trust(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /trust [project] — send 'trust, always allow' to a blocked agent."""
    args = ctx.args
    blocked = [a for a in agents if a.get("status") == "blocked"]

    if not blocked:
        await update.message.reply_text("No blocked agents.")
        return

    if not args:
        await update.message.reply_text(
            "Trust which agent?",
            reply_markup=build_agent_keyboard("trust", agent_list=blocked),
        )
        return

    query = " ".join(args).lower()
    match = next((a for a in blocked if query in a.get("project", "").lower() or query in a.get("agent", "").lower()), None)
    if not match:
        await update.message.reply_text(f"No blocked agent matching '{query}'.")
        return

    try:
        await send_to_relay(
            match["pane_id"],
            "trust, always allow",
            prompt_id=blocked_prompt_ids.get(match["pane_id"]),
        )
        await update.message.reply_text(f"Trusted {match['project']} (always allow)")
    except Exception as e:
        await update.message.reply_text(f"Failed: {scrub(e)}")


async def cmd_digest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle /digest — show today's agent activity summary."""
    if not daily_stats:
        await update.message.reply_text("No activity recorded yet today.")
        return

    import time
    lines = ["Today's activity:\n"]
    sorted_agents = sorted(daily_stats.values(), key=lambda x: x.get("working_mins", 0), reverse=True)
    for s in sorted_agents:
        blocked = f", blocked {s['blocked_count']}x" if s.get("blocked_count") else ""
        mins = s.get("working_mins", 0)
        time_str = f"{mins}m" if mins < 60 else f"{mins//60}h{mins%60}m"
        lines.append(f"  {s['project']} ({s['agent']}): {time_str} working{blocked}")

    await update.message.reply_text("\n".join(lines))


# --- Callback handler (buttons) ---

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle inline keyboard button presses."""
    query = update.callback_query
    if CHAT_ID and str(update.effective_chat.id) != CHAT_ID:
        await query.answer("Unauthorized")
        return
    await query.answer()

    if not relay_connected:
        await query.message.reply_text("Relay disconnected. Use /start after it reconnects.")
        return

    data = parse_callback_data(query.data)
    action = data.get("action", "approval")

    if action == "page":
        menu = data.get("menu", "select_reply")
        if not agents_for_action(menu):
            await query.message.reply_text("No eligible agents are available.")
            return
        await query.edit_message_reply_markup(
            reply_markup=build_agent_keyboard(menu, page=data.get("page", 0))
        )
        return

    selected_agent = None
    if action in {"read", "interrupt", "select_send", "select_reply", "trust", "approval"}:
        eligible_action = "trust" if action == "approval" else action
        selected_agent = next(
            (agent for agent in agents_for_action(eligible_action) if agent.get("pane_id") == data.get("pane_id")),
            None,
        )
        if selected_agent is None:
            await query.message.reply_text(
                "That agent is no longer available for this action. Refresh the list and choose another agent.",
                reply_markup=refresh_keyboard(eligible_action),
            )
            return

    if action == "read":
        content = await agent_output(selected_agent, 3500)
        await send_pane_output(query.message, update.effective_chat.id, data["pane_id"], content)
        return

    if action == "interrupt":
        try:
            keys, label = interrupt_keys(selected_agent)
            await send_keys_to_relay(data["pane_id"], keys)
            await query.message.reply_text(f"Sent {label}")
        except Exception as e:
            await query.message.reply_text(f"Failed: {scrub(e)}")
        return

    if action == "select_send":
        await send_reply_prompt(query.message, update.effective_chat, selected_agent)
        return

    if action == "select_reply":
        content = await agent_output(selected_agent, 3000)
        await send_reply_prompt(query.message, update.effective_chat, selected_agent, content)
        return

    if action == "trust":
        try:
            await send_to_relay(
                data["pane_id"],
                "trust, always allow",
                prompt_id=blocked_prompt_ids.get(data["pane_id"]),
            )
            await query.message.reply_text(f"Trusted {selected_agent['project']} (always allow)")
        except Exception as e:
            await query.message.reply_text(f"Failed: {scrub(e)}")
        return

    if action != "approval":
        await query.message.reply_text("That action is no longer valid. Refresh the list and try again.")
        return

    pane_id = data["pane_id"]
    if not data.get("g") or data["g"] != approval_tokens.get(pane_id):
        await query.message.reply_text(
            "That approval belongs to an older prompt. Use the controls on the latest blocked notification."
        )
        return

    prompt_id = blocked_prompt_ids.get(pane_id)
    if not prompt_id:
        await query.message.reply_text(
            "That approval belongs to an older prompt. Use the controls on the latest blocked notification."
        )
        return

    option_index = data.get("i")
    if option_index is not None:
        try:
            option_index = int(option_index)
        except (TypeError, ValueError):
            await query.message.reply_text("That approval action is no longer supported. Use the latest notification.")
            return
        label = None
        if query.message and query.message.reply_markup:
            for row in query.message.reply_markup.inline_keyboard:
                for btn in row:
                    try:
                        button_data = parse_callback_data(btn.callback_data)
                    except (ValueError, TypeError, json.JSONDecodeError):
                        continue
                    if button_data.get("i") == str(option_index):
                        label = btn.text
                        break
                if label is not None:
                    break
        if not label:
            await query.message.reply_text("That approval action is no longer supported. Use the latest notification.")
            return
        try:
            await send_to_relay(pane_id, label, prompt_id=prompt_id)
        except Exception as e:
            await query.message.reply_text(f"Failed: {scrub(e)}")
            return
        approval_tokens.pop(pane_id, None)
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"Sent: {label}")
        return

    # Confirm a blocked agent's prompt by pressing the option number.
    # Sending the option *text* via `respond` does NOT work: the relay pastes it via
    # send-text, and Claude's TUI treats a pasted trailing newline as paste content,
    # not as Enter, so the prompt never gets confirmed. A real key press does.
    key = data.get("k")
    if key is None:
        await query.message.reply_text("That approval action is no longer supported. Use the latest notification.")
        return
    # Recover the pressed button's label for the confirmation (kept out of
    # callback_data to respect Telegram's 64-byte limit).
    label = f"option {key}"
    if query.message and query.message.reply_markup:
        for row in query.message.reply_markup.inline_keyboard:
            for btn in row:
                try:
                    if json.loads(btn.callback_data).get("k") == key:
                        label = btn.text
                except (ValueError, TypeError):
                    pass
    try:
        await send_keys_to_relay(pane_id, [key], prompt_id=prompt_id)
    except Exception as e:
        await query.message.reply_text(f"Failed: {scrub(e)}")
        return
    approval_tokens.pop(pane_id, None)
    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(f"Sent: {label}")


# --- Free text reply ---

async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Send a Telegram reply to its associated agent pane."""
    if not authorized(update) or not update.message.reply_to_message:
        return
    pane_id = pending_pane(update.effective_chat.id, update.message.reply_to_message.message_id)
    if not pane_id:
        return
    if not relay_connected:
        await update.message.reply_text("Relay disconnected. Use /start after it reconnects.")
        return
    if find_agent(pane_id) is None:
        await update.message.reply_text(
            "That agent is no longer available. Refresh the list and choose another agent.",
            reply_markup=refresh_keyboard("select_reply"),
        )
        return

    try:
        prompt_id = blocked_prompt_ids.get(pane_id)
        if prompt_id:
            await send_to_relay(pane_id, update.message.text, prompt_id=prompt_id)
        else:
            await send_text_to_relay(pane_id, update.message.text)
        await update.message.reply_text("Sent")
    except Exception as e:
        await update.message.reply_text(f"Failed: {scrub(e)}")


# --- Blocked notification ---

TOOL_BUTTONS = [
    ("Yes (once)", "yes, single permission"),
    ("Trust (always)", "trust, always allow"),
    ("No", "no (tab to edit)"),
]

SUBAGENT_BUTTONS = [
    ("Approve all", "approve all pending"),
    ("Configure", "configure individually"),
    ("Cancel", "exit (cancel subagents)"),
]


# Prompts answered by sending the option's label: the relay turns it into the right input
# (an omp question's selection, or the arrow keys that pick an opencode permission option).
LABEL_INTERACTIONS = frozenset({"omp_question", "opencode_permission"})


def make_keyboard(
    pane_id: str,
    options: list[str] | None,
    generation: str | None = None,
    interaction: str | None = None,
) -> InlineKeyboardMarkup:
    if not options:
        return interaction_keyboard(pane_id)
    if interaction in LABEL_INTERACTIONS:
        buttons = [(opt, opt) for opt in options]
    elif "trust" in " ".join(options).lower():
        buttons = TOOL_BUTTONS
    elif "approve all" in " ".join(options).lower():
        buttons = SUBAGENT_BUTTONS
    else:
        buttons = [(opt.split(",")[0], opt) for opt in options]

    generation = generation or secrets.token_hex(4)
    if interaction in LABEL_INTERACTIONS:
        keyboard = [
            [InlineKeyboardButton(label, callback_data=pane_callback_data(
                "approval", pane_id, g=generation, i=str(i),
            ))]
            for i, (label, _resp) in enumerate(buttons)
        ]
        keyboard.append([InlineKeyboardButton(
            "Open output & reply",
            callback_data=pane_callback_data("select_reply", pane_id),
        )])
        return InlineKeyboardMarkup(keyboard)

    # Standard approval prompts require a real numeric key press.
    keyboard = [
        [InlineKeyboardButton(label, callback_data=pane_callback_data(
            "approval", pane_id, g=generation, k=str(i + 1)))]
        for i, (label, _resp) in enumerate(buttons)
    ]
    keyboard.append([InlineKeyboardButton(
        "Open output & reply",
        callback_data=pane_callback_data("select_reply", pane_id),
    )])
    return InlineKeyboardMarkup(keyboard)


def interaction_keyboard(pane_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "Open output & reply",
            callback_data=pane_callback_data("select_reply", pane_id),
        )
    ]])


async def notify_blocked(
    app: Application,
    pane_id: str,
    agent: str,
    project: str,
    prompt: str,
    options: list[str] | None,
    prompt_id: str | None = None,
    interaction: str | None = None,
    multi: bool = False,
):
    if not CHAT_ID:
        return
    instruction = (
        "Use the web terminal or manual terminal controls to select multiple options."
        if multi
        else "Use an approval button, open the output, or reply to this notification."
    )
    text = (
        f"*{agent}* blocked in `{project}`\n\n```\n{prompt[:400]}\n```\n\n"
        f"{instruction}"
    )
    generation = secrets.token_hex(4)
    keyboard = make_keyboard(
        pane_id,
        [] if multi else options,
        generation=generation,
        interaction=interaction,
    )
    msg = await app.bot.send_message(
        chat_id=int(CHAT_ID), text=text, parse_mode="Markdown", reply_markup=keyboard
    )
    approval_tokens[pane_id] = generation
    if prompt_id:
        blocked_prompt_ids[pane_id] = prompt_id
    register_pending(int(CHAT_ID), msg.message_id, pane_id)


async def notify_blocked_safely(app: Application, msg: dict):
    if msg.get("update"):
        return
    try:
        await notify_blocked(
            app,
            pane_id=msg["pane_id"],
            agent=msg.get("agent", "unknown"),
            project=msg.get("project", ""),
            prompt=msg.get("prompt", ""),
            options=msg.get("options"),
            prompt_id=msg.get("prompt_id"),
            interaction=msg.get("interaction"),
            multi=bool(msg.get("multi")),
        )
    except Exception as e:
        log.warning("Failed to send blocked notification: %s", scrub(e))


# --- Relay listener ---

async def track_agent_updates(app: Application, updated_agents: list[dict]):
    """Track status transitions for full snapshots and pane-level updates."""
    if not CHAT_ID:
        return

    import time
    now = time.time()
    for agent_data in updated_agents:
        pane_id = agent_data["pane_id"]
        new_status = agent_data.get("status", "unknown")
        old_status = prev_statuses.get(pane_id)

        if pane_id not in daily_stats:
            daily_stats[pane_id] = {
                "agent": agent_data.get("agent", ""),
                "project": agent_data.get("project", ""),
                "blocked_count": 0,
                "working_mins": 0,
                "last_change": now,
            }
        stats = daily_stats[pane_id]
        if old_status == "working" and old_status != new_status:
            stats["working_mins"] += int((now - stats["last_change"]) / 60)
        if new_status == "blocked" and old_status != "blocked":
            stats["blocked_count"] += 1
        if old_status != new_status:
            stats["last_change"] = now
        if agent_data.get("status") and new_status != "blocked":
            approval_tokens.pop(pane_id, None)
            blocked_prompt_ids.pop(pane_id, None)

        if old_status and old_status != new_status and new_status in ("idle", "done") and old_status in ("working", "blocked"):
            try:
                msg = await app.bot.send_message(
                    chat_id=int(CHAT_ID),
                    text=(
                        f"{agent_data['project']} ({agent_data['agent']}) finished.\n\n"
                        "Open the output below, or reply to this notification to send a follow-up."
                    ),
                    reply_markup=interaction_keyboard(pane_id),
                )
                register_pending(int(CHAT_ID), msg.message_id, pane_id)
            except Exception as e:
                log.warning("Failed to send completion notification: %s", scrub(e))
        prev_statuses[pane_id] = new_status


async def relay_listener(app: Application):
    """Persistent WebSocket connection to relay."""
    import websockets
    global agents, relay_connected, prev_statuses

    while True:
        try:
            async with websockets.connect(RELAY_WS) as ws:
                relay_connected = True
                log.info(f"Connected to relay at {RELAY_WS_SAFE}")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("type") == "agents":
                        new_agents = msg.get("agents", [])
                        blocked_panes = {
                            agent.get("pane_id") for agent in new_agents
                            if agent.get("status") == "blocked"
                        }
                        for pane_id in list(approval_tokens):
                            if pane_id not in blocked_panes:
                                approval_tokens.pop(pane_id, None)
                                blocked_prompt_ids.pop(pane_id, None)
                        await track_agent_updates(app, new_agents)
                        agents = apply_agent_message(agents, msg)

                    elif msg.get("type") == "agent_update":
                        updated_agent = msg.get("agent") or {}
                        if updated_agent.get("pane_id"):
                            await track_agent_updates(app, [updated_agent])
                            agents = apply_agent_message(agents, msg)

                    elif msg.get("type") == "blocked":
                        await notify_blocked_safely(app, msg)
                clear_relay_connection_state()
        except Exception as e:
            clear_relay_connection_state()
            log.warning(f"Relay connection lost: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)


# --- Main ---

def main():
    app = (
        Application.builder()
        .token(TOKEN)
        .connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
        .get_updates_connect_timeout(TELEGRAM_CONNECT_TIMEOUT)
        .build()
    )
    if CHAT_ID:
        auth_filter = filters.Chat(chat_id=int(CHAT_ID))
    else:
        auth_filter = filters.ALL  # No restriction in discovery mode

    app.add_handler(CommandHandler("start", cmd_start, filters=auth_filter))
    if SPAWN_CMD:
        # block=False: a spawn can take minutes, and approval taps must not queue behind it.
        app.add_handler(CommandHandler("run", cmd_run, filters=auth_filter, block=False))
        app.add_handler(CommandHandler("job", cmd_run, filters=auth_filter, block=False))
    app.add_handler(CommandHandler("agents", cmd_agents, filters=auth_filter))
    app.add_handler(CommandHandler("status", cmd_status, filters=auth_filter))
    app.add_handler(CommandHandler("read", cmd_read, filters=auth_filter))
    app.add_handler(CommandHandler("reply", cmd_reply, filters=auth_filter))
    app.add_handler(CommandHandler("send", cmd_send, filters=auth_filter))
    app.add_handler(CommandHandler("trust", cmd_trust, filters=auth_filter))
    app.add_handler(CommandHandler("digest", cmd_digest, filters=auth_filter))
    app.add_handler(CommandHandler(["interrupt", "esc"], cmd_interrupt, filters=auth_filter))
    app.add_handler(CommandHandler("help", cmd_help, filters=auth_filter))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & auth_filter, handle_text))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def run():
        async with app:
            await app.start()
            try:
                # Keep Telegram's command menu in step with the handlers above, so it never
                # offers a command this bot does not answer.
                await app.bot.set_my_commands(bot_commands())
            except Exception as e:
                log.warning("Failed to set the command menu: %s", scrub(e))
            await app.updater.start_polling()
            await relay_listener(app)

    loop.run_until_complete(run())


if __name__ == "__main__":
    main()
