import asyncio
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock
import uuid


TELEGRAM_PATH = Path(__file__).resolve().parents[1] / "relay" / "herdr_telegram.py"


def _telegram_stubs():
    telegram = types.ModuleType("telegram")

    class InlineKeyboardButton:
        def __init__(self, text, callback_data=None):
            self.text = text
            self.callback_data = callback_data

    class InlineKeyboardMarkup:
        def __init__(self, inline_keyboard):
            self.inline_keyboard = inline_keyboard

    class ForceReply:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    telegram.Update = object
    telegram.ForceReply = ForceReply
    telegram.InlineKeyboardButton = InlineKeyboardButton
    telegram.InlineKeyboardMarkup = InlineKeyboardMarkup

    ext = types.ModuleType("telegram.ext")
    ext.Application = object
    ext.CallbackQueryHandler = object
    ext.CommandHandler = object
    ext.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
    ext.MessageHandler = object
    ext.filters = types.SimpleNamespace()
    return {"telegram": telegram, "telegram.ext": ext}


def loaded_telegram():
    module_name = f"herdr_telegram_test_{uuid.uuid4().hex}"
    with mock.patch.dict(os.environ, {"HERDR_TG_TOKEN": "test"}, clear=False), mock.patch.dict(
        sys.modules, _telegram_stubs(), clear=False
    ), mock.patch.object(sys, "path", [str(TELEGRAM_PATH.parent), *sys.path]):
        spec = importlib.util.spec_from_file_location(module_name, TELEGRAM_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module


class _Bot:
    def __init__(self):
        self.sent = []

    async def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return types.SimpleNamespace(message_id=42)


class TelegramQuestionTests(unittest.TestCase):
    def test_multi_select_notification_only_offers_safe_output_link(self):
        telegram = loaded_telegram()
        telegram.CHAT_ID = "1"
        bot = _Bot()
        app = types.SimpleNamespace(bot=bot)

        asyncio.run(
            telegram.notify_blocked(
                app,
                pane_id="pane-1",
                agent="omp",
                project="demo",
                prompt="Which capabilities?",
                options=["Color", "Font"],
                multi=True,
            )
        )

        markup = bot.sent[0]["reply_markup"]
        self.assertEqual(
            [button.text for row in markup.inline_keyboard for button in row],
            ["Open output & reply"],
        )
        self.assertIn("web terminal or manual terminal controls", bot.sent[0]["text"])
        self.assertEqual(telegram.pending[(1, 42)], "pane-1")


class _Message:
    def __init__(self, text):
        self.text = text
        self.replies = []

    async def reply_text(self, text, **kwargs):
        self.replies.append(text)


def _command(text, chat_id=1):
    message = _Message(text)
    update = types.SimpleNamespace(message=message, effective_chat=types.SimpleNamespace(id=chat_id))
    ctx = types.SimpleNamespace(args=text.split()[1:])
    return update, ctx, message


class TelegramSpawnCommandTests(unittest.TestCase):
    """The opt-in HERDR_TG_SPAWN_CMD hook behind /run <alias> and /start <alias>."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.argv_log = self.tmp / "argv"
        self.telegram = loaded_telegram()
        self.telegram.CHAT_ID = "1"

    def _spawn_cmd(self, body):
        script = self.tmp / "spawn.sh"
        script.write_text(f'#!/bin/sh\nprintf "%s\\n" "$@" > "{self.argv_log}"\n{body}\n')
        script.chmod(0o755)
        self.telegram.SPAWN_CMD = str(script)

    def _argv(self):
        return self.argv_log.read_text().splitlines() if self.argv_log.exists() else None

    def test_run_passes_chat_and_command_as_argv_and_replies_with_stdout(self):
        self._spawn_cmd('echo "Started nightly in w1:p2"')
        update, ctx, message = _command("/run nightly")

        asyncio.run(self.telegram.cmd_run(update, ctx))

        self.assertEqual(self._argv(), ["--chat-id", "1", "--text", "/run nightly"])
        self.assertEqual(message.replies, ["Started nightly in w1:p2"])

    def test_bot_name_suffix_is_dropped_before_the_command(self):
        self._spawn_cmd("echo ok")
        update, ctx, _ = _command("/run@herdr_bot nightly")

        asyncio.run(self.telegram.cmd_run(update, ctx))

        self.assertEqual(self._argv()[-1], "/run nightly")

    def test_start_with_an_alias_spawns(self):
        self._spawn_cmd("echo ok")
        update, ctx, _ = _command("/start nightly")

        asyncio.run(self.telegram.cmd_start(update, ctx))

        self.assertEqual(self._argv()[-1], "/start nightly")

    def test_bare_start_still_shows_the_dashboard(self):
        self._spawn_cmd("echo ok")
        update, ctx, message = _command("/start")

        asyncio.run(self.telegram.cmd_start(update, ctx))

        self.assertIsNone(self._argv(), "bare /start must not run the spawn command")
        self.assertEqual(len(message.replies), 1)

    def test_failed_command_without_output_reports_its_stderr(self):
        self._spawn_cmd('echo "telegram_spawn is not configured" >&2; exit 1')
        update, ctx, message = _command("/run nightly")

        asyncio.run(self.telegram.cmd_run(update, ctx))

        self.assertEqual(message.replies, ["Spawn failed (exit 1): telegram_spawn is not configured"])

    def test_unauthorized_chat_never_runs_the_command(self):
        self._spawn_cmd("echo ok")
        update, ctx, message = _command("/run nightly", chat_id=2)

        asyncio.run(self.telegram.cmd_run(update, ctx))

        self.assertIsNone(self._argv())
        self.assertEqual(message.replies, [])


if __name__ == "__main__":
    unittest.main()
