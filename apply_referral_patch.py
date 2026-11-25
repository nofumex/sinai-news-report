#!/usr/bin/env python3
"""Apply the referral-report integration to the existing sinai-news-report checkout.

Run from the repository root (or copy this script into that root and run it).
It validates every anchor before writing anything and is safe to rerun only after
restoring main.py; it intentionally refuses an already patched file.
"""
from __future__ import annotations

import py_compile
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MAIN = ROOT / "main.py"
ENV_EXAMPLE = ROOT / ".env.example"
README = ROOT / "README.md"


def main() -> None:
    if not MAIN.is_file():
        raise SystemExit("main.py не найден. Поместите скрипт в корень sinai-news-report.")
    source = MAIN.read_text(encoding="utf-8")
    if "from referral_control import ReferralControl" in source:
        raise SystemExit("main.py уже содержит интеграцию referral_control; повторно патчить нельзя.")

    changes: list[tuple[str, str, str]] = []

    def add(label: str, old: str, new: str) -> None:
        count = source.count(old)
        if count != 1:
            raise SystemExit(f"Неоднозначный/не найденный блок {label}: совпадений {count}. Файлы не изменены.")
        changes.append((label, old, new))

    add("import", "from dotenv import load_dotenv\n", "from dotenv import load_dotenv\nfrom referral_control import ReferralControl\n")
    add(
        "manager attribution",
        """def resolve_lead_manager(lead: dict, users: dict[int, str], field_id: int = 0) -> tuple[str | None, str, str]:
    \"\"\"Owner whitelist wins; otherwise use field Manager, never fall back to other owners.\"\"\"
    owner = users.get(int(lead.get('responsible_user_id') or 0), '')
    match = resolve_manager(owner)
    brief = field_manager(lead, field_id)
    if match:
        return match, 'ответственный', owner
    manager = resolve_manager(brief)
    return manager, ('Бриф → Менеджер' if manager else 'не определён'), (brief or owner or 'пусто')
""",
        """def resolve_lead_manager(lead: dict, users: dict[int, str], field_id: int = 0) -> tuple[str | None, str, str]:
    \"\"\"Use the configured Manager field first, then the deal's responsible user.\"\"\"
    owner = users.get(int(lead.get('responsible_user_id') or 0), '')
    brief = field_manager(lead, field_id)
    manager = resolve_manager(brief)
    if manager:
        return manager, 'поле Менеджер', brief
    match = resolve_manager(owner)
    return match, ('ответственный' if match else 'не определён'), (owner or brief or 'пусто')
""",
    )
    add("manager field default", "manager_field_id=int(os.getenv('MANAGER_FIELD_ID', '0'))", "manager_field_id=int(os.getenv('MANAGER_FIELD_ID', '518097'))")
    add(
        "ReportService referral slot",
        """        self.cfg, self.state, self.tg = cfg, state, tg
        self.api = Api(cfg)
""",
        """        self.cfg, self.state, self.tg = cfg, state, tg
        self.referrals: ReferralControl | None = None
        self.api = Api(cfg)
""",
    )
    add(
        "daily report footer",
        """        lines.extend(['', f'👥 <b>Итого: {total}</b> · +{daily} за день'])
        if stale:
""",
        """        lines.extend(['', f'👥 <b>Итого: {total}</b> · +{daily} за день'])
        if self.referrals:
            lines.extend(['', *self.referrals.daily_lines(day)])
        if stale:
""",
    )
    add(
        "App initialization",
        """        self.tg = Telegram(cfg)
        self.svc = ReportService(cfg, self.state, self.tg)
        self.restored_daily: set[tuple[date, int, int]] = set()
""",
        """        self.tg = Telegram(cfg)
        self.svc = ReportService(cfg, self.state, self.tg)
        self.referrals = ReferralControl(cfg, self.state, self.tg, self.svc.api)
        self.svc.referrals = self.referrals
        self.restored_daily: set[tuple[date, int, int]] = set()
""",
    )
    add(
        "group message intake",
        """        msg = (callback or {}).get('message') or update.get('message') or {}
        chat_obj = msg.get('chat') or {}
        chat = int(chat_obj.get('id', 0))
""",
        """        msg = (callback or {}).get('message') or update.get('message') or update.get('edited_message') or {}
        chat_obj = msg.get('chat') or {}
        chat = int(chat_obj.get('id', 0))
        if not callback and self.referrals.handle_message(msg):
            return
""",
    )
    add(
        "monthly callbacks",
        """            elif payload == 'status':
                self.tg.edit(chat, mid, self.svc.status_text(),
                             {'inline_keyboard': [[{'text': '🏠 Меню', 'callback_data': 'home'}]]})
            elif payload.startswith('m:'):
""",
        """            elif payload == 'status':
                self.tg.edit(chat, mid, self.svc.status_text(),
                             {'inline_keyboard': [[{'text': '🏠 Меню', 'callback_data': 'home'}]]})
            elif payload.startswith('month:'):
                month = payload.split(':', 1)[1]
                try:
                    datetime.strptime(month, '%Y-%m')
                    self.tg.edit(chat, mid, self.referrals.monthly_text(month),
                                 self.referrals.monthly_markup(month))
                except ValueError:
                    self.tg.edit(chat, mid, 'Некорректный месяц.',
                                 {'inline_keyboard': [[{'text': '🏠 Меню', 'callback_data': 'home'}]]})
            elif payload.startswith('pen:'):
                try:
                    _, month, idx, page = payload.split(':')
                    datetime.strptime(month, '%Y-%m')
                    screen, buttons = self.referrals.manager_page(month, int(idx), int(page))
                    self.tg.edit(chat, mid, screen, buttons)
                except (ValueError, KeyError):
                    self.tg.edit(chat, mid, 'Некорректные данные. Вернитесь к месячному отчёту.',
                                 {'inline_keyboard': [[{'text': '🏠 Меню', 'callback_data': 'home'}]]})
            elif payload.startswith('m:'):
""",
    )
    add(
        "daily sync before delivery",
        """                        try:
                            mid = self.tg.send(
                                chat,
                                self.svc.format_report(day, self.state.snapshot(day)),
                                report_markup(day, delivery_date)
                            )
""",
        """                        try:
                            # Refresh only filtered status-change events; CRM failure must not block subscription reporting.
                            self.referrals.sync()
                        except Exception:
                            LOG.exception('Daily referral sync failed; using saved data')

                        try:
                            mid = self.tg.send(
                                chat,
                                self.svc.format_report(day, self.state.snapshot(day)),
                                report_markup(day, delivery_date)
                            )
""",
    )
    add(
        "monthly scheduler",
        """                        self.state.mark_daily(delivery_date, chat, 'delivered', mid)

                # Restore pending daily placeholders for managers only.
""",
        """                        self.state.mark_daily(delivery_date, chat, 'delivered', mid)

                month = self.referrals.monthly_due(now)
                if month:
                    try:
                        self.referrals.sync()
                    except Exception:
                        LOG.exception('Monthly referral sync failed; using saved data')
                    for chat in sorted(manager_ids):
                        if not self.referrals.claim_monthly(month, chat):
                            continue
                        try:
                            mid = self.tg.send(chat, self.referrals.monthly_text(month),
                                               self.referrals.monthly_markup(month))
                        except Exception:
                            self.referrals.mark_monthly(month, chat, 'unknown')
                            LOG.exception('Monthly Telegram send uncertain: month=%s chat=%s', month, chat)
                        else:
                            self.referrals.mark_monthly(month, chat, 'delivered', mid)

                # Restore pending daily placeholders for managers only.
""",
    )
    add(
        "edited messages update type",
        "allowed_updates=['message', 'callback_query']",
        "allowed_updates=['message', 'edited_message', 'callback_query']",
    )
    add(
        "referral background thread",
        """        threading.Thread(target=self.svc.work, daemon=True, name='crm-reports').start()
        threading.Thread(target=self.daily_loop, daemon=True, name='daily-scheduler').start()
        threading.Thread(target=self.precompute_loop, daemon=True, name='report-precompute').start()
        offset = self.state.offset()
""",
        """        threading.Thread(target=self.svc.work, daemon=True, name='crm-reports').start()
        threading.Thread(target=self.daily_loop, daemon=True, name='daily-scheduler').start()
        threading.Thread(target=self.precompute_loop, daemon=True, name='report-precompute').start()
        if self.referrals.enabled:
            threading.Thread(target=self.referrals.loop, daemon=True, name='referral-control').start()
        offset = self.state.offset()
""",
    )

    # Validate every anchor and compile in memory before touching the checkout.
    patched = source
    for label, old, new in changes:
        patched = patched.replace(old, new, 1)
    compile(patched, str(MAIN), "exec")
    module_path = ROOT / "referral_control.py"
    if not module_path.is_file():
        raise SystemExit("referral_control.py отсутствует рядом с apply_referral_patch.py")
    py_compile.compile(str(module_path), doraise=True)

    backup = MAIN.with_suffix(".py.before-referral-control.bak")
    if not backup.exists():
        shutil.copy2(MAIN, backup)
    MAIN.write_text(patched, encoding="utf-8")

    if ENV_EXAMPLE.is_file():
        env = ENV_EXAMPLE.read_text(encoding="utf-8")
        env = env.replace("MANAGER_FIELD_ID=0", "MANAGER_FIELD_ID=518097")
        if "REPORTS_GROUP_CHAT_ID=" not in env:
            env += "\n# Telegram group for managers' three-line referral reports (usually -100...).\n"
            env += "REPORTS_GROUP_CHAT_ID=\nREFERRAL_POLL_SECONDS=60\nMONTHLY_REPORT_TIME=23:59\n"
        ENV_EXAMPLE.write_text(env, encoding="utf-8")

    if README.is_file():
        readme = README.read_text(encoding="utf-8")
        heading = "## Контроль отчётов по сарафану"
        if heading not in readme:
            readme += """

## Контроль отчётов по сарафану

В `config.json` заданы четыре менеджера и их amoCRM/Telegram ID, поле `Менеджер` (`518097`), целевая воронка `Отдел продаж` и этап `сарафан`. При старте интеграция находит фактические `pipeline_id` и `status_id` по этим названиям через API amoCRM и пишет оба числовых ID в лог. Можно указать их в `config.json`, если хотите зафиксировать значения.

Добавьте в `.env`:

```dotenv
REPORTS_GROUP_CHAT_ID=-100XXXXXXXXXX
REFERRAL_POLL_SECONDS=60
MONTHLY_REPORT_TIME=23:59
MANAGER_FIELD_ID=518097
```

`REPORTS_GROUP_CHAT_ID` — ID новой группы, обычно отрицательный `-100...`. У отчётного бота отключите Group Privacy в BotFather (`/setprivacy` → Disable) либо назначьте бота администратором группы, чтобы Telegram передавал ему обычные сообщения. После изменения privacy безопаснее удалить бота из группы и добавить заново. В карточке участника Telegram должно быть видно, что у бота есть доступ к сообщениям.

Бот принимает от четырёх Telegram ID из `config.json` сообщения минимум из трёх непустых строк; номер сделки должен быть первой строкой (например, `1. 32647005`). Сообщение учитывается в дате отправки в `TIMEZONE`. Отчёт должен быть отправлен тем же менеджером в ту же календарную дату, что и переход сделки в этап; порядок по времени внутри дня не важен. Отправка отчёта другим менеджером или на следующий день штраф не снимает.

Сверка использует `GET /api/v4/events` с типом `lead_status_changed` и фильтром по целевому статусу, опрашивая события раз в минуту. Никакие сделки CRM не изменяются; события, сообщения, кэш и начисления хранятся в `REPORT_STATE_PATH`, отдельно от базы основного бота. Ежедневная проверка отображается в существующем отчёте за предыдущий день в 09:00. Месячный отчёт отправляется в последний календарный день месяца в `MONTHLY_REPORT_TIME` (по умолчанию 23:59); при перезапуске на первое число месяца недоставленный итог предыдущего месяца будет отправлен как catch-up. Переходы через месяцы и високосный февраль вычисляются календарём, а не фиксированным списком дат.

Проверки:

```bash
venv/bin/python -m unittest discover -s tests -v
```
"""
            README.write_text(readme, encoding="utf-8")

    py_compile.compile(str(MAIN), doraise=True)
    py_compile.compile(str(ROOT / "referral_control.py"), doraise=True)
    print("Патч применён. Резервная копия:", backup.name)
    print("Далее задайте REPORTS_GROUP_CHAT_ID в .env и перезапустите отчётного бота.")


if __name__ == "__main__":
    main()
