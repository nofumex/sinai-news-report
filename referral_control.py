"""Background control of daily referral-stage reports in a Telegram group.

The module deliberately uses amoCRM's event feed instead of scanning all leads.
All CRM I/O is performed by the caller's background worker, never in the Telegram UI.
"""
from __future__ import annotations

import calendar
import html
import json
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

LOG = logging.getLogger("sinai-report.referrals")
EVENT_BACKFILL_VERSION = "4"
USER_REPORT_RULES_VERSION = "1"
MONTHS_RU = (
    "январь", "февраль", "март", "апрель", "май", "июнь",
    "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь",
)


def parse_report_deal_id(text: object) -> int | None:
    matches = re.findall(r"(?<!\d)(\d{6,12})(?!\d)", str(text or ""))
    return int(matches[0]) if matches else None


def parse_clock(value: str, default: str) -> tuple[int, int]:
    raw = (value or default).strip()
    match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", raw)
    if not match:
        raise ValueError(f"Некорректное время: {raw!r}; ожидается ЧЧ:ММ")
    return int(match.group(1)), int(match.group(2))


def month_key(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def month_bounds(month: str) -> tuple[date, date]:
    year, number = (int(part) for part in month.split("-", 1))
    last = calendar.monthrange(year, number)[1]
    return date(year, number, 1), date(year, number, last)


def _normal(value: object) -> str:
    return " ".join(str(value or "").casefold().replace("ё", "е").split())


class ReferralControl:
    def __init__(self, cfg: Any, state: Any, telegram: Any, api: Any):
        self.cfg, self.state, self.tg, self.api = cfg, state, telegram, api
        raw = json.loads((Path(__file__).resolve().parent / "config.json").read_text(encoding="utf-8"))
        self.managers = raw["managers"]
        if len(self.managers) != 4:
            raise ValueError("config.json: ожидаются ровно четыре менеджера")
        self.manager_by_tg = {int(m["telegram_id"]): m for m in self.managers}
        self.manager_by_amo = {int(m["amo_id"]): m for m in self.managers}
        if len(self.manager_by_tg) != 4 or len(self.manager_by_amo) != 4:
            raise ValueError("config.json: Telegram ID и amoCRM ID менеджеров должны быть уникальны")
        self.manager_field_id = int(raw.get("manager_field_id", 518097))
        self.stage = raw.get("referral_stage", {})
        self.pipeline_name = str(self.stage.get("pipeline_name", "Отдел продаж"))
        self.status_name = str(self.stage.get("status_name", "сарафан"))
        self.pipeline_id = int(self.stage.get("pipeline_id") or 0)
        self.status_id = int(self.stage.get("status_id") or 0)
        self.penalty = int(self.stage.get("penalty_rub", 500))
        self.page_size = int(self.stage.get("page_size", 5))
        if self.penalty < 0 or not 1 <= self.page_size <= 10:
            raise ValueError("config.json: проверьте penalty_rub/page_size")
        self.group_chat_id = int(os.getenv("REPORTS_GROUP_CHAT_ID", "0") or 0)
        self.poll_seconds = max(30, int(os.getenv("REFERRAL_POLL_SECONDS", self.stage.get("poll_seconds", 60))))
        self.monthly_hour, self.monthly_minute = parse_clock(
            os.getenv("MONTHLY_REPORT_TIME", str(self.stage.get("monthly_report_time", "23:59"))), "23:59")
        self.enabled = bool(self.group_chat_id)
        self._sync_lock = threading.Lock()
        self._target_ready = False
        self.last_sync: float | None = None
        self.last_error = ""
        with self.state.mu, self.state.db:
            self.state.db.executescript("""
                CREATE TABLE IF NOT EXISTS referral_meta (
                    key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS referral_events (
                    event_id TEXT PRIMARY KEY, lead_id INTEGER NOT NULL,
                    event_at REAL NOT NULL, report_day TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS referral_entries (
                    lead_id INTEGER NOT NULL, report_day TEXT NOT NULL,
                    event_at REAL NOT NULL, manager_name TEXT, manager_amo_id INTEGER,
                    manager_tg_id INTEGER,
                    PRIMARY KEY (lead_id, report_day)
                );
                CREATE TABLE IF NOT EXISTS referral_reports (
                    chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                    report_day TEXT NOT NULL, lead_id INTEGER NOT NULL,
                    reporter_tg_id INTEGER NOT NULL, submitted_at REAL NOT NULL,
                    message_text TEXT NOT NULL,
                    deal_day TEXT, submitted_day TEXT,
                    is_late INTEGER NOT NULL DEFAULT 0,
                    manager_name TEXT, manager_amo_id INTEGER, manager_tg_id INTEGER,
                    PRIMARY KEY (chat_id, message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_referral_entries_day
                    ON referral_entries(report_day, manager_name);
                CREATE INDEX IF NOT EXISTS idx_referral_reports_lookup
                    ON referral_reports(report_day, lead_id, reporter_tg_id, submitted_at);
                CREATE TABLE IF NOT EXISTS referral_monthly_delivery (
                    report_month TEXT NOT NULL, chat_id INTEGER NOT NULL,
                    status TEXT NOT NULL, message_id INTEGER,
                    created_at REAL NOT NULL,
                    PRIMARY KEY (report_month, chat_id)
                );
            """)
            cols = {r[1] for r in self.state.db.execute(
                "PRAGMA table_info(referral_reports)"
            )}
            for col, definition in (
                ("deal_day", "TEXT"),
                ("submitted_day", "TEXT"),
                ("is_late", "INTEGER NOT NULL DEFAULT 0"),
                ("manager_name", "TEXT"),
                ("manager_amo_id", "INTEGER"),
                ("manager_tg_id", "INTEGER"),
            ):
                if col not in cols:
                    self.state.db.execute(
                        f"ALTER TABLE referral_reports ADD COLUMN {col} {definition}"
                    )

            self.state.db.execute(
                "UPDATE referral_reports SET submitted_day=COALESCE(submitted_day,report_day)"
            )
            self.state.db.execute("""
                UPDATE referral_reports SET
                    deal_day=COALESCE(deal_day,(
                        SELECT e.report_day FROM referral_entries e
                        WHERE e.lead_id=referral_reports.lead_id
                        ORDER BY e.event_at LIMIT 1
                    )),
                    manager_name=COALESCE(manager_name,(
                        SELECT e.manager_name FROM referral_entries e
                        WHERE e.lead_id=referral_reports.lead_id
                        ORDER BY e.event_at LIMIT 1
                    )),
                    manager_amo_id=COALESCE(manager_amo_id,(
                        SELECT e.manager_amo_id FROM referral_entries e
                        WHERE e.lead_id=referral_reports.lead_id
                        ORDER BY e.event_at LIMIT 1
                    )),
                    manager_tg_id=COALESCE(manager_tg_id,(
                        SELECT e.manager_tg_id FROM referral_entries e
                        WHERE e.lead_id=referral_reports.lead_id
                        ORDER BY e.event_at LIMIT 1
                    ))
            """)
            self.state.db.execute("""
                UPDATE referral_reports SET is_late=CASE
                    WHEN deal_day IS NOT NULL AND submitted_day>deal_day
                    THEN 1 ELSE 0 END
            """)
            self.state.db.execute(
                "CREATE INDEX IF NOT EXISTS idx_referral_reports_lead "
                "ON referral_reports(lead_id,submitted_at)"
            )
        if not self.enabled:
            LOG.warning("Контроль сарафана отключён: задайте REPORTS_GROUP_CHAT_ID в .env")
        else:
            LOG.info("Контроль сарафана включён: group_chat_id=%s, polling=%ss",
                     self.group_chat_id, self.poll_seconds)

    def _meta(self, key: str) -> str | None:
        with self.state.mu:
            row = self.state.db.execute("SELECT value FROM referral_meta WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else None

    def _set_meta(self, key: str, value: object) -> None:
        with self.state.mu, self.state.db:
            self.state.db.execute(
                "INSERT INTO referral_meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

    def _target_stage(self) -> tuple[int, int]:
        if self._target_ready:
            return self.pipeline_id, self.status_id
        pipelines: list[dict] = []
        page = 1
        while True:
            data = self.api.crm("/api/v4/leads/pipelines", {"page": page, "limit": 250})
            pipelines.extend(data.get("_embedded", {}).get("pipelines", []))
            if not data.get("_links", {}).get("next"):
                break
            page += 1
        if self.pipeline_id:
            matches = [p for p in pipelines if int(p.get("id", 0)) == self.pipeline_id]
        else:
            matches = [p for p in pipelines if _normal(p.get("name")) == _normal(self.pipeline_name)]
        if len(matches) != 1:
            raise RuntimeError(
                f"Не удалось однозначно найти воронку {self.pipeline_name!r} в amoCRM "
                f"(совпадений: {len(matches)}). Укажите referral_stage.pipeline_id в config.json.")
        pipeline = matches[0]
        self.pipeline_id = int(pipeline["id"])
        statuses = pipeline.get("_embedded", {}).get("statuses", [])
        if not statuses:
            data = self.api.crm(f"/api/v4/leads/pipelines/{self.pipeline_id}/statuses")
            statuses = data.get("_embedded", {}).get("statuses", [])
        if self.status_id:
            status_matches = [s for s in statuses if int(s.get("id", 0)) == self.status_id]
        else:
            status_matches = [s for s in statuses if _normal(s.get("name")) == _normal(self.status_name)]
        if len(status_matches) != 1:
            raise RuntimeError(
                f"Не удалось однозначно найти этап {self.status_name!r} в воронке "
                f"{self.pipeline_name!r} (совпадений: {len(status_matches)}). "
                "Укажите referral_stage.status_id в config.json.")
        self.status_id = int(status_matches[0]["id"])
        self._target_ready = True
        LOG.info("amoCRM target stage resolved: pipeline=%r pipeline_id=%s status=%r status_id=%s",
                 pipeline.get("name"), self.pipeline_id, status_matches[0].get("name"), self.status_id)
        return self.pipeline_id, self.status_id

    def _initial_backfill_cursor(self, now: int) -> int:
        """Start at the first day of the previous month, including the current month.

        This also repairs installations created by the earlier buggy version, which
        persisted a cursor near startup and silently skipped pre-install events.
        """
        local_date = datetime.fromtimestamp(now, self.cfg.tz).date()
        previous_month_last_day = local_date.replace(day=1) - timedelta(days=1)
        first_day = previous_month_last_day.replace(day=1)
        return int(datetime.combine(first_day, datetime.min.time(), self.cfg.tz).timestamp())

    def _target_events(self, cursor: int) -> list[dict]:
        pipeline_id, status_id = self._target_stage()
        params = {
            "limit": 100,
            "page": 1,
            "filter[entity]": "lead",
            "filter[type]": "lead_status_changed",
            "filter[created_at][from]": max(0, cursor - 300),
            "filter[value_after][leads_statuses][0][pipeline_id]": pipeline_id,
            "filter[value_after][leads_statuses][0][status_id]": status_id,
        }
        found: list[dict] = []
        page = 1
        while True:
            params["page"] = page
            data = self.api.crm("/api/v4/events", params)
            found.extend(data.get("_embedded", {}).get("events", []))
            if not data.get("_links", {}).get("next"):
                break
            page += 1
        return found

    def _manager_for_lead(self, lead: dict | None) -> dict | None:
        """Manager field 518097 has priority; amoCRM responsible is fallback."""
        if not lead:
            return None

        manager_ids = {
            "миллер": 8476783,
            "дегтярева": 7074220,
            "шевелева": 3298921,
            "карепов": 2328073,
            "павел": 2328073,
        }

        for field in lead.get("custom_fields_values") or []:
            try:
                if int(field.get("field_id") or 0) != 518097:
                    continue
            except (TypeError, ValueError):
                continue

            for item in field.get("values") or []:
                value = item.get("value", "")
                if isinstance(value, dict):
                    value = value.get("name") or value.get("value") or ""

                normalized = str(value).strip().casefold().replace("ё", "е")
                if not normalized:
                    continue

                parts = re.split(r"\s*[-–—]\s*", normalized, maxsplit=1)
                primary = parts[0] if len(parts) > 1 else normalized
                tokens = set(re.findall(r"[а-яa-z]+", primary))

                amo_id = next(
                    (mid for surname, mid in manager_ids.items()
                     if surname in tokens),
                    None
                )

                if amo_id is not None:
                    manager = (
                        self.manager_by_amo.get(amo_id)
                        or self.manager_by_amo.get(str(amo_id))
                    )
                    if manager:
                        return manager

        try:
            responsible_id = int(lead.get("responsible_user_id") or 0)
        except (TypeError, ValueError):
            return None

        return (
            self.manager_by_amo.get(responsible_id)
            or self.manager_by_amo.get(str(responsible_id))
        )
    def _link_reports_to_entry(self, lead_id: int, report_day: str,
                               manager: dict | None) -> None:
        with self.state.mu, self.state.db:
            self.state.db.execute(
                "UPDATE referral_reports SET deal_day=?, manager_name=?, "
                "manager_amo_id=?, manager_tg_id=?, "
                "is_late=CASE WHEN COALESCE(submitted_day,report_day)>? "
                "THEN 1 ELSE 0 END WHERE lead_id=?",
                (report_day,
                 manager.get("name") if manager else None,
                 int(manager["amo_id"]) if manager else None,
                 int(manager["telegram_id"]) if manager else None,
                 report_day, lead_id)
            )


    def sync(self) -> int:
        """Fetch only matching stage-change events since the persisted cursor."""
        if not self.enabled:
            return 0
        self._sync_lock.acquire()
        try:
            now = int(time.time())
            cursor_raw = self._meta("amo_event_cursor")
            backfill_version = self._meta("amo_event_backfill_version")
            if backfill_version != EVENT_BACKFILL_VERSION:
                cursor = self._initial_backfill_cursor(now)
                LOG.warning(
                    "Выполняется первичная/восстановительная загрузка событий amoCRM с %s",
                    datetime.fromtimestamp(cursor, self.cfg.tz).strftime("%d.%m.%Y %H:%M"),
                )
            else:
                cursor = int(cursor_raw) if cursor_raw else now - 300
            events = self._target_events(cursor)
            newest = cursor
            inserted = 0
            for event in sorted(events, key=lambda e: (int(e.get("created_at", 0)), str(e.get("id", "")))):
                event_id = str(event.get("id") or "")
                lead_id = int(event.get("entity_id") or 0)
                event_at = int(event.get("created_at") or 0)
                after = (event.get("value_after") or [{}])[0].get("lead_status") or {}
                if (not event_id or not lead_id or not event_at
                        or int(after.get("pipeline_id") or 0) != self.pipeline_id
                        or int(after.get("id") or 0) != self.status_id):
                    continue
                newest = max(newest, event_at)
                with self.state.mu:
                    exists = self.state.db.execute(
                        "SELECT 1 FROM referral_events WHERE event_id=?", (event_id,)).fetchone()
                try:
                    lead = self.api.crm(f"/api/v4/leads/{lead_id}")
                except LookupError:
                    lead = None
                manager = self._manager_for_lead(lead)
                report_day = datetime.fromtimestamp(event_at, self.cfg.tz).date().isoformat()
                if exists:
                    with self.state.mu, self.state.db:
                        self.state.db.execute(
                            "UPDATE referral_entries SET manager_name=?, manager_amo_id=?, "
                            "manager_tg_id=? WHERE lead_id=? AND report_day=?",
                            (manager.get("name") if manager else None,
                             int(manager["amo_id"]) if manager else None,
                             int(manager["telegram_id"]) if manager else None,
                             lead_id, report_day)
                        )
                    self._link_reports_to_entry(lead_id, report_day, manager)
                    continue
                with self.state.mu, self.state.db:
                    cur = self.state.db.execute(
                        "INSERT OR IGNORE INTO referral_events(event_id,lead_id,event_at,report_day) VALUES(?,?,?,?)",
                        (event_id, lead_id, event_at, report_day))
                    if cur.rowcount:
                        self.state.db.execute(
                            "INSERT OR IGNORE INTO referral_entries"
                            "(lead_id,report_day,event_at,manager_name,manager_amo_id,manager_tg_id) "
                            "VALUES(?,?,?,?,?,?)",
                            (lead_id, report_day, event_at,
                             manager.get("name") if manager else None,
                             int(manager["amo_id"]) if manager else None,
                             int(manager["telegram_id"]) if manager else None))
                        inserted += 1
                self._link_reports_to_entry(lead_id, report_day, manager)
                if manager is None:
                    LOG.warning("Не определён менеджер для сделки %s из события %s", lead_id, event_id)
            # Keep a small overlap for events that arrive late or share the same second; event IDs deduplicate it.
            self._set_meta("amo_event_cursor", max(cursor, now - 300, newest - 300))
            # Only mark history as initialized after the full paginated API request succeeded.
            self._set_meta("amo_event_backfill_version", EVENT_BACKFILL_VERSION)
            self.last_sync = time.time()
            self.last_error = ""
            if inserted:
                LOG.info("Referral stage events imported: %s", inserted)
            return inserted
        except Exception as exc:
            self.last_error = str(exc)
            LOG.exception("Referral events sync failed")
            raise
        finally:
            self._sync_lock.release()

    def loop(self) -> None:
        while True:
            try:
                self.sync()
            except Exception:
                pass
            time.sleep(self.poll_seconds)

    def handle_message(self, message: dict) -> bool:
        chat = message.get("chat") or {}
        chat_id = int(chat.get("id") or 0)

        if chat.get("type") not in ("group", "supergroup"):
            return False
        if not self.group_chat_id or chat_id != self.group_chat_id:
            return False

        sender = message.get("from") or {}
        sender_id = int(sender.get("id") or 0)
        sender_chat = message.get("sender_chat") or {}
        body = message.get("text") or message.get("caption") or ""
        deal_id = parse_report_deal_id(body)

        if not deal_id:
            LOG.info("Referral message ignored: deal ID not found, message_id=%s",
                     message.get("message_id"))
            return True

        stamp = int(message.get("date") or message.get("edit_date") or time.time())
        submitted_day = datetime.fromtimestamp(
            stamp, self.cfg.tz
        ).date().isoformat()

        with self.state.mu, self.state.db:
            entry = self.state.db.execute(
                "SELECT report_day,manager_name,manager_amo_id,manager_tg_id "
                "FROM referral_entries WHERE lead_id=? ORDER BY event_at LIMIT 1",
                (deal_id,)
            ).fetchone()

            deal_day = entry["report_day"] if entry else None
            manager_name = entry["manager_name"] if entry else None
            manager_amo_id = entry["manager_amo_id"] if entry else None
            manager_tg_id = entry["manager_tg_id"] if entry else None
            is_late = int(bool(deal_day and submitted_day > deal_day))

            self.state.db.execute(
                "INSERT INTO referral_reports("
                "chat_id,message_id,report_day,lead_id,reporter_tg_id,"
                "submitted_at,message_text,deal_day,submitted_day,is_late,"
                "manager_name,manager_amo_id,manager_tg_id"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(chat_id,message_id) DO UPDATE SET "
                "report_day=excluded.report_day,lead_id=excluded.lead_id,"
                "reporter_tg_id=excluded.reporter_tg_id,submitted_at=excluded.submitted_at,"
                "message_text=excluded.message_text,deal_day=excluded.deal_day,"
                "submitted_day=excluded.submitted_day,is_late=excluded.is_late,"
                "manager_name=excluded.manager_name,manager_amo_id=excluded.manager_amo_id,"
                "manager_tg_id=excluded.manager_tg_id",
                (chat_id, int(message.get("message_id") or 0), submitted_day,
                 deal_id, sender_id, stamp, str(body), deal_day, submitted_day,
                 is_late, manager_name, manager_amo_id, manager_tg_id)
            )

        LOG.info(
            "Referral report saved: deal=%s responsible_manager=%s sender_id=%s "
            "sender_chat_id=%s deal_day=%s submitted_day=%s late=%s",
            deal_id, manager_name or "not-yet-resolved", sender_id,
            sender_chat.get("id"), deal_day or "not-yet-synced",
            submitted_day, bool(is_late)
        )
        return True


    def _missing_for_range(self, start: date, end: date) -> tuple[dict[str, list[int]], list[int], int]:
        missing = {m["name"]: [] for m in self.managers}
        unassigned: list[int] = []
        with self.state.mu:
            rows = self.state.db.execute(
                "SELECT lead_id,report_day,event_at,manager_name,manager_tg_id FROM referral_entries "
                "WHERE report_day>=? AND report_day<=? ORDER BY report_day,event_at,lead_id",
                (start.isoformat(), end.isoformat())).fetchall()
            for row in rows:
                if not row["manager_name"] or not row["manager_tg_id"]:
                    unassigned.append(int(row["lead_id"]))
                    continue
                # The business rule is calendar-day based: a same-day report counts
                # even if the manager posted it shortly before the CRM stage change.
                reported = self.state.db.execute(
                    "SELECT 1 FROM referral_reports WHERE lead_id=? LIMIT 1",
                    (int(row["lead_id"]),)
                ).fetchone()
                if not reported:
                    missing[row["manager_name"]].append(int(row["lead_id"]))
        for ids in missing.values():
            ids[:] = list(dict.fromkeys(ids))
        return missing, list(dict.fromkeys(unassigned)), len(rows)

    def daily_lines(self, day: date) -> list[str]:
        label = f"<b>{day:%d.%m.%Y}</b>"
        stage = f"<b>«{html.escape(self.pipeline_name)} / {html.escape(self.status_name)}»</b>"
        if not self.enabled:
            return ["⚠️ Контроль отчётов по сарафану отключён: задайте REPORTS_GROUP_CHAT_ID в .env."]
        if self._meta("amo_event_backfill_version") != EVENT_BACKFILL_VERSION:
            return [
                "⚠️ <b>Сверка с историей amoCRM ещё не завершена.</b>",
                "Бот не подтверждает отсутствие пропусков, пока не загрузит историю переходов в этап.",
            ]
        missing, unassigned, total = self._missing_for_range(day, day)
        lines: list[str] = []
        for manager in self.managers:
            name = manager["name"]
            ids = missing[name]
            if not ids:
                continue
            lines.append(f"📄 <b>{html.escape(name)}</b> — не предоставлен отчёт по {len(ids)} сделкам")
            for lead_id in ids:
                url = f"{self.cfg.amo_url}/leads/detail/{lead_id}"
                lines.append(f"- <a href=\"{html.escape(url, quote=True)}\">{lead_id}</a>")
            lines.append("")
        if unassigned:
            lines.append(f"⚠️ Не удалось определить менеджера по {len(unassigned)} сделкам:")
            for lead_id in unassigned:
                url = f"{self.cfg.amo_url}/leads/detail/{lead_id}"
                lines.append(f"- <a href=\"{html.escape(url, quote=True)}\">{lead_id}</a>")
            lines.append("")
        if self.last_error:
            lines.append("⚠️ <b>Последняя сверка с amoCRM завершилась ошибкой.</b> Показаны последние сохранённые данные; полный статус отчётов не подтверждён.")
            lines.append(html.escape(self.last_error[:220]))
        elif total == 0:
            lines.append(f"ℹ️ По журналу событий amoCRM за {label} не найдено переходов сделок в {stage}.")
        elif not any(missing.values()) and not unassigned:
            lines.append(f"📄 Отчёт по всем сделкам из {stage} за {label} предоставлен.")
        else:
            lines.append(f"Отчёт по всем остальным сделкам из {stage} за {label} предоставлен.")
        return lines

    def _monthly_missing(self, month: str) -> dict[str, list[int]]:
        start, end = month_bounds(month)
        missing, _unassigned, _total = self._missing_for_range(start, end)
        return missing

    def monthly_text(self, month: str) -> str:
        year, number = (int(part) for part in month.split("-", 1))
        start, end = month_bounds(month)
        missing, unassigned, _total = self._missing_for_range(start, end)
        lines = ["📊 <b>ЮК Синай · Новости (контроль отчётов)</b>",
                 f"📅 Отчёт: <b>{MONTHS_RU[number - 1].capitalize()} {year}</b>", ""]
        for manager in self.managers:
            count = len(missing[manager["name"]])
            lines.append(
                f"📄 {html.escape(manager['name'])} — <b>пропущен отчёт по {count} сделкам, "
                f"штраф {count * self.penalty} рублей</b>")
        if unassigned:
            lines.extend(["", f"⚠️ Не удалось определить менеджера по {len(unassigned)} сделкам; "
                          "штраф по ним не начислен. Проверьте поле 518097 и ответственного:"])
            for lead_id in unassigned:
                url = f"{self.cfg.amo_url}/leads/detail/{lead_id}"
                lines.append(f'- <a href="{html.escape(url, quote=True)}">{lead_id}</a>')
        if self.last_error:
            lines.extend(["", "⚠️ Последняя сверка amoCRM завершилась ошибкой: " + html.escape(self.last_error[:180])])
        return "\n".join(lines)

    def monthly_markup(self, month: str) -> dict:
        rows = []
        for i in range(0, len(self.managers), 2):
            row = [{"text": m["name"], "callback_data": f"pen:{month}:{j}:0"}
                   for j, m in enumerate(self.managers[i:i + 2], start=i)]
            rows.append(row)
        return {"inline_keyboard": rows}

    def manager_page(self, month: str, index: int, page: int) -> tuple[str, dict]:
        if not 0 <= index < len(self.managers):
            raise ValueError("Unknown manager")
        manager = self.managers[index]
        rows = self._monthly_missing(month)[manager["name"]]
        pages = max(1, (len(rows) + self.page_size - 1) // self.page_size)
        page = max(0, min(page, pages - 1))
        lines = [f"📄 <b>{html.escape(manager['name'])}</b>",
                 f"📅 Пропущенные отчёты за <b>{month}</b>",
                 f"Штраф: <b>{len(rows) * self.penalty} рублей</b>", ""]
        if not rows:
            lines.append("Пропущенных отчётов нет.")
        buttons = []
        for lead_id in rows[page * self.page_size:(page + 1) * self.page_size]:
            url = f"{self.cfg.amo_url}/leads/detail/{lead_id}"
            buttons.append([{"text": str(lead_id), "url": url}])
        navigation = []
        if page > 0:
            navigation.append({"text": "←", "callback_data": f"pen:{month}:{index}:{page - 1}"})
        navigation.append({"text": f"{page + 1}/{pages}", "callback_data": "noop"})
        if page + 1 < pages:
            navigation.append({"text": "→", "callback_data": f"pen:{month}:{index}:{page + 1}"})
        if navigation and rows:
            buttons.append(navigation)
        buttons.append([{"text": "↩ К месячному отчёту", "callback_data": f"month:{month}"}])
        return "\n".join(lines), {"inline_keyboard": buttons}

    def monthly_due(self, now: datetime) -> str | None:
        """Run at the configured time on the actual last day; catch up after restarts on day 1."""
        # Day 1 is also a catch-up opportunity after an outage at month-end.
        if now.day == 1:
            return month_key(now.date() - timedelta(days=1))
        due = (now.hour, now.minute) >= (self.monthly_hour, self.monthly_minute)
        if not due:
            return None
        last_day = calendar.monthrange(now.year, now.month)[1]
        if now.day == last_day:
            return month_key(now.date())
        return None

    def claim_monthly(self, month: str, chat: int) -> bool:
        with self.state.mu, self.state.db:
            cur = self.state.db.execute(
                "INSERT OR IGNORE INTO referral_monthly_delivery "
                "(report_month,chat_id,status,message_id,created_at) VALUES(?,?,?,NULL,?)",
                (month, chat, "sending", time.time()))
            return cur.rowcount == 1

    def mark_monthly(self, month: str, chat: int, status: str, message_id: int | None = None) -> None:
        with self.state.mu, self.state.db:
            self.state.db.execute(
                "UPDATE referral_monthly_delivery SET status=?,message_id=COALESCE(?,message_id) "
                "WHERE report_month=? AND chat_id=?", (status, message_id, month, chat))
