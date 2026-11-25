import unittest
from datetime import date

from referral_control import month_bounds, month_key, parse_report_deal_id


class ReferralParsingTests(unittest.TestCase):
    def test_three_line_report(self):
        text = "1. 32647005\n2. Паспорт не запросил\n3. Не подходит под процедуру"
        self.assertEqual(parse_report_deal_id(text), 32647005)

    def test_plain_deal_id_on_first_line(self):
        self.assertEqual(parse_report_deal_id("32647005\nПричина\nКомментарий"), 32647005)

    def test_reject_short_or_unstructured_message(self):
        self.assertIsNone(parse_report_deal_id("32647005"))
        self.assertIsNone(parse_report_deal_id("Новый клиент\n32647005\nТекст"))

    def test_month_boundaries_regular_and_leap_year(self):
        self.assertEqual(month_bounds("2026-11"), (date(2026, 11, 1), date(2026, 11, 30)))
        self.assertEqual(month_bounds("2028-02"), (date(2028, 2, 1), date(2028, 2, 29)))
        self.assertEqual(month_key(date(2026, 11, 25)), "2026-11")


if __name__ == "__main__":
    unittest.main()

# The following tests exercise the SQLite reconciliation without network access.
import os
import sqlite3
import time
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from referral_control import ReferralControl


class FakeState:
    def __init__(self):
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        import threading
        self.mu = threading.RLock()


class FakeTelegram:
    pass


class FakeApi:
    def __init__(self, event_at):
        self.event_at = event_at
        self.calls = []
        self.event = {
            "id": "event-1", "entity_type": "lead", "entity_id": 32647005,
            "type": "lead_status_changed", "created_at": event_at,
            "value_after": [{"lead_status": {"pipeline_id": 100, "id": 200}}],
        }

    def crm(self, path, params=None):
        self.calls.append((path, params))
        if path == "/api/v4/leads/pipelines":
            return {"_embedded": {"pipelines": [{
                "id": 100, "name": "Отдел продаж",
                "_embedded": {"statuses": [{"id": 200, "name": "сарафан"}]},
            }]}}
        if path == "/api/v4/events":
            return {"_embedded": {"events": [self.event]}}
        if path == "/api/v4/leads/32647005":
            return {"id": 32647005, "responsible_user_id": 2328073, "custom_fields_values": []}
        raise AssertionError(path)


class ReferralReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.tz = ZoneInfo("Asia/Krasnoyarsk")
        self.event_at = int(datetime.now(self.tz).timestamp()) - 20
        self.cfg = SimpleNamespace(tz=self.tz, amo_url="https://sinai24.amocrm.ru")
        self.state = FakeState()
        self.api = FakeApi(self.event_at)
        self.control = None
        self.env = patch.dict(os.environ, {"REPORTS_GROUP_CHAT_ID": "-1001234567890"})
        self.env.start()
        self.control = ReferralControl(self.cfg, self.state, FakeTelegram(), self.api)

    def tearDown(self):
        self.env.stop()
        self.state.db.close()

    def test_stage_event_is_saved_once_and_correct_manager_report_clears_it(self):
        self.assertEqual(self.control.sync(), 1)
        self.assertEqual(self.control.sync(), 0)
        day = datetime.fromtimestamp(self.event_at, self.tz).date()
        missing, _unassigned, total = self.control._missing_for_range(day, day)
        self.assertEqual(total, 1)
        self.assertEqual(missing["Павел"], [32647005])

        self.control.handle_message({
            "chat": {"id": -1001234567890, "type": "supergroup"},
            "from": {"id": 6875793727}, "message_id": 77,
            "date": self.event_at + 10,
            "text": "1. 32647005\n2. Паспорт не запросил\n3. Не подходит под процедуру",
        })
        missing, _unassigned, _total = self.control._missing_for_range(day, day)
        self.assertEqual(missing["Павел"], [])

    def test_wrong_manager_and_next_day_report_do_not_clear_penalty(self):
        self.control.sync()
        day = datetime.fromtimestamp(self.event_at, self.tz).date()
        # Another manager cannot satisfy Pavel's reporting obligation.
        self.control.handle_message({
            "chat": {"id": -1001234567890, "type": "supergroup"},
            "from": {"id": 1062804251}, "message_id": 78,
            "date": self.event_at + 10,
            "text": "1. 32647005\n2. Паспорт не запросил\n3. Не подходит",
        })
        # Nor can a report submitted on the following calendar day.
        next_day = self.event_at + 86400
        self.control.handle_message({
            "chat": {"id": -1001234567890, "type": "supergroup"},
            "from": {"id": 6875793727}, "message_id": 79,
            "date": next_day,
            "text": "1. 32647005\n2. Паспорт не запросил\n3. Не подходит",
        })
        missing, _unassigned, _total = self.control._missing_for_range(day, day)
        self.assertEqual(missing["Павел"], [32647005])


    def test_monthly_scheduler_uses_real_calendar_last_day_and_catchup(self):
        from datetime import datetime
        self.assertIsNone(self.control.monthly_due(datetime(2026, 11, 29, 23, 59, tzinfo=self.tz)))
        self.assertIsNone(self.control.monthly_due(datetime(2026, 11, 30, 23, 58, tzinfo=self.tz)))
        self.assertEqual(self.control.monthly_due(datetime(2026, 11, 30, 23, 59, tzinfo=self.tz)), "2026-11")
        self.assertEqual(self.control.monthly_due(datetime(2028, 3, 1, 0, 1, tzinfo=self.tz)), "2028-02")

    def test_monthly_manager_pagination_is_five_deals(self):
        for offset in range(11):
            with self.state.mu, self.state.db:
                self.state.db.execute(
                    "INSERT INTO referral_entries(lead_id,report_day,event_at,manager_name,manager_amo_id,manager_tg_id) "
                    "VALUES(?,?,?,?,?,?)",
                    (32647005 + offset, "2026-11-01", self.event_at + offset,
                     "Павел", 2328073, 6875793727),
                )
        _screen, markup = self.control.manager_page("2026-11", 0, 0)
        buttons = markup["inline_keyboard"]
        self.assertEqual(len([row for row in buttons if len(row) == 1 and row[0].get("url")]), 5)
        nav = next(row for row in buttons if any(b.get("callback_data") == "noop" for b in row))
        self.assertEqual([b["text"] for b in nav], ["1/3", "→"])
        _screen, markup = self.control.manager_page("2026-11", 0, 2)
        nav = next(row for row in markup["inline_keyboard"] if any(b.get("callback_data") == "noop" for b in row))
        self.assertEqual([b["text"] for b in nav], ["←", "3/3"])

    def test_manager_field_518097_takes_precedence_over_current_responsible(self):
        lead = {
            "responsible_user_id": 2328073,
            "custom_fields_values": [{"field_id": 518097, "values": [{"value": "Юлия Миллер"}]}],
        }
        manager = self.control._manager_for_lead(lead)
        self.assertEqual(manager["name"], "Юлия Миллер")

    def test_same_day_report_counts_even_if_posted_before_stage_change(self):
        self.control.sync()
        day = datetime.fromtimestamp(self.event_at, self.tz).date()
        self.control.handle_message({
            "chat": {"id": -1001234567890, "type": "supergroup"},
            "from": {"id": 6875793727}, "message_id": 80,
            "date": self.event_at - 60,
            "text": "1. 32647005\n2. Паспорт не запросил\n3. Не подходит",
        })
        missing, _unassigned, _total = self.control._missing_for_range(day, day)
        self.assertEqual(missing["Павел"], [])


if __name__ == "__main__":
    unittest.main()
