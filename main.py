from __future__ import annotations
#!/usr/bin/env python3
"""Синай · Новости — instant Telegram UI, read-only source, background amoCRM reports."""

import concurrent.futures
import html
import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time as dtime
from pathlib import Path
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / '.env', encoding='utf-8-sig')
LOG = logging.getLogger('sinai-report')
MANAGERS = ('Павел', 'Ольга Шевелева', 'Юлия Миллер', 'Юлия Дегтярева')
REPORT_SCHEMA_VERSION = 2


def words(value: object) -> tuple[str, ...]:
    text = unicodedata.normalize('NFKC', str(value or '')).casefold().replace('ё', 'е')
    return tuple(re.findall(r'[а-яa-z]+', text))


def edit_distance_at_most_one(a: str, b: str) -> bool:
    if abs(len(a) - len(b)) > 1:
        return False
    if a == b:
        return True
    if len(a) > len(b):
        a, b = b, a
    i = j = errors = 0
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            errors += 1
            if errors > 1:
                return False
            if len(a) == len(b):
                i += 1
            j += 1
    return errors + len(b) - j <= 1


def resolve_manager(value: object) -> str | None:
    """Conservative matching: order-independent, aliases, unique surname + initial, minor typo.

    Never guess when 2 managers could match. 'Павел' is a single-token manager.
    """
    tokens = words(value)
    if not tokens:
        return None
    name_aliases = {'юлия': {'юлия', 'юля', 'юли', 'ю'},
                    'ольга': {'ольга', 'оля', 'ольги', 'о'},
                    'павел': {'павел', 'паша', 'павла'}}
    matches = set()
    for candidate in MANAGERS:
        req = words(candidate)
        first = req[0]
        first_hits = [w for w in tokens if w in name_aliases.get(first, {first})]
        if len(req) == 1:
            # Карепов — известная фамилия Павла; отдельный Павел тоже допустим.
            if first_hits or (candidate == 'Павел' and any(
                    w in {'карепов', 'карепова'} for w in tokens)):
                matches.add(candidate)
            continue
        surname = req[1]
        exact_surname = surname in tokens
        fuzzy_surname = any(len(w) >= 5 and edit_distance_at_most_one(w, surname)
                            for w in tokens)
        # Unique surname is enough with initial, full first name or even surname only.
        if exact_surname and (first_hits or len(tokens) == 1):
            matches.add(candidate)
        elif first_hits and fuzzy_surname:
            matches.add(candidate)
    # A surname-only match is unambiguous for this fixed team.
    return next(iter(matches)) if len(matches) == 1 else None


def field_manager(lead: dict, manager_field_id: int = 0) -> str:
    found = []
    for f in lead.get('custom_fields_values') or []:
        fid = f.get('field_id')
        if manager_field_id:
            if str(fid) != str(manager_field_id):
                continue
        elif words(f.get('field_name')) != ('менеджер',):
            continue
        for v in f.get('values') or []:
            raw = v.get('value')
            if isinstance(raw, dict):
                raw = raw.get('name') or raw.get('value')
            if isinstance(raw, str) and raw.strip():
                found.append(raw.strip())
    return ' / '.join(dict.fromkeys(found))


def resolve_lead_manager(lead: dict, users: dict[int, str], field_id: int = 0) -> tuple[str | None, str, str]:
    """Owner whitelist wins; otherwise use field Manager, never fall back to other owners."""
    owner = users.get(int(lead.get('responsible_user_id') or 0), '')
    match = resolve_manager(owner)
    brief = field_manager(lead, field_id)
    if match:
        return match, 'ответственный', owner
    manager = resolve_manager(brief)
    return manager, ('Бриф → Менеджер' if manager else 'не определён'), (brief or owner or 'пусто')


def day_bounds(day: date, timezone: ZoneInfo) -> tuple[float, float]:
    return (datetime.combine(day, dtime.min, timezone).timestamp(),
            datetime.combine(day + timedelta(days=1), dtime.min, timezone).timestamp())


def local_day(ts: float, tz: ZoneInfo) -> date:
    return datetime.fromtimestamp(ts, tz).date()



def report_markup(day: date, today: date) -> dict:
    prev = day - timedelta(days=1)
    row = [
        {"text": f"◀️ {prev:%d.%m}",
         "callback_data": "r:" + prev.strftime("%Y%m%d")},
        {"text": f"{day:%d.%m}", "callback_data": "noop"}
    ]
    nxt = day + timedelta(days=1)
    if nxt < today:
        row.append({
            "text": f"{nxt:%d.%m} ▶️",
            "callback_data": "r:" + nxt.strftime("%Y%m%d")
        })
    return {"inline_keyboard": [row]}



@dataclass(frozen=True)
class Config:
    tg_token: str
    allowed: frozenset[int]
    source: Path
    state: Path
    amo_url: str
    amo_token: str
    tz: ZoneInfo
    report_at: dtime
    manager_field_id: int
    refresh_after: int
    workers: int

    @classmethod
    def load(cls) -> 'Config':
        def require(name: str) -> str:
            val = os.getenv(name, '').strip()
            if not val:
                raise ValueError(f'Нужно заполнить {name} в .env')
            return val
        access = set()
        for field in ('REPORT_CHAT_ID', 'ALLOWED_USER_IDS', 'MANAGER_IDS'):
            access.update(int(item.strip()) for item in re.split(r'[,;\s]+', os.getenv(field, '')) if item.strip())
        if not access:
            raise ValueError('Укажите REPORT_CHAT_ID или ALLOWED_USER_IDS (список Telegram ID)')
        def absolute(key: str, default: str) -> Path:
            p = Path(os.getenv(key, default)).expanduser()
            return (p if p.is_absolute() else ROOT / p).resolve()
        clock = require('DAILY_REPORT_TIME') if os.getenv('DAILY_REPORT_TIME') else '09:00'
        h, m = map(int, clock.split(':'))
        base = require('AMOCRM_BASE_URL').rstrip('/')
        if urlsplit(base).scheme != 'https':
            raise ValueError('amoCRM URL должен использовать HTTPS')
        return cls(tg_token=require('REPORT_TG_BOT_TOKEN'), allowed=frozenset(access),
                   source=absolute('SOURCE_DATABASE_PATH', '../sinai-news-bot2/data/sinai.sqlite3'),
                   state=absolute('REPORT_STATE_PATH', './data/report_state.sqlite3'),
                   amo_url=base, amo_token=os.getenv('AMOCRM_ACCESS_TOKEN', '').strip(),
                   tz=ZoneInfo(os.getenv('TIMEZONE', 'Asia/Krasnoyarsk')),
                   report_at=dtime(h, m), manager_field_id=int(os.getenv('MANAGER_FIELD_ID', '0')),
                   refresh_after=max(60, int(os.getenv('CACHE_TTL_SECONDS', '900'))),
                   workers=max(1, min(5, int(os.getenv('CRM_WORKERS', '4')))))


class State:
    """All writes local; source bot SQLite is NEVER mutated."""
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), check_same_thread=False, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.mu = threading.RLock()
        with self.mu:
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.executescript('''
            CREATE TABLE IF NOT EXISTS subscribers (chat_id INTEGER PRIMARY KEY, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS snapshots (day TEXT PRIMARY KEY, captured REAL NOT NULL, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS daily (delivery_day TEXT NOT NULL, chat_id INTEGER NOT NULL,
                status TEXT NOT NULL, message_id INTEGER, created_at REAL NOT NULL,
                PRIMARY KEY(delivery_day, chat_id));
            CREATE TABLE IF NOT EXISTS offsets (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            ''')
            self.db.commit()

    def subscribe(self, chat: int) -> None:
        with self.mu, self.db:
            self.db.execute('INSERT OR IGNORE INTO subscribers VALUES (?,?)', (chat, time.time()))

    def subscribers(self) -> list[int]:
        with self.mu:
            return [r[0] for r in self.db.execute('SELECT chat_id FROM subscribers')]

    def offset(self) -> int:
        with self.mu:
            row = self.db.execute("SELECT value FROM offsets WHERE key='tg'").fetchone()
            return int(row[0]) if row else 0

    def save_offset(self, offset: int) -> None:
        with self.mu, self.db:
            self.db.execute('INSERT INTO offsets(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                            ('tg', offset))

    def snapshot(self, day: date) -> dict | None:
        with self.mu:
            row = self.db.execute('SELECT captured,payload FROM snapshots WHERE day=?', (day.isoformat(),)).fetchone()
            if not row:
                return None
            out = json.loads(row['payload'])
            if out.get('_schema_version') != REPORT_SCHEMA_VERSION:
                return None  # Recalculate legacy reports after changing manager attribution.
            out['captured'] = row['captured']
            return out

    def save_snapshot(self, day: date, report: dict) -> None:
        with self.mu, self.db:
            self.db.execute('INSERT INTO snapshots VALUES (?,?,?) ON CONFLICT(day) DO UPDATE SET '
                            'captured=excluded.captured,payload=excluded.payload',
                            (day.isoformat(), time.time(), json.dumps({**report, '_schema_version': REPORT_SCHEMA_VERSION}, ensure_ascii=False)))

    def claim_daily(self, delivery_date: date, chat: int) -> bool:
        with self.mu, self.db:
            cur = self.db.execute('INSERT OR IGNORE INTO daily VALUES (?,?,?,?,?)',
                                  (delivery_date.isoformat(), chat, 'sending', None, time.time()))
            return cur.rowcount == 1

    def mark_daily(self, delivery_date: date, chat: int, status: str, mid: int | None = None) -> None:
        with self.mu, self.db:
            self.db.execute('UPDATE daily SET status=?,message_id=COALESCE(?,message_id) '
                            'WHERE delivery_day=? AND chat_id=?',
                            (status, mid, delivery_date.isoformat(), chat))

    def pending_daily(self) -> list[tuple[date, int, int]]:
        with self.mu:
            return [(date.fromisoformat(r[0]), int(r[1]), int(r[2])) for r in self.db.execute(
                "SELECT delivery_day,chat_id,message_id FROM daily WHERE status='pending' AND message_id IS NOT NULL")]


class Api:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.thread_local = threading.local()
        self.rate_mu = threading.Lock()
        self.next_request = 0.0
        self.token_mu = threading.Lock()
        self.current_token = cfg.amo_token

    def session(self) -> requests.Session:
        if not hasattr(self.thread_local, 'session'):
            self.thread_local.session = requests.Session()
        return self.thread_local.session

    def throttle(self) -> None:
        with self.rate_mu:
            now = time.monotonic()
            wait = max(0., self.next_request - now)
            self.next_request = max(now, self.next_request) + .26  # < 4 req/s; below amoCRM 5/s
        if wait:
            time.sleep(wait)

    def source_token(self) -> str:
        """Source app may refresh OAuth; read its persisted current token on demand."""
        try:
            uri = 'file:' + quote(str(self.cfg.source), safe='/') + '?mode=ro'
            with sqlite3.connect(uri, uri=True, timeout=3) as db:
                row = db.execute("SELECT value FROM settings WHERE key='oauth'").fetchone()
                if row:
                    token = json.loads(row[0]).get('access_token')
                    if token:
                        return str(token)
        except (sqlite3.Error, ValueError, TypeError):
            pass
        return self.cfg.amo_token

    def crm(self, path: str, params: dict | None = None) -> dict:
        if not self.current_token:
            with self.token_mu:
                self.current_token = self.source_token()
        if not self.current_token:
            raise RuntimeError('Нет токена amoCRM ни в .env, ни в settings.oauth исходной БД')
        for attempt in range(6):
            self.throttle()
            try:
                resp = self.session().get(self.cfg.amo_url + path,
                         headers={'Authorization': 'Bearer ' + self.current_token},
                         params=params, timeout=(7, 20))
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt == 5:
                    raise RuntimeError('amoCRM: ошибка сети') from e
                time.sleep(min(2 ** attempt, 15))
                continue
            if resp.status_code == 401:
                fresh = self.source_token()
                if fresh and fresh != self.current_token:
                    with self.token_mu:
                        self.current_token = fresh
                    continue
                raise RuntimeError('amoCRM: 401. Проверьте OAuth-токен основного бота')
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == 5:
                    raise RuntimeError('amoCRM: HTTP ' + str(resp.status_code))
                try:
                    retry_after = min(30, max(0, float(resp.headers.get('Retry-After', 0))))
                except ValueError:
                    retry_after = 0
                time.sleep(max(retry_after, min(2 ** attempt, 15)))
                continue
            if resp.status_code == 404:
                raise LookupError('Сделка удалена или недоступна')
            resp.raise_for_status()
            return resp.json() if resp.content else {}
        raise RuntimeError('amoCRM request retries exceeded')

    def users(self) -> dict[int, str]:
        result = {}
        page = 1
        while True:
            data = self.crm('/api/v4/users', {'page': page, 'limit': 250})
            for u in data.get('_embedded', {}).get('users', []):
                result[int(u['id'])] = u.get('name', '')
            if not data.get('_links', {}).get('next'):
                break
            page += 1
        return result

    def detect_manager_field(self) -> int:
        """Use the field explicitly inside 'Бриф' (not a namesake in another tab)."""
        if self.cfg.manager_field_id:
            return self.cfg.manager_field_id
        try:
            data = self.crm('/api/v4/leads/custom_fields/groups')
            groups = data.get('_embedded', {}).get('custom_field_groups', [])
            brief_groups = {str(g['id']) for g in groups if words(g.get('name')) == ('бриф',)}
            all_manager_fields = []
            page = 1
            while True:
                d = self.crm('/api/v4/leads/custom_fields', {'page': page, 'limit': 50})
                for field in d.get('_embedded', {}).get('custom_fields', []):
                    if words(field.get('name')) == ('менеджер',):
                        all_manager_fields.append(field)
                if not d.get('_links', {}).get('next'):
                    break
                page += 1
            in_brief = [f for f in all_manager_fields if str(f.get('group_id')) in brief_groups]
            if len(in_brief) == 1:
                LOG.info('amoCRM field Бриф → Менеджер: %s', in_brief[0]['id'])
                return int(in_brief[0]['id'])
            # If no tab named Brief is exposed, use the globally unique field by name.
            if not brief_groups and len(all_manager_fields) == 1:
                LOG.warning('В API нет группы Бриф; используется единственное поле Менеджер #%s',
                            all_manager_fields[0]['id'])
                return int(all_manager_fields[0]['id'])
            if len(in_brief) > 1 or len(all_manager_fields) > 1:
                raise RuntimeError('Неоднозначное поле «Менеджер». Задайте MANAGER_FIELD_ID в .env')
            if brief_groups:
                LOG.warning('В группе Бриф не найдено поле Менеджер; пропускаем неоднозначный fallback')
                return -1
            LOG.warning('Не найдено поле Менеджер по метаданным CRM; будет поиск по имени')
            return 0
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (403, 404):
                LOG.warning('Нет доступа к метаданным полей, поиск по названию')
                return 0
            raise

    def lead_batch(self, ids: list[int]) -> dict[int, dict | None]:
        """Fast filtered multi-ID GET. Verify filtering, fall back to individual GETs."""
        def separate() -> dict[int, dict | None]:
            out = {}
            for lid in ids:
                try:
                    out[lid] = self.crm(f'/api/v4/leads/{lid}')
                except LookupError:
                    out[lid] = None
            return out
        if len(ids) < 6:
            return separate()
        params: dict = {'limit': 250}
        params.update({f'filter[id][{i}]': lid for i, lid in enumerate(ids)})
        try:
            data = self.crm('/api/v4/leads', params)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (400, 422):
                LOG.warning('CRM batch ID filter refused, using individual GET')
                return separate()
            raise
        items = data.get('_embedded', {}).get('leads', [])
        wanted = set(ids)
        # Some accounts ignore filter parameters. Treat unexpected leads as proof of that.
        if data.get('_links', {}).get('next') or any(int(item['id']) not in wanted for item in items):
            LOG.warning('CRM did not apply filter[id]; using verified individual GET for this batch')
            return separate()
        found = {int(item['id']): item for item in items}
        # Distinguish genuinely removed/inaccessible leads from a partial batch response.
        for lid in wanted - found.keys():
            try:
                found[lid] = self.crm(f'/api/v4/leads/{lid}')
            except LookupError:
                found[lid] = None
        return found


def read_source(path: Path) -> list[tuple[int, float]]:
    if not path.is_file():
        raise FileNotFoundError(f'База основного бота не найдена: {path}')
    uri = 'file:' + quote(str(path), safe='/') + '?mode=ro'
    with sqlite3.connect(uri, uri=True, timeout=8) as db:
        cols = {r[1] for r in db.execute('PRAGMA table_info(users)')}
        required = {'lead_id', 'subscribed_at', 'platform'}
        if not required.issubset(cols):
            raise RuntimeError('Неизвестная схема таблицы users основного бота')
        # One deal = one client, even if both Telegram and MAX are connected.
        rows = db.execute('''SELECT lead_id, MIN(subscribed_at) AS since FROM users
            WHERE lead_id IS NOT NULL AND lead_id>0 AND subscribed_at IS NOT NULL
            GROUP BY lead_id ORDER BY lead_id''').fetchall()
        return [(int(r[0]), float(r[1])) for r in rows]


class Telegram:
    def __init__(self, cfg: Config):
        self.token = cfg.tg_token
        self.local = threading.local()

    def call(self, method: str, **params):
        if not hasattr(self.local, 'session'):
            self.local.session = requests.Session()
        s = self.local.session
        response = s.post(f'https://api.telegram.org/bot{self.token}/{method}', json=params,
                          timeout=(5, (int(params.get('timeout', 0)) + 12 if method == 'getUpdates' else 8)))
        if response.status_code == 429:
            raise RuntimeError('Telegram 429, превышен лимит')
        response.raise_for_status()
        data = response.json()
        if not data.get('ok'):
            raise RuntimeError(f'Telegram {method}: {str(data.get("description", "unknown"))[:180]}')
        return data['result']

    def send(self, chat: int, text: str, markup: dict | None = None) -> int:
        params = {'chat_id': chat, 'text': text, 'parse_mode': 'HTML',
                  'disable_web_page_preview': True}
        if markup is not None:
            params['reply_markup'] = markup
        return int(self.call('sendMessage', **params)['message_id'])

    def edit(self, chat: int, mid: int, text: str, markup: dict | None = None) -> bool:
        params = {'chat_id': chat, 'message_id': mid, 'text': text, 'parse_mode': 'HTML',
                  'disable_web_page_preview': True}
        if markup is not None:
            params['reply_markup'] = markup
        try:
            self.call('editMessageText', **params)
            return True
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                LOG.warning('Telegram edit ignored: chat=%s message=%s HTTP 400', chat, mid)
                return False
            raise
        except RuntimeError as e:
            if 'message is not modified' in str(e).lower():
                return True
            raise

    def answer(self, cid: str, text: str = '') -> None:
        try:
            self.call('answerCallbackQuery', callback_query_id=cid, text=text[:180])
        except Exception as e:
            LOG.debug('Callback already expired: %s', e)


class ReportService:
    def __init__(self, cfg: Config, state: State, tg: Telegram):
        self.cfg, self.state, self.tg = cfg, state, tg
        self.api = Api(cfg)
        self.jobs: queue.Queue[date] = queue.Queue()
        self.mu = threading.RLock()
        self.queued: set[date] = set()
        self.waiters: dict[date, set[tuple[int, int]]] = {}
        self.views: dict[tuple[int, int], date | None] = {}
        self.active: date | None = None
        self.progress = (0, 0)
        self.active_at = 0.

    def now(self) -> datetime:
        return datetime.now(self.cfg.tz)

    def busy(self) -> str:
        with self.mu:
            if self.active:
                a, b = self.progress
                return f'🔄 Сверка {self.active:%d.%m}: {a}/{b} сделок'
            if self.queued:
                return f'⏳ В очереди: {len(self.queued)} отчёт(ов)'
            return '🟢 Ожидание запросов'

    def set_view(self, chat: int, mid: int, day: date | None) -> None:
        """Invalidate old async edits when user navigates to another screen."""
        with self.mu:
            key = (chat, mid)
            self.views[key] = day
            for watchers in self.waiters.values():
                watchers.discard(key)

    def schedule(self, day: date, chat: int | None = None, mid: int | None = None) -> None:
        with self.mu:
            if chat is not None and mid is not None:
                self.views[(chat, mid)] = day
                self.waiters.setdefault(day, set()).add((chat, mid))
            if day not in self.queued:
                self.queued.add(day)
                self.jobs.put(day)

    def format_report(self, day: date, snapshot: dict, stale: bool = False) -> str:
        lines = ['📊 <b>ЮК Синай · Новости</b>',
                 f'📅 Отчёт: <b>{day:%d.%m.%Y}</b>', '']
        for name in MANAGERS:
            total, added = snapshot['counts'][name]
            lines.append(f'👤 {html.escape(name)} — <b>{total}</b> всего <i>(+{added} за день)</i>')
        total = sum(values[0] for values in snapshot['counts'].values())
        daily = sum(values[1] for values in snapshot['counts'].values())
        lines.extend(['', f'👥 <b>Итого: {total}</b> · +{daily} за день'])
        if stale:
            lines.append('⏳ Обновляю данные…')
        return '\n'.join(lines)

    def placeholder(self, day: date) -> str:
        snap = self.state.snapshot(day)
        if snap:
            return self.format_report(day, snap, True)
        return (f'📊 <b>ЮК Синай · Новости</b>\n'
                f'📅 Отчёт: <b>{day:%d.%m.%Y}</b>\n\n'
                '⏳ Формирую отчёт…')


    def open_report(self, chat: int, day: date, mid: int | None = None, force: bool = False) -> None:
        today = self.now().date()
        if day >= today or day < date(2020, 1, 1):
            return

        snap = self.state.snapshot(day)

        if snap:
            text = self.format_report(day, snap)
            markup = report_markup(day, today)

            if mid is None:
                mid = self.tg.send(chat, text, markup)
            else:
                self.set_view(chat, mid, day)
                self.tg.edit(chat, mid, text, markup)

            self.set_view(chat, mid, day)

            if force or time.time() - snap["captured"] >= self.cfg.refresh_after:
                self.schedule(day, chat, mid)

        elif mid is not None:
            self.schedule(day, chat, mid)
        else:
            self.schedule(day)
            self.tg.send(chat, "Отчёт временно недоступен. Повторите /start позже.")

    def build(self, day: date) -> dict:
        source = read_source(self.cfg.source)
        user_map = self.api.users()
        manager_field_id = self.api.detect_manager_field()
        start, end = day_bounds(day, self.cfg.tz)
        counts = {m: [0, 0] for m in MANAGERS}
        by_manager = {m: [] for m in MANAGERS}
        with self.mu:
            self.progress = (0, len(source))
            self.active_at = time.time()

        def get_batch(batch: list[tuple[int, float]]):
            data = self.api.lead_batch([lid for lid, _ in batch])
            results = []
            for lid, joined in batch:
                lead = data.get(lid)
                if lead is None:
                    results.append((lid, joined, None, 'Сделка удалена или недоступна'))
                    continue
                manager, origin, raw = resolve_lead_manager(lead, user_map, manager_field_id)
                if not manager and len(batch) > 1:
                    # A list response might omit some custom fields: verify detail before excluding.
                    try:
                        details = self.api.crm(f'/api/v4/leads/{lid}')
                        manager, origin, raw = resolve_lead_manager(details, user_map, manager_field_id)
                    except LookupError:
                        results.append((lid, joined, None, 'Сделка удалена или недоступна'))
                        continue
                results.append((lid, joined, manager, f'{origin}: {raw}'))
            return results

        # Usually ~80 leads/request. On accounts rejecting API filters, verified GET fallback.
        batches = [source[i:i+80] for i in range(0, len(source), 80)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.cfg.workers) as pool:
            futures = [pool.submit(get_batch, batch) for batch in batches]
            processed = 0
            for future in concurrent.futures.as_completed(futures):
                for lid, joined, manager, reason in future.result():
                    if manager:
                        counts[manager][0] += 1
                        is_new = start <= joined < end
                        if is_new:
                            counts[manager][1] += 1
                        by_manager[manager].append([lid, is_new])
                    processed += 1
                with self.mu:
                    self.progress = (processed, len(source))
        for entries in by_manager.values():
            entries.sort(key=lambda row: (not row[1], row[0]))  # Today's additions first.
        return {'day': day.isoformat(), 'counts': counts,
                'by_manager': by_manager, 'source_count': len(source),
                'matched_count': sum(x[0] for x in counts.values())}

    def _notify(self, day: date, error: Exception | None) -> None:
        with self.mu:
            watchers = list(self.waiters.pop(day, set()))
        snap = self.state.snapshot(day)
        outcomes: dict[tuple[int, int], str] = {}
        for chat, mid in watchers:
            with self.mu:
                if self.views.get((chat, mid)) != day:
                    outcomes[(chat, mid)] = 'superseded'
                    continue  # The user navigated to a newer screen.
            try:
                if error:
                    info = f'❌ <b>Не удалось обновить отчёт.</b>\n{html.escape(str(error))[:220]}\n\n'
                    if snap:
                        info += self.format_report(day, snap, True)
                    else:
                        info += 'Повторите /report позже.'
                    self.tg.edit(chat, mid, info, report_markup(day, self.now().date()))
                elif snap:
                    self.tg.edit(chat, mid, self.format_report(day, snap),
                                 report_markup(day, self.now().date()))
                outcomes[(chat, mid)] = 'error' if error else 'delivered'
            except Exception:
                outcomes[(chat, mid)] = 'error'
                LOG.exception('Could not update report view, chat=%s mid=%s', chat, mid)
        # In a single report run multiple daily recipients might be awaiting the same day.
        for delivery_date, chat, mid in self.state.pending_daily():
            if delivery_date - timedelta(days=1) != day:
                continue
            # Never mark delivered unless the final Telegram edit succeeded.
            status = outcomes.get((chat, mid), 'error')
            self.state.mark_daily(delivery_date, chat, status, mid)

    def work(self) -> None:
        while True:
            day = self.jobs.get()
            err = None
            with self.mu:
                self.active = day
                self.progress = (0, 0)
            try:
                result = self.build(day)
                self.state.save_snapshot(day, result)
                LOG.info('Report built for %s: source=%s matched=%s', day,
                         result['source_count'], result['matched_count'])
            except Exception as e:
                err = e
                LOG.exception('Report refresh failed for %s', day)
            finally:
                # Drop the job marker BEFORE delivering edits: any new refresh must get its own job.
                with self.mu:
                    self.queued.discard(day)
                    self.active = None
                try:
                    self._notify(day, err)
                except Exception:
                    LOG.exception('Could not finish report notifications')
                finally:
                    self.jobs.task_done()

    def status_text(self) -> str:
        today = self.now().date()
        snap = self.state.snapshot(today - timedelta(days=1))
        stamp = datetime.fromtimestamp(snap['captured'], self.cfg.tz).strftime('%d.%m %H:%M') if snap else 'ещё нет'
        return (f'ℹ️ <b>Синай · состояние</b>\n\n{html.escape(self.busy())}\n'
                f'🕘 Автоотчёт: {self.cfg.report_at:%H:%M} (Красноярск)\n'
                f'📊 Последняя сверка за вчера: {stamp}\n'
                f'👥 Получателей: {len(self.state.subscribers())}\n'
                '🔒 amoCRM и SQLite основного бота используются только для чтения.')

    def manager_page(self, day: date, index: int, page: int) -> tuple[str, dict]:
        if not (0 <= index < len(MANAGERS)):
            raise ValueError('Unknown manager')
        snap = self.state.snapshot(day)
        back = {'text': '◀️ К отчёту', 'callback_data': 'r:' + day.strftime('%Y%m%d')}
        if not snap:
            return 'Снимка пока нет. Откройте отчёт и дождитесь фоновой сверки.', {
                'inline_keyboard': [[back]]}
        name = MANAGERS[index]
        rows = snap.get('by_manager', {}).get(name, [])
        count = len(rows)
        n_pages = max(1, (count + 11) // 12)
        page = max(0, min(page, n_pages - 1))
        text = [f'👤 <b>{html.escape(name)}</b>',
                f'📅 Новые за {day:%d.%m.%Y}',
                f'👥 Всего: <b>{count}</b> · новых: <b>{snap["counts"][name][1]}</b>',
                f'📄 Страница {page + 1}/{n_pages}', '']
        for lid, new in rows[page*12:(page+1)*12]:
            url = f'{self.cfg.amo_url}/leads/detail/{lid}'
            text.append(f'{"🆕" if new else "•"} <a href="{html.escape(url, quote=True)}">Сделка {lid}</a>')
        if not rows:
            text.append('Привязанных сделок у менеджера пока нет.')
        nav = []
        date_key = day.strftime('%Y%m%d')
        if page:
            nav.append({'text': '◀️', 'callback_data': f'm:{date_key}:{index}:{page-1}'})
        if page + 1 < n_pages:
            nav.append({'text': '▶️', 'callback_data': f'm:{date_key}:{index}:{page+1}'})
        return '\n'.join(text), {'inline_keyboard': ([nav] if nav else []) + [[back]]}



class App:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.state = State(cfg.state)
        self.tg = Telegram(cfg)
        self.svc = ReportService(cfg, self.state, self.tg)
        self.restored_daily: set[tuple[date, int, int]] = set()

    def menu(self, chat: int, mid: int | None = None) -> None:
        today = datetime.now(self.cfg.tz).date()
        status = self.svc.busy()
        text = ('🏠 <b>ЮК Синай · отчёты по подпискам</b>\n\n'
                'Бот считает уникальные сделки клиентов «Синай · Новости» в Telegram и MAX.\n'
                'Показывает каждого менеджера: всего и новых за выбранный день.\n\n'
                f'{html.escape(status)}\n'
                '📍 Ежедневно в 09:00 по Красноярску.\n\n'
                '<i>/report — вчера; /today — сегодня; /status — состояние.</i>')
        markup = {'inline_keyboard': [
            [{'text': '📊 Отчёт за вчера', 'callback_data': 'r:' + (today-timedelta(days=1)).strftime('%Y%m%d')}],
            [{'text': '📅 Сегодня', 'callback_data': 'r:' + today.strftime('%Y%m%d')},
             {'text': '🔄 Обновить вчера', 'callback_data': 'f:' + (today-timedelta(days=1)).strftime('%Y%m%d')}],
            [{'text': 'ℹ️ Статус', 'callback_data': 'status'}]]}
        if mid is not None:
            self.svc.set_view(chat, mid, None)
            self.tg.edit(chat, mid, text, markup)
        else:
            self.tg.send(chat, text, markup)

    def authorized(self, raw: dict) -> tuple[int, int] | None:
        who = raw.get('from') or {}
        chat = raw.get('message', {}).get('chat') or raw.get('chat') or {}
        if chat.get('type') != 'private':
            return None
        user_id = int(who.get('id', 0))
        chat_id = int(chat.get('id', 0))
        if user_id not in self.cfg.allowed or chat_id != user_id:
            return None
        return user_id, chat_id

    def handle(self, update: dict) -> None:
        callback = update.get('callback_query')
        msg = (callback or {}).get('message') or update.get('message') or {}
        chat_obj = msg.get('chat') or {}
        chat = int(chat_obj.get('id', 0))
        who = (callback or update.get('message') or {}).get('from') or {}
        is_auth = (chat_obj.get('type') == 'private' and int(who.get('id', 0)) in self.cfg.allowed
                   and int(who.get('id', 0)) == chat)
        if callback:
            # Answer Telegram callback immediately, even if UI/backend is busy.
            self.tg.answer(str(callback.get('id', '')),
                           '' if is_auth else 'Нет доступа')
            if not is_auth:
                return
            payload = str(callback.get('data', ''))
            if payload == 'noop':
                return
            mid = msg.get('message_id')
            if not mid:
                return
            self.svc.set_view(chat, mid, None)
            if payload == 'home':
                self.svc.open_report(chat, datetime.now(self.cfg.tz).date() - timedelta(days=1), mid)
            elif payload == 'status':
                self.tg.edit(chat, mid, self.svc.status_text(),
                             {'inline_keyboard': [[{'text': '🏠 Меню', 'callback_data': 'home'}]]})
            elif payload.startswith('m:'):
                try:
                    _, date_str, idx, page = payload.split(':')
                    day = datetime.strptime(date_str, '%Y%m%d').date()
                    screen, buttons = self.svc.manager_page(day, int(idx), int(page))
                    self.tg.edit(chat, mid, screen, buttons)
                except (ValueError, KeyError):
                    self.tg.edit(chat, mid, 'Некорректные данные. Вернитесь в меню.',
                                 {'inline_keyboard': [[{'text': '🏠 Меню', 'callback_data': 'home'}]]})
            elif payload.startswith(('r:', 'f:')):
                cmd, _, val = payload.partition(':')
                try:
                    day = datetime.strptime(val, '%Y%m%d').date()
                except ValueError:
                    return
                self.svc.open_report(chat, day, mid, force=(cmd == 'f'))
            return
        if not is_auth:
            # Silently ignore strangers so statistics never leak.
            return
        command = str(msg.get('text', '')).strip().split()
        if not command:
            return
        name = command[0].split('@')[0].lower()
        if name == '/start':
            self.state.subscribe(chat)
            self.svc.open_report(chat, datetime.now(self.cfg.tz).date() - timedelta(days=1))
        elif name in ('/report', '/yesterday', '/refresh', '/today'):
            now = datetime.now(self.cfg.tz)
            day = now.date() - timedelta(days=1)
            if len(command) > 1:
                try:
                    day = date.fromisoformat(command[1])
                except ValueError:
                    self.tg.send(chat, 'Используйте /report ГГГГ-ММ-ДД, например /report 2026-10-14')
                    return
            self.svc.open_report(chat, day, force=name == '/refresh')
        elif name == '/status':
            self.tg.send(chat, self.svc.status_text(),
                         {'inline_keyboard': [[{'text': '🏠 Меню', 'callback_data': 'home'}]]})
        else:
            self.svc.open_report(chat, datetime.now(self.cfg.tz).date() - timedelta(days=1))

    def daily_loop(self) -> None:
        """Daily report delivery to managers only."""
        while True:
            try:
                now = datetime.now(self.cfg.tz)

                manager_ids = {
                    int(x.strip())
                    for x in os.getenv('MANAGER_IDS', '').split(',')
                    if x.strip()
                }

                if now.time().replace(tzinfo=None) >= self.cfg.report_at:
                    for chat in sorted(manager_ids):
                        delivery_date = now.date()

                        if not self.state.snapshot(delivery_date - timedelta(days=1)):
                            self.svc.schedule(delivery_date - timedelta(days=1))
                            continue

                        if not self.state.claim_daily(delivery_date, chat):
                            continue

                        day = delivery_date - timedelta(days=1)

                        try:
                            mid = self.tg.send(
                                chat,
                                self.svc.format_report(day, self.state.snapshot(day)),
                                report_markup(day, delivery_date)
                            )
                        except Exception:
                            # Sending might have succeeded remotely. Never blind-retry.
                            self.state.mark_daily(delivery_date, chat, 'unknown')
                            LOG.exception('Daily Telegram send uncertain for chat=%s', chat)
                            continue

                        self.state.mark_daily(delivery_date, chat, 'delivered', mid)

                # Restore pending daily placeholders for managers only.
                for delivery_date, chat, mid in self.state.pending_daily():
                    if chat not in manager_ids:
                        continue

                    key = (delivery_date, chat, mid)
                    if delivery_date <= now.date() and key not in self.restored_daily:
                        self.restored_daily.add(key)
                        self.svc.schedule(delivery_date - timedelta(days=1), chat, mid)

            except Exception:
                LOG.exception('Daily schedule error')

            time.sleep(10)

    def precompute_loop(self) -> None:
        while True:
            try:
                today = datetime.now(self.cfg.tz).date()
                for day in (today, today - timedelta(days=1), today - timedelta(days=2)):
                    snap = self.state.snapshot(day)
                    if snap is None or time.time() - snap['captured'] >= self.cfg.refresh_after:
                        self.svc.schedule(day)
            except Exception:
                LOG.exception('Precompute error')
            time.sleep(min(300, self.cfg.refresh_after))

    def run(self) -> None:
        if not self.cfg.source.is_file():
            raise FileNotFoundError(f'Основная база недоступна: {self.cfg.source}')
        # Check token and webhook before any background jobs.
        bot = self.tg.call('getMe')
        webhook = self.tg.call('getWebhookInfo')
        if webhook.get('url'):
            raise RuntimeError('У отчётного Telegram-бота настроен webhook: удалите его, чтобы использовать polling')
        LOG.info('Started @%s, timezone=%s, source=%s', bot.get('username'), self.cfg.tz, self.cfg.source)
        yesterday = datetime.now(self.cfg.tz).date() - timedelta(days=1)
        if self.state.snapshot(yesterday) is None:
            LOG.info('Precomputing report: %s', yesterday)
            self.state.save_snapshot(yesterday, self.svc.build(yesterday))
        threading.Thread(target=self.svc.work, daemon=True, name='crm-reports').start()
        threading.Thread(target=self.daily_loop, daemon=True, name='daily-scheduler').start()
        threading.Thread(target=self.precompute_loop, daemon=True, name='report-precompute').start()
        offset = self.state.offset()
        while True:
            try:
                updates = self.tg.call('getUpdates', offset=offset, timeout=25,
                                       allowed_updates=['message', 'callback_query'], limit=100)
                for upd in updates:
                    new_offset = int(upd['update_id']) + 1
                    try:
                        self.handle(upd)
                    except Exception:
                        LOG.exception('Telegram UI handler error')
                    # Persist update cursor; duplicate effects are limited to UI on crashes.
                    offset = new_offset
                    self.state.save_offset(offset)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 409:
                    LOG.error('409 polling conflict: stop other instances using this REPORT_TG_BOT_TOKEN')
                else:
                    LOG.exception('Telegram polling HTTP error')
                time.sleep(5)
            except Exception:
                LOG.exception('Telegram polling error')
                time.sleep(3)


def main() -> None:
    logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO').upper(),
                        format='%(asctime)s %(levelname)s %(threadName)s %(message)s')
    App(Config.load()).run()


if __name__ == '__main__':
    main()
