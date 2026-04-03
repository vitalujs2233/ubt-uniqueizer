const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const dotenv = require('dotenv');
const { Pool } = require('pg');
const geoip = require('geoip-lite');
const multer = require('multer');
dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || 'http://localhost:3000';
const ADMIN_ID = 232682307;
const TRAFORCE_API_BASE = (process.env.TRAFORCE_API_BASE || 'https://api-victoriya.affise.com/3.0').replace(/\/$/, '');
const TRAFORCE_API_KEY = process.env.TRAFORCE_API_KEY || '';
const CPA_MARGIN = Number(process.env.CPA_MARGIN || 0.80);
const TRAFORCE_SMARTLINK_ADULT_WW = process.env.TRAFORCE_SMARTLINK_ADULT_WW || '';
const TRAFORCE_MAINSTREAM_LINK = process.env.TRAFORCE_MAINSTREAM_LINK || '';
const cpaOffersCache = new Map();

app.use(cors());
app.use(express.json());
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 5 * 1024 * 1024
  },
  fileFilter: (req, file, cb) => {
    if (!file.mimetype || !file.mimetype.startsWith('image/')) {
      return cb(new Error('Можно загружать только изображения'));
    }
    cb(null, true);
  }
});

app.get('/', (req, res) => {
  res.json({
    ok: true,
    message: 'UBT backend is running'
  });
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

function validateTelegramInitData(initData, botToken) {
  const urlParams = new URLSearchParams(initData);
  const hash = urlParams.get('hash');

  if (!hash) return null;
  urlParams.delete('hash');

  const dataCheckString = [...urlParams.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`)
    .join('\n');

  const secretKey = crypto
    .createHmac('sha256', 'WebAppData')
    .update(botToken)
    .digest();

  const calculatedHash = crypto
    .createHmac('sha256', secretKey)
    .update(dataCheckString)
    .digest('hex');

  if (calculatedHash !== hash) return null;

  const userRaw = urlParams.get('user');
  if (!userRaw) return null;

  return JSON.parse(userRaw);
}

async function telegramApi(method, body) {
  const response = await fetch(`https://api.telegram.org/bot${process.env.BOT_TOKEN}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });

  return response.json();
}

async function addTransaction({
  telegram_id,
  type,
  amount,
  credits,
  status = 'completed',
  description = null
}) {
  await pool.query(
    `insert into transactions (telegram_id, type, amount, credits, status, description)
     values ($1, $2, $3, $4, $5, $6)`,
    [telegram_id, type, amount, credits, status, description]
  );
}

function generateShortCode(length = 6) {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = '';

  for (let i = 0; i < length; i++) {
    result += chars[Math.floor(Math.random() * chars.length)];
  }

  return result;
}

async function generateUniqueShortCode() {
  while (true) {
    const code = generateShortCode(6);

    const existing = await pool.query(
      'select id from smart_links where short_code = $1 limit 1',
      [code]
    );

    if (existing.rows.length === 0) {
      return code;
    }
  }
}

function normalizeUrl(url) {
  try {
    const parsed = new URL(url);

    if (!/^https?:$/i.test(parsed.protocol)) {
      return null;
    }

    return parsed.toString();
  } catch {
    return null;
  }
}

function getClientIp(req) {
  const forwarded = req.headers['x-forwarded-for'];

  if (forwarded) {
    return forwarded.split(',')[0].trim();
  }

  return req.socket.remoteAddress || '';
}

function hashIp(ip) {
  return crypto
    .createHash('sha256')
    .update(ip + 'ubt_secret_salt')
    .digest('hex');
}

function detectDevice(userAgent = '') {
  const ua = userAgent.toLowerCase();

  if (/iphone|ipad|ipod/.test(ua)) return 'iOS';
  if (/android/.test(ua)) return 'Android';
  if (/windows/.test(ua)) return 'Windows';
  if (/macintosh|mac os/.test(ua)) return 'Mac';
  if (/linux/.test(ua)) return 'Linux';

  return 'Unknown';
}
function isAdmin(user) {
  return String(user?.id) === String(ADMIN_ID);
}
async function spendUserCredits(telegramId, amount, description = 'Создание смарт-ссылки') {
  const result = await pool.query(
    'select * from users where telegram_id = $1',
    [telegramId]
  );

  if (result.rows.length === 0) {
    throw new Error('Пользователь не найден');
  }

  const dbUser = result.rows[0];

  if (Number(dbUser.balance) < Number(amount)) {
    throw new Error('Недостаточно средств');
  }

  await pool.query(
    'update users set balance = balance - $2 where telegram_id = $1',
    [telegramId, amount]
  );

  await addTransaction({
    telegram_id: telegramId,
    type: 'spend',
    amount: amount,
    credits: -amount,
    status: 'completed',
    description
  });
}


function safeNumber(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function getUserIdentityFromBody(req) {
  const { initData, telegram_id } = req.body || {};

  if (initData) {
    const tgUser = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (tgUser) return tgUser;
  }

  if (telegram_id) {
    return { id: Number(telegram_id) };
  }

  return null;
}

function parseTraforceOffer(raw = {}) {
  const payments = Array.isArray(raw.payments) ? raw.payments : [];
  const payment = payments[0] || {};
  const apiPayout =
    safeNumber(payment.total, NaN) ||
    safeNumber(payment.amount, NaN) ||
    safeNumber(raw.payout, NaN) ||
    safeNumber(raw.pay, 0);

  const displayPayout = Number((apiPayout * CPA_MARGIN).toFixed(2));
  const countries = Array.isArray(payment.countries)
    ? payment.countries
    : Array.isArray(raw.countries)
      ? raw.countries
      : [];

  const allowedTraffic = Array.isArray(raw.allowed_traffic_types)
    ? raw.allowed_traffic_types
    : Array.isArray(raw.allowed_traffic)
      ? raw.allowed_traffic
      : [];

  const restrictedTraffic = Array.isArray(raw.restricted_traffic_types)
    ? raw.restricted_traffic_types
    : Array.isArray(raw.restricted_traffic)
      ? raw.restricted_traffic
      : [];

  const categories = Array.isArray(raw.categories)
    ? raw.categories.map((item) => item?.title || item?.name || item).filter(Boolean)
    : [];

  const description =
    raw.description_lang?.en ||
    raw.description_lang?.ru ||
    raw.description ||
    raw.preview_url ||
    '';

  const link =
    raw.link ||
    raw.url ||
    raw.tracking_url ||
    raw.preview_url ||
    raw.offer_url ||
    raw.lp_url ||
    null;

  return {
    id: String(raw.id || raw.offer_id || ''),
    title: raw.title || raw.name || `Offer ${raw.id || ''}`,
    category: categories[0] || raw.vertical || 'Dating',
    countries,
    api_payout: Number(apiPayout.toFixed ? apiPayout.toFixed(2) : apiPayout || 0),
    display_payout: displayPayout,
    currency: payment.currency || raw.currency || 'USD',
    description,
    flow: raw.flow || description,
    allowed_traffic: allowedTraffic,
    restricted_traffic: restrictedTraffic,
    preview_url: raw.preview_url || null,
    link
  };
}

async function fetchTraforce(path, params = {}) {
  if (!TRAFORCE_API_KEY) {
    throw new Error('TRAFORCE_API_KEY is missing');
  }

  const url = new URL(`${TRAFORCE_API_BASE}${path}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value));
    }
  });

  const response = await fetch(url.toString(), {
    method: 'GET',
    headers: {
      'API-Key': TRAFORCE_API_KEY,
      'Accept': 'application/json'
    }
  });

  if (!response.ok) {
    const raw = await response.text();
    throw new Error(`Traforce API ${response.status}: ${raw}`);
  }

  return response.json();
}

async function tryQuery(sql, params = []) {
  try {
    return await pool.query(sql, params);
  } catch (error) {
    console.error('Optional query failed:', error.message);
    return { rows: [] };
  }
}

async function ensureCpaTables() {
  await tryQuery(`
    create table if not exists cpa_offer_links (
      id serial primary key,
      telegram_id bigint not null,
      offer_id text not null,
      sub1 text,
      sub2 text,
      generated_url text,
      created_at timestamptz default now()
    )
  `);

  await tryQuery(`
    create table if not exists cpa_postback_logs (
      id serial primary key,
      payload jsonb,
      created_at timestamptz default now()
    )
  `);

  await tryQuery(`
    create table if not exists cpa_conversions (
      id serial primary key,
      telegram_id bigint,
      offer_id text,
      click_id text,
      sub1 text,
      sub2 text,
      status text,
      country text,
      payout_api numeric default 0,
      payout_user numeric default 0,
      service_margin numeric default 0,
      created_at timestamptz default now(),
      raw_payload jsonb
    )
  `);
}

async function updateUserUsdBalance(telegramId, delta) {
  const amount = safeNumber(delta, 0);
  await tryQuery(
    `update users
     set usd_balance = coalesce(usd_balance, 0) + $2
     where telegram_id = $1`,
    [telegramId, amount]
  );
}

app.post('/register', async (req, res) => {
  try {
    const { initData } = req.body;
    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);

    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const existing = await pool.query(
      'select * from users where telegram_id = $1',
      [user.id]
    );

    let dbUser;

    if (existing.rows.length === 0) {
     const inserted = await pool.query(
  `insert into users (telegram_id, username, first_name, photo_url)
   values ($1, $2, $3, $4) returning *`,
  [
    user.id,
    user.username || null,
    user.first_name || null,
    user.photo_url || null
  ]
);
      dbUser = inserted.rows[0];
    } else {
      const updated = await pool.query(
  `update users
   set username = $2,
       first_name = $3,
       photo_url = $4
   where telegram_id = $1
   returning *`,
  [
    user.id,
    user.username || null,
    user.first_name || null,
    user.photo_url || null
  ]
);

dbUser = updated.rows[0];
    }

    res.json({
      user: {
        telegram_id: dbUser.telegram_id,
        username: dbUser.username,
        first_name: dbUser.first_name,
        balance: dbUser.balance,
        plan: dbUser.plan
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Server error' });
  }
});

app.post('/spend', async (req, res) => {
  try {
    const { initData, action, amount } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) return res.status(401).json({ message: 'Invalid user' });

    const result = await pool.query(
      'select * from users where telegram_id = $1',
      [user.id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Пользователь не найден' });
    }

    const dbUser = result.rows[0];

    if (dbUser.balance < amount) {
      return res.status(400).json({ message: 'Недостаточно средств' });
    }

    const updated = await pool.query(
      'update users set balance = balance - $2 where telegram_id = $1 returning *',
      [user.id, amount]
    );

    const updatedUser = updated.rows[0];

    await addTransaction({
      telegram_id: user.id,
      type: 'spend',
      amount: amount,
      credits: -amount,
      status: 'completed',
      description: action || 'Списание кредитов'
    });

    res.json({
      user: {
        telegram_id: updatedUser.telegram_id,
        username: updatedUser.username,
        first_name: updatedUser.first_name,
        balance: updatedUser.balance,
        plan: updatedUser.plan
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Server error' });
  }
});

app.post('/create-topup-link', async (req, res) => {
  try {
    const { initData, amount, credits } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const payload = `topup:${user.id}:${credits}:${Date.now()}`;

    const data = await telegramApi('createInvoiceLink', {
      title: 'Пополнение баланса',
      description: `${credits} кредитов для UBT ToolKit`,
      payload,
      currency: 'XTR',
      prices: [
        {
          label: `${credits} credits`,
          amount: amount
        }
      ]
    });

    if (!data.ok) {
      return res.status(400).json({
        message: data.description || 'Telegram invoice error'
      });
    }

    res.json({ invoice_link: data.result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Server error' });
  }
});

app.post('/telegram-webhook', async (req, res) => {
  try {
    const update = req.body;

    if (update.pre_checkout_query) {
      await telegramApi('answerPreCheckoutQuery', {
        pre_checkout_query_id: update.pre_checkout_query.id,
        ok: true
      });

      return res.json({ ok: true });
    }

    if (update.message && update.message.successful_payment) {
      const payment = update.message.successful_payment;
      const payload = payment.invoice_payload || '';

      if (payment.currency === 'XTR' && payload.startsWith('topup:')) {
        const parts = payload.split(':');
        const telegramId = Number(parts[1]);
        const credits = Number(parts[2]);

        if (telegramId && credits) {
          await pool.query(
            'update users set balance = balance + $2 where telegram_id = $1 returning *',
            [telegramId, credits]
          );

          await addTransaction({
            telegram_id: telegramId,
            type: 'topup',
            amount: payment.total_amount,
            credits: credits,
            status: 'completed',
            description: 'Пополнение через Telegram Stars'
          });
        }
      }

      return res.json({ ok: true });
    }

    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Webhook error' });
  }
});

app.post('/transactions', async (req, res) => {
  try {
    const { initData } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const result = await pool.query(
      `select id, type, amount, credits, status, description, created_at
       from transactions
       where telegram_id = $1
       order by created_at desc
       limit 30`,
      [user.id]
    );

    res.json({
      transactions: result.rows
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Server error' });
  }
});

app.post('/smart-link/create', async (req, res) => {
  try {
    const { initData, original_url } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const normalizedUrl = normalizeUrl(original_url);

    if (!normalizedUrl) {
      return res.status(400).json({ message: 'Некорректная ссылка' });
    }

    await spendUserCredits(user.id, 2, 'Создание смарт-ссылки');

    const shortCode = await generateUniqueShortCode();
    const shortUrl = `${PUBLIC_BASE_URL}/r/${shortCode}`;

    const result = await pool.query(
      `insert into smart_links (user_id, original_url, short_code, short_url, clicks, unique_clicks)
       values ($1, $2, $3, $4, 0, 0)
       returning *`,
      [user.id, normalizedUrl, shortCode, shortUrl]
    );

    res.json({
      ok: true,
      link: result.rows[0]
    });
  } catch (error) {
    console.error(error);

    if (error.message === 'Недостаточно средств' || error.message === 'Пользователь не найден') {
      return res.status(400).json({ message: error.message });
    }

    res.status(500).json({ message: 'Server error' });
  }
});

app.post('/smart-link/list', async (req, res) => {
  try {
    const { initData, limit = 5 } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const result = await pool.query(
      `select id, original_url, short_code, short_url, clicks, unique_clicks, created_at
       from smart_links
       where user_id = $1
       order by created_at desc
       limit $2`,
      [user.id, Number(limit)]
    );

    res.json({
      ok: true,
      links: result.rows
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Server error' });
  }
});

app.post('/smart-link/stats', async (req, res) => {
  try {
    const { initData, smart_link_id } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const linkResult = await pool.query(
      `select id, original_url, short_url, clicks, unique_clicks
       from smart_links
       where id = $1 and user_id = $2
       limit 1`,
      [smart_link_id, user.id]
    );

    if (linkResult.rows.length === 0) {
      return res.status(404).json({ message: 'Ссылка не найдена' });
    }

    const clicksResult = await pool.query(
      `select country, device
       from smart_link_clicks
       where smart_link_id = $1`,
      [smart_link_id]
    );

    const dailyResult = await pool.query(
      `select
          to_char(created_at::date, 'YYYY-MM-DD') as date,
          count(*)::int as clicks,
          count(distinct ip_hash)::int as unique_clicks
       from smart_link_clicks
       where smart_link_id = $1
       group by created_at::date
       order by created_at::date asc`,
      [smart_link_id]
    );

    const countries = {};
    const devices = {};

    for (const row of clicksResult.rows) {
      const country = row.country || 'Unknown';
      const device = row.device || 'Unknown';

      countries[country] = (countries[country] || 0) + 1;
      devices[device] = (devices[device] || 0) + 1;
    }

    res.json({
      ok: true,
      stats: {
        ...linkResult.rows[0],
        countries,
        devices,
        daily: dailyResult.rows || []
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ message: 'Server error' });
  }
});

app.get('/r/:code', async (req, res) => {
  try {
    const { code } = req.params;

    const linkResult = await pool.query(
      `select *
       from smart_links
       where short_code = $1
       limit 1`,
      [code]
    );

    if (linkResult.rows.length === 0) {
      return res.status(404).send('Link not found');
    }

    const link = linkResult.rows[0];
    const ip = getClientIp(req);
    const ipHash = hashIp(ip);
    const userAgent = req.headers['user-agent'] || '';
    const geo = geoip.lookup(ip);
    const country = geo?.country || 'Unknown';
    const device = detectDevice(userAgent);

    const uniqueResult = await pool.query(
      `select id
       from smart_link_clicks
       where smart_link_id = $1 and ip_hash = $2
       limit 1`,
      [link.id, ipHash]
    );

    const isUnique = uniqueResult.rows.length === 0;

    await pool.query(
      `insert into smart_link_clicks (smart_link_id, ip_hash, country, device, user_agent)
       values ($1, $2, $3, $4, $5)`,
      [link.id, ipHash, country, device, userAgent]
    );

    if (isUnique) {
      await pool.query(
        `update smart_links
         set clicks = clicks + 1,
             unique_clicks = unique_clicks + 1
         where id = $1`,
        [link.id]
      );
    } else {
      await pool.query(
        `update smart_links
         set clicks = clicks + 1
         where id = $1`,
        [link.id]
      );
    }

    return res.redirect(link.original_url);
  } catch (error) {
    console.error(error);
    return res.status(500).send('Internal server error');
  }
});

// =======================
// CPA MODULE START
// =======================

app.post('/cpa/offers', async (req, res) => {
  try {
    const user = getUserIdentityFromBody(req);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const data = await fetchTraforce('/offers', {
      limit: 50,
      page: 1,
    });

    const items = Array.isArray(data?.offers)
      ? data.offers
      : Array.isArray(data?.data)
        ? data.data
        : Array.isArray(data)
          ? data
          : [];

    const offers = items
      .map((rawOffer) => {
        const parsed = parseTraforceOffer(rawOffer);
        if (parsed.id) {
          cpaOffersCache.set(parsed.id, { parsed, raw: rawOffer });
        }
        return parsed;
      })
      .filter((offer) => offer.id);

    res.json({ ok: true, offers });
  } catch (error) {
    console.error('CPA offers error:', error);
    res.status(500).json({ message: error.message || 'CPA offers error' });
  }
});

app.post('/cpa/dashboard', async (req, res) => {
  try {
    const user = getUserIdentityFromBody(req);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const userResult = await pool.query(
      `select telegram_id,
              coalesce(username, '') as username,
              coalesce(first_name, '') as first_name,
              coalesce(photo_url, '') as photo_url,
              coalesce(usd_balance, 0) as usd_balance,
              coalesce(usd_hold, 0) as usd_hold,
              coalesce(total_withdrawn, 0) as total_withdrawn
       from users
       where telegram_id = $1
       limit 1`,
      [user.id]
    );

    if (!userResult.rows.length) {
      return res.status(404).json({ message: 'Пользователь не найден' });
    }

    const statsResult = await tryQuery(
      `select
          count(*)::int as conversions,
          coalesce(sum(payout_user), 0) as revenue,
          coalesce(sum(case when status = 'approved' then payout_user else 0 end), 0) as approved_revenue
       from cpa_conversions
       where telegram_id = $1`,
      [user.id]
    );

    const clicksResult = await tryQuery(
      `select count(*)::int as clicks
       from cpa_offer_links
       where telegram_id = $1`,
      [user.id]
    );

    const conversions = statsResult.rows[0] || {};
    const clicks = clicksResult.rows[0] || {};

    res.json({
      ok: true,
      dashboard: {
        user: userResult.rows[0],
        balance: {
          available: safeNumber(userResult.rows[0].usd_balance, 0),
          hold: safeNumber(userResult.rows[0].usd_hold, 0),
          total_withdrawn: safeNumber(userResult.rows[0].total_withdrawn, 0)
        },
        stats: {
          clicks: safeNumber(clicks.clicks, 0),
          conversions: safeNumber(conversions.conversions, 0),
          revenue: Number(safeNumber(conversions.revenue, 0).toFixed(2)),
          approved_revenue: Number(safeNumber(conversions.approved_revenue, 0).toFixed(2))
        }
      }
    });
  } catch (error) {
    console.error('CPA dashboard error:', error);
    res.status(500).json({ message: error.message || 'CPA dashboard error' });
  }
});


app.post('/cpa/statistics', async (req, res) => {
  try {
    const user = getUserIdentityFromBody(req);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const {
      limit = 100,
      status = '',
      sub_type = '',
      sub_value = '',
      geo = '',
      date_from = '',
      date_to = ''
    } = req.body || {};

    const values = [user.id];
    let where = 'where telegram_id = $1';

    if (status) {
      values.push(String(status).trim().toLowerCase());
      where += ` and lower(status) = $${values.length}`;
    }

    if (sub_type === 'sub1' && sub_value) {
      values.push(String(sub_value).trim());
      where += ` and sub1 = $${values.length}`;
    }

    if (sub_type === 'sub2' && sub_value) {
      values.push(String(sub_value).trim());
      where += ` and sub2 = $${values.length}`;
    }

    if (geo) {
      values.push(String(geo).trim().toUpperCase());
      where += ` and upper(country) = $${values.length}`;
    }

    if (date_from) {
      values.push(String(date_from));
      where += ` and created_at::date >= $${values.length}::date`;
    }

    if (date_to) {
      values.push(String(date_to));
      where += ` and created_at::date <= $${values.length}::date`;
    }

    const summaryValues = values.slice();
    values.push(Number(limit) > 0 ? Number(limit) : 100);

    const itemsResult = await tryQuery(
      `select
          id,
          telegram_id,
          offer_id,
          click_id,
          sub1,
          sub2,
          status,
          country,
          payout_api,
          payout_user,
          service_margin,
          created_at
       from cpa_conversions
       ${where}
       order by created_at desc
       limit $${values.length}`,
      values
    );

    const summaryResult = await tryQuery(
      `select
          count(*)::int as total_conversions,
          coalesce(sum(payout_api), 0) as payout_api_total,
          coalesce(sum(payout_user), 0) as payout_user_total,
          coalesce(sum(service_margin), 0) as service_margin_total
       from cpa_conversions
       ${where}`,
      summaryValues
    );

    const geoStatsResult = await tryQuery(
      `select
          upper(coalesce(country, 'UNKNOWN')) as geo,
          count(*)::int as clicks,
          count(distinct coalesce(nullif(click_id, ''), id::text))::int as unique_clicks,
          count(*)::int as conversions,
          coalesce(sum(payout_api), 0) as payout_api_total
       from cpa_conversions
       ${where}
       group by upper(coalesce(country, 'UNKNOWN'))
       order by clicks desc, geo asc`,
      summaryValues
    );

    const geo_stats = (geoStatsResult.rows || []).map((row) => {
      const clicks = safeNumber(row.clicks, 0);
      const conversions = safeNumber(row.conversions, 0);
      const payoutApiTotal = safeNumber(row.payout_api_total, 0);
      return {
        geo: row.geo || 'UNKNOWN',
        clicks,
        unique_clicks: safeNumber(row.unique_clicks, 0),
        conversions,
        cr: clicks > 0 ? Number(((conversions / clicks) * 100).toFixed(2)) : 0,
        epc: clicks > 0 ? Number((payoutApiTotal / clicks).toFixed(4)) : 0
      };
    });

    res.json({
      ok: true,
      statistics: {
        items: itemsResult.rows || [],
        summary: {
          total_conversions: safeNumber(summaryResult.rows?.[0]?.total_conversions, 0),
          payout_api_total: Number(safeNumber(summaryResult.rows?.[0]?.payout_api_total, 0).toFixed(2)),
          payout_user_total: Number(safeNumber(summaryResult.rows?.[0]?.payout_user_total, 0).toFixed(2)),
          service_margin_total: Number(safeNumber(summaryResult.rows?.[0]?.service_margin_total, 0).toFixed(2))
        },
        geo_stats
      }
    });
  } catch (error) {
    console.error('CPA statistics error:', error);
    res.status(500).json({ message: error.message || 'CPA statistics error' });
  }
});

app.post('/cpa/generate-link', async (req, res) => {
  try {
    const user = getUserIdentityFromBody(req);
    if (!user) {
      return res.status(401).json({ message: 'Invalid user' });
    }

    const { offer_id, sub2 = '' } = req.body || {};
    if (!offer_id) {
      return res.status(400).json({ message: 'offer_id required' });
    }

    let cached = cpaOffersCache.get(String(offer_id));

    if (!cached) {
      try {
        const fresh = await fetchTraforce('/offers', {
          limit: 100,
          page: 1,
            });
        const items = Array.isArray(fresh?.offers)
          ? fresh.offers
          : Array.isArray(fresh?.data)
            ? fresh.data
            : [];
        for (const rawOffer of items) {
          const parsed = parseTraforceOffer(rawOffer);
          if (parsed.id) {
            cpaOffersCache.set(parsed.id, { parsed, raw: rawOffer });
          }
        }
        cached = cpaOffersCache.get(String(offer_id));
      } catch (e) {
        console.error('Refresh offers before link generation failed:', e);
      }
    }

    const tgId = String(user.id);
    const sub1 = tgId;
    const offer = cached?.parsed || {};

    let directTarget = offer.link || '';

    if (!directTarget && String(offer_id) === 'smartlink_adult_ww') {
      directTarget = TRAFORCE_SMARTLINK_ADULT_WW;
    }

    if (!directTarget && String(offer_id) === 'mainstream_dating') {
      directTarget = TRAFORCE_MAINSTREAM_LINK;
    }

    const trackingUrl = new URL(`${PUBLIC_BASE_URL}/cpa/go/${encodeURIComponent(String(offer_id))}`);
    trackingUrl.searchParams.set('sub1', sub1);
    if (sub2) {
      trackingUrl.searchParams.set('sub2', String(sub2));
    }
    if (directTarget) {
      trackingUrl.searchParams.set('target', directTarget);
    }

    const generatedUrl = trackingUrl.toString();

    await tryQuery(
      `insert into cpa_offer_links (telegram_id, offer_id, sub1, sub2, generated_url)
       values ($1, $2, $3, $4, $5)`,
      [user.id, String(offer_id), sub1, String(sub2 || ''), generatedUrl]
    );

    res.json({
      ok: true,
      link: generatedUrl,
      offer: {
        id: String(offer_id),
        title:
          offer.title ||
          (String(offer_id) === 'smartlink_adult_ww'
            ? 'Smartlink Adult Dating WW'
            : String(offer_id) === 'mainstream_dating'
              ? 'Mainstream Dating'
              : `Offer ${offer_id}`)
      }
    });
  } catch (error) {
    console.error('CPA generate-link error:', error);
    res.status(500).json({ message: error.message || 'CPA generate-link error' });
  }
});

app.get('/cpa/go/:offerId', async (req, res) => {
  try {
    const { offerId } = req.params;
    const { sub1 = '', sub2 = '', target = '' } = req.query || {};

    let baseLink = '';
    if (target) {
      baseLink = String(target);
    } else if (String(offerId) === 'smartlink_adult_ww' && TRAFORCE_SMARTLINK_ADULT_WW) {
      baseLink = TRAFORCE_SMARTLINK_ADULT_WW;
    } else if (String(offerId) === 'mainstream_dating' && TRAFORCE_MAINSTREAM_LINK) {
      baseLink = TRAFORCE_MAINSTREAM_LINK;
    } else {
      baseLink = `https://affiliate.traforce.com/v2/offer/${encodeURIComponent(String(offerId))}`;
    }

    const ip = getClientIp(req);
    const ipHash = hashIp(ip);
    const userAgent = req.headers['user-agent'] || '';
    const geo = geoip.lookup(ip);
    const country = geo?.country || 'UNKNOWN';

    await tryQuery(
      `insert into cpa_clicks (telegram_id, offer_id, sub1, sub2, country, ip_hash, user_agent)
       values ($1, $2, $3, $4, $5, $6, $7)`,
      [
        sub1 ? Number(sub1) : null,
        String(offerId),
        String(sub1 || ''),
        String(sub2 || ''),
        String(country || 'UNKNOWN'),
        ipHash,
        userAgent
      ]
    );

    const url = new URL(baseLink);
    if (sub1) url.searchParams.set('sub1', String(sub1));
    if (sub2) url.searchParams.set('sub2', String(sub2));

    return res.redirect(url.toString());
  } catch (error) {
    console.error('CPA redirect error:', error);
    return res.status(500).send('CPA redirect error');
  }
});

app.post('/cpa/postback/traforce', async (req, res) => {
  try {
    const payload = req.body || {};
    await tryQuery(
      `insert into cpa_postback_logs (payload) values ($1)`,
      [JSON.stringify(payload)]
    );

    const sub1 = payload.sub1 || payload.aff_sub1 || payload.sub_id_1 || null;
    const sub2 = payload.sub2 || payload.aff_sub2 || payload.sub_id_2 || null;
    const offerId = payload.offer_id || payload.offer || payload.offerid || null;
    const clickId = payload.clickid || payload.click_id || payload.aff_click_id || null;
    const status = payload.status || payload.goal || 'approved';
    const country = payload.country || payload.geo || null;
    const apiPayout = safeNumber(payload.payout || payload.sum || payload.revenue || 0, 0);
    const userPayout = Number((apiPayout * CPA_MARGIN).toFixed(2));
    const serviceMargin = Number((apiPayout - userPayout).toFixed(2));

    if (sub1) {
      await tryQuery(
        `insert into cpa_conversions
           (telegram_id, offer_id, click_id, sub1, sub2, status, country, payout_api, payout_user, service_margin, raw_payload)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          Number(sub1),
          offerId ? String(offerId) : null,
          clickId ? String(clickId) : null,
          String(sub1),
          sub2 ? String(sub2) : null,
          String(status),
          country ? String(country) : null,
          apiPayout,
          userPayout,
          serviceMargin,
          JSON.stringify(payload)
        ]
      );

      if (status !== 'rejected') {
        await updateUserUsdBalance(Number(sub1), userPayout);
      }
    }

    res.json({ ok: true });
  } catch (error) {
    console.error('CPA postback error:', error);
    res.status(500).json({ message: error.message || 'CPA postback error' });
  }
});

app.get('/cpa/postback/traforce', async (req, res) => {
  try {
    const payload = req.query || {};

    await tryQuery(
      `insert into cpa_postback_logs (payload) values ($1)`,
      [JSON.stringify(payload)]
    );

    const sub1 = payload.sub1 || payload.aff_sub1 || payload.sub_id_1 || null;
    const sub2 = payload.sub2 || payload.aff_sub2 || payload.sub_id_2 || null;
    const offerId = payload.offer_id || payload.offer || payload.offerid || null;
    const clickId = payload.clickid || payload.click_id || payload.aff_click_id || null;
    const status = payload.status || payload.goal || 'approved';
    const country = payload.country || payload.geo || null;
    const apiPayout = safeNumber(payload.payout || payload.sum || payload.revenue || 0, 0);
    const userPayout = Number((apiPayout * CPA_MARGIN).toFixed(2));
    const serviceMargin = Number((apiPayout - userPayout).toFixed(2));

    if (sub1) {
      await tryQuery(
        `insert into cpa_conversions
           (telegram_id, offer_id, click_id, sub1, sub2, status, country, payout_api, payout_user, service_margin, raw_payload)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          Number(sub1),
          offerId ? String(offerId) : null,
          clickId ? String(clickId) : null,
          String(sub1),
          sub2 ? String(sub2) : null,
          String(status),
          country ? String(country) : null,
          apiPayout,
          userPayout,
          serviceMargin,
          JSON.stringify(payload)
        ]
      );

      if (String(status).toLowerCase() !== 'rejected' && String(status).toLowerCase() !== 'declined') {
        await updateUserUsdBalance(Number(sub1), userPayout);
      }
    }

    res.send('ok');
  } catch (error) {
    console.error('CPA GET postback error:', error);
    res.status(500).send('error');
  }
});


// =======================
// CPA MODULE END
// =======================

// =======================
// DVIZH MODULE START
// =======================

// получить ленту
app.post('/dvizh/feed', async (req, res) => {
  try {
    const { initData } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) return res.status(401).json({ message: 'Invalid user' });

    const result = await pool.query(`
     select 
  p.id,
  p.text,
  p.image_urls,
  p.likes_count,
  p.views_count,
  p.created_at,
  u.telegram_id,
  u.first_name,
  u.username,
  u.photo_url
      from posts p
      join users u on u.telegram_id = p.user_id
      where p.status = 'approved'
      order by p.created_at desc
      limit 20
    `);

    res.json({
      ok: true,
      items: result.rows.map(row => ({
        id: row.id,
        text: row.text,
        imageUrls: row.image_urls || [],
        likesCount: row.likes_count,
        viewsCount: row.views_count,
        createdAt: row.created_at,
        author: {
          id: row.telegram_id,
          name: row.first_name,
          username: row.username,
          photoUrl: row.photo_url || ''
        }
      }))
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Server error' });
  }
});

// создать пост
app.post('/dvizh/create-post', upload.single('image'), async (req, res) => {
  try {
    const initData = req.body?.initData;
const text = req.body?.text;

    console.log('INITDATA:', initData);
console.log('BODY:', req.body);
console.log('FILE:', !!req.file);

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) return res.status(401).json({ message: 'Invalid user' });

    if (!text || text.trim().length === 0) {
      return res.status(400).json({ message: 'Пустой текст' });
    }
let imageUrls = [];

if (req.file && req.file.buffer) {
  const mime = req.file.mimetype || 'image/jpeg';
  const base64 = req.file.buffer.toString('base64');

  const dataUrl = `data:${mime};base64,${base64}`;
  imageUrls = [dataUrl];
}
    await spendUserCredits(user.id, 5, 'Публикация поста');

    await pool.query(
      `insert into posts (user_id, text, image_urls, status, likes_count, views_count)
      values ($1, $2, $3, 'pending', 0, 0)`,
      [user.id, text, JSON.stringify(imageUrls)]
    );

    res.json({ ok: true });
  } catch (e) {
    console.error(e);

    if (e.message === 'Недостаточно средств') {
      return res.status(400).json({ message: e.message });
    }

    res.status(500).json({ message: 'Server error' });
  }
});

// лайк
app.post('/dvizh/toggle-like', async (req, res) => {
  try {
    const { initData, postId } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) return res.status(401).json({ message: 'Invalid user' });

    const existing = await pool.query(
      `select id from post_likes where post_id = $1 and user_id = $2`,
      [postId, user.id]
    );

    let liked;

    if (existing.rows.length > 0) {
      await pool.query(
        `delete from post_likes where post_id = $1 and user_id = $2`,
        [postId, user.id]
      );

      await pool.query(
        `update posts set likes_count = GREATEST(likes_count - 1, 0) where id = $1`,
        [postId]
      );

      liked = false;
    } else {
      await pool.query(
        `insert into post_likes (post_id, user_id) values ($1, $2)`,
        [postId, user.id]
      );

      await pool.query(
        `update posts set likes_count = likes_count + 1 where id = $1`,
        [postId]
      );

      liked = true;
    }

    res.json({ ok: true, liked });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Server error' });
  }
});

// просмотры
app.post('/dvizh/add-view', async (req, res) => {
  try {
    const { initData, postId } = req.body;

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) return res.status(401).json({ message: 'Invalid user' });

    const existing = await pool.query(
      `select id from post_views where post_id = $1 and user_id = $2`,
      [postId, user.id]
    );

    if (existing.rows.length === 0) {
      await pool.query(
        `insert into post_views (post_id, user_id) values ($1, $2)`,
        [postId, user.id]
      );

      await pool.query(
        `update posts set views_count = views_count + 1 where id = $1`,
        [postId]
      );
    }

    res.json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Server error' });
  }
});
// ===== ADMIN: получить pending посты =====
app.post('/dvizh/pending', async (req, res) => {
  try {
    const { initData } = req.body || {};

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) return res.status(401).json({ message: 'Invalid user' });

    if (!isAdmin(user)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    const result = await pool.query(`
      select
        p.id,
        p.text,
        p.image_urls,
        p.status,
        p.likes_count,
        p.views_count,
        p.created_at,
        u.telegram_id,
        u.first_name,
        u.username,
        u.photo_url
      from posts p
      join users u on u.telegram_id = p.user_id
      where p.status = 'pending'
      order by p.created_at desc
    `);

    res.json({
      ok: true,
      items: result.rows
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Server error' });
  }
});


// ===== ADMIN: approve / reject =====
app.post('/dvizh/moderate', async (req, res) => {
  try {
    const { initData, postId, action } = req.body || {};

    const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
    if (!user) return res.status(401).json({ message: 'Invalid user' });

    if (!isAdmin(user)) {
      return res.status(403).json({ message: 'Access denied' });
    }

    if (!postId) {
      return res.status(400).json({ message: 'postId required' });
    }

    if (!['approve', 'reject'].includes(action)) {
      return res.status(400).json({ message: 'Invalid action' });
    }

    const status = action === 'approve' ? 'approved' : 'rejected';

    const result = await pool.query(
      `update posts
       set status = $2
       where id = $1
       returning id, status`,
      [postId, status]
    );

    res.json({
      ok: true,
      post: result.rows[0]
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ message: 'Server error' });
  }
});
// =======================
// DVIZH MODULE END
// =======================
ensureCpaTables().finally(() => {
  app.listen(port, () => {
    console.log('Server running on port ' + port);
  });
});
