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
      where p.status in ('approved', 'pending')
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

// =======================
// DVIZH MODULE END
// =======================
app.listen(port, () => {
  console.log('Server running on port ' + port);
});
