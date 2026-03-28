const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const dotenv = require('dotenv');
const { Pool } = require('pg');

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

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
        `insert into users (telegram_id, username, first_name)
         values ($1, $2, $3) returning *`,
        [user.id, user.username || null, user.first_name || null]
      );
      dbUser = inserted.rows[0];
    } else {
      dbUser = existing.rows[0];
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
    const { initData, amount } = req.body;

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
            'update users set balance = balance + $2 where telegram_id = $1',
            [telegramId, credits]
          );
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

app.listen(port, () => {
  console.log('Server running on port ' + port);
});
