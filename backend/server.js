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

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

function validateTelegramInitData(initData, botToken) {
  const urlParams = new URLSearchParams(initData);
  const hash = urlParams.get('hash');
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

  return JSON.parse(urlParams.get('user'));
}

app.post('/register', async (req, res) => {
  const { initData } = req.body;

  const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
  if (!user) return res.status(401).json({ message: 'Invalid user' });

  const existing = await pool.query(
    'select * from users where telegram_id = $1',
    [user.id]
  );

  let dbUser;

  if (existing.rows.length === 0) {
    const inserted = await pool.query(
      `insert into users (telegram_id, username, first_name)
       values ($1, $2, $3) returning *`,
      [user.id, user.username, user.first_name]
    );
    dbUser = inserted.rows[0];
  } else {
    dbUser = existing.rows[0];
  }

  res.json({ user: dbUser });
});

app.post('/spend', async (req, res) => {
  const { initData, amount } = req.body;

  const user = validateTelegramInitData(initData, process.env.BOT_TOKEN);
  if (!user) return res.status(401).json({ message: 'Invalid user' });

  const result = await pool.query(
    'select * from users where telegram_id = $1',
    [user.id]
  );

  const dbUser = result.rows[0];

  if (dbUser.balance < amount) {
    return res.status(400).json({ message: 'Недостаточно средств' });
  }

  const updated = await pool.query(
    'update users set balance = balance - $2 where telegram_id = $1 returning *',
    [user.id, amount]
  );

  res.json({ user: updated.rows[0] });
});

app.listen(port, () => {
  console.log('Server running on port ' + port);
});
