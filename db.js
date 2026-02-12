const { Pool } = require('pg');

const DB_USER = "sophiademodev"
const DB_PASSWORD = "Postgressophiadev@2024"
const DB_NAME = 'sophia-dev'
const DB_HOST = '20.67.48.60'
const DB_PORT = 5432

const pool = new Pool({
  host: DB_HOST,
  user: DB_USER,
  password: DB_PASSWORD,
  database: DB_NAME,
  port: DB_PORT,
});

module.exports = {
  query: (text, params) => pool.query(text, params),
};
