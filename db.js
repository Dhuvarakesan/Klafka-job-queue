const { Pool } = require('pg');


// DB_DEBUG_NOTICES=true
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
