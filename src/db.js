
const { MongoClient } = require('mongodb');
const { mongoUri } = require('./env');

let client = null;
let db = null;

async function getDB() {
  if (!client) {
    client = new MongoClient(mongoUri(), { maxPoolSize: 20 });
    await client.connect();
    const url = new URL(mongoUri());
    const dbName = url.pathname.replace(/^\//, '') || 'queuectl';
    db = client.db(dbName);
  }
  return db;
}

async function closeDB() {
  if (client) {
    try { await client.close(); } catch (e) {}
  }
  client = null;
  db = null;
}

module.exports = { getDB, closeDB };
