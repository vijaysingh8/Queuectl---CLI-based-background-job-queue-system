
const { getDB } = require('./db');

async function getConfig(key) {
  const db = await getDB();
  const row = await db.collection('config').findOne({ key });
  return row?.value;
}

async function setConfig(key, value) {
  const db = await getDB();
  await db.collection('config').updateOne({ key }, { $set: { key, value } }, { upsert: true });
}

async function getAllConfig() {
  const db = await getDB();
  const rows = await db.collection('config').find({}).toArray();
  const out = {};
  for (const r of rows) out[r.key] = r.value;
  return out;
}

module.exports = { getConfig, setConfig, getAllConfig };
