
const { getDB, closeDB } = require('./db');

async function migrate() {
  const db = await getDB();
  const jobs = db.collection('jobs');
  const config = db.collection('config');
  const workers = db.collection('workers');

  await jobs.createIndex({ id: 1 }, { unique: true });
  await jobs.createIndex({ state: 1, next_run_at: 1, priority: -1, created_at: 1 });
  await jobs.createIndex({ created_at: 1 });
  await jobs.createIndex({ priority: -1 });

  await workers.createIndex({ pid: 1 }, { unique: true });

 
  const defaults = [
    { key: 'max_retries', value: 3 },
    { key: 'backoff_base', value: 2 },
    { key: 'job_timeout_sec', value: 90},
    { key: 'watchdog_heartbeat_sec', value: 90 } 
  ];
  for (const kv of defaults) {
    await config.updateOne({ key: kv.key }, { $setOnInsert: kv }, { upsert: true });
  }

  console.log('migrate: OK');
}

if (require.main === module) {
  migrate().then(()=>process.exit(0)).catch(e => { console.error(e); process.exit(1); });
} else {
  module.exports = { migrate };
}
