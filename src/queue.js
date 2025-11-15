
const { getDB } = require('./db');
const { getConfig } = require('./config');

function nowISO(){ return new Date().toISOString(); }
function secondsFromNow(sec){ return new Date(Date.now() + sec*1000).toISOString(); }

async function enqueue(job) {
  const db = await getDB();
  const maxRetries = job.max_retries ?? await getConfig('max_retries') ?? 3;
  const doc = {
    id: job.id,
    command: job.command,
    state: 'pending',
    attempts: 0,
    max_retries: maxRetries,
    created_at: job.created_at ?? nowISO(),
    updated_at: nowISO(),
    run_at: job.run_at ?? null,
    next_run_at: job.next_run_at ?? (job.run_at ?? null),
    priority: Number.isFinite(job.priority) ? job.priority : 0,
    locked_by: null,
    locked_at: null,
    last_error: null,
    exit_code: null,
    output: null,
    started_at: null,
    finished_at: null,
    duration_ms: null
  };
  if (!doc.id || !doc.command) throw new Error('Job must include id and command');
  await db.collection('jobs').insertOne(doc);
  return doc.id;
}

// Claim next job atomically. Prevent same pid from holding >1 processing job.
async function claimNextJob(pid) {
  const db = await getDB();
  const now = nowISO();

  try {
    // guard: if this pid already has a processing job, do not claim another
    const already = await db.collection('jobs').findOne({ locked_by: pid, state: 'processing' });
    if (already) return null;

    const query = {
      state: { $in: ['pending', 'failed'] },
      $and: [
        { $or: [{ next_run_at: null }, { next_run_at: { $lte: now } }] },
        { $or: [{ locked_by: null }, { locked_by: { $exists: false } }] }
      ]
    };

    const res = await db.collection('jobs').findOneAndUpdate(
      query,
      {
        $set: {
          state: 'processing',
          locked_by: pid,
          locked_at: now,
          updated_at: now,
          started_at: now,
        }
      },
      {
        sort: { priority: -1, created_at: 1 },
        returnDocument: 'after'
      }
    );

    if (!res) return null;
    return res.value || null;
  } catch (err) {
    try { console.error('claimNextJob db error:', err && err.message ? err.message : String(err)); } catch {}
    return null;
  }
}

async function completeJob(id, exitCode, output) {
  const db = await getDB();
  const now = Date.now();
  const job = await db.collection('jobs').findOne({ id });
  const started = job?.started_at ? new Date(job.started_at).getTime() : now;
  const duration = Math.max(0, now - started);

  if (exitCode === 0) {
    await db.collection('jobs').updateOne(
      { id },
      { $set: {
          state: 'completed',
          exit_code: 0,
          output: output ?? null,
          updated_at: nowISO(),
          locked_by: null, locked_at: null,
          finished_at: new Date().toISOString(),
          duration_ms: duration,
          last_error: null
        } }
    );
  } else {
    const attempts = (job?.attempts ?? 0) + 1;
    const maxRetries = job?.max_retries ?? 3;
    const base = await getConfig('backoff_base') ?? 2;
    const delay = Math.pow(base, attempts);
    if (attempts <= maxRetries) {
      await db.collection('jobs').updateOne(
        { id },
        { $set: {
            state: 'failed',
            attempts,
            next_run_at: secondsFromNow(delay),
            exit_code: exitCode,
            updated_at: nowISO(),
            locked_by: null, locked_at: null,
            finished_at: new Date().toISOString(),
            duration_ms: duration,
            last_error: null
          } }
      );
    } else {
      await db.collection('jobs').updateOne(
        { id },
        { $set: {
            state: 'dead',
            attempts,
            exit_code: exitCode,
            updated_at: nowISO(),
            locked_by: null, locked_at: null,
            finished_at: new Date().toISOString(),
            duration_ms: duration
          } }
      );
    }
  }
}

async function failJob(id, errorMsg, exitCode=1) {
  const db = await getDB();
  const job = await db.collection('jobs').findOne({ id });
  const now = Date.now();
  const started = job?.started_at ? new Date(job.started_at).getTime() : now;
  const duration = Math.max(0, now - started);

  const attempts = (job?.attempts ?? 0) + 1;
  const maxRetries = job?.max_retries ?? 3;
  const base = await getConfig('backoff_base') ?? 2;
  const delay = Math.pow(base, attempts);
  if (attempts <= maxRetries) {
    await db.collection('jobs').updateOne(
      { id },
      { $set: {
          state: 'failed',
          attempts,
          next_run_at: secondsFromNow(delay),
          last_error: String(errorMsg).slice(0, 2000),
          exit_code: exitCode,
          updated_at: nowISO(),
          locked_by: null, locked_at: null,
          finished_at: new Date().toISOString(),
          duration_ms: duration
        } }
    );
  } else {
    await db.collection('jobs').updateOne(
      { id },
      { $set: {
          state: 'dead',
          attempts,
          last_error: String(errorMsg).slice(0, 2000),
          exit_code: exitCode,
          updated_at: nowISO(),
          locked_by: null, locked_at: null,
          finished_at: new Date().toISOString(),
          duration_ms: duration
        } }
    );
  }
}

async function listJobsByState(state) {
  const db = await getDB();
  return await db.collection('jobs').find({ state }).sort({ priority: -1, created_at: 1 }).toArray();
}

async function counters() {
  const db = await getDB();
  const states = ['pending','processing','completed','failed','dead'];
  const out = {};
  for (const s of states) out[s] = await db.collection('jobs').countDocuments({ state: s });

  const recent = await db.collection('jobs')
    .find({ state: 'completed', duration_ms: { $ne: null } })
    .sort({ finished_at: -1 }).limit(50).toArray();
  const avg = recent.length ? Math.round(recent.reduce((a,j)=>a+(j.duration_ms||0),0)/recent.length) : 0;
  out['avg_completed_runtime_ms'] = avg;

  const oldest = await db.collection('jobs').find({ state: 'pending' }).sort({ created_at: 1 }).limit(1).toArray();
  out['oldest_pending_age_sec'] = oldest.length ? Math.max(0, (Date.now() - new Date(oldest[0].created_at).getTime())/1000|0) : 0;

  return out;
}

async function retryFromDLQ(id) {
  const db = await getDB();
  const job = await db.collection('jobs').findOne({ id });
  if (!job) throw new Error('Job not found');
  if (job.state !== 'dead') throw new Error('Only dead jobs can be retried');
  await db.collection('jobs').updateOne(
    { id },
    { $set: { state: 'pending', attempts: 0, next_run_at: job.run_at ?? null, updated_at: nowISO(), exit_code: null, last_error: null, started_at: null, finished_at: null, duration_ms: null } }
  );
}

module.exports = { enqueue, claimNextJob, completeJob, failJob, listJobsByState, counters, retryFromDLQ };
