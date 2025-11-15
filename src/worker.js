
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const { getDB } = require('./db');
const { claimNextJob, completeJob, failJob } = require('./queue');
const { getConfig } = require('./config');
const { logsDir } = require('./env');

let stopping = false;
let busy = false;

function nowISO() { return new Date().toISOString(); }

async function registerWorker(pid) {
  const db = await getDB();
  try {
    await db.collection('workers').updateOne(
      { pid },
      { $set: { pid, started_at: nowISO(), heartbeat_at: nowISO() } },
      { upsert: true }
    );
    console.log(`[worker ${pid}] registered`);
  } catch (e) {
    console.error(`[worker ${pid}] registerWorker error:`, e && e.message ? e.message : String(e));
  }
}

async function heartbeat(pid) {
  const db = await getDB();
  try {
    await db.collection('workers').updateOne({ pid }, { $set: { heartbeat_at: nowISO() } });
  } catch (e) {
    console.error(`[worker ${pid}] heartbeat error:`, e && e.message ? e.message : String(e));
  }
}

async function unregisterWorker(pid) {
  const db = await getDB();
  try {
    await db.collection('workers').deleteOne({ pid });
    console.log(`[worker ${pid}] unregistered`);
  } catch (e) {
    console.error(`[worker ${pid}] unregisterWorker error:`, e && e.message ? e.message : String(e));
  }
}

async function reclaimStuckJobs() {
  const db = await getDB();
  const timeoutSec = (await getConfig('job_timeout_sec')) ?? 300;
  const staleMs = Math.floor(timeoutSec * 1000 * 1.2);
  const cutoff = new Date(Date.now() - staleMs);

  try {
    const stuck = await db.collection('jobs').find({
      state: 'processing',
      locked_at: { $lt: cutoff.toISOString() }
    }).toArray();

    if (!stuck.length) return;

    console.log(`[watchdog] reclaiming ${stuck.length} stuck job(s) (locked before ${cutoff.toISOString()})`);
    const base = (await getConfig('backoff_base')) ?? 2;

    for (const j of stuck) {
      const attempts = (j.attempts || 0) + 1;
      const willRetry = attempts < (j.max_retries ?? 3);
      const delay = Math.pow(base, attempts);
      await db.collection('jobs').updateOne(
        { id: j.id },
        {
          $set: {
            state: willRetry ? 'failed' : 'dead',
            attempts,
            next_run_at: willRetry ? new Date(Date.now() + delay * 1000).toISOString() : null,
            last_error: 'Stuck processing lock reclaimed by watchdog',
            exit_code: j.exit_code ?? 1,
            updated_at: nowISO(),
            locked_by: null,
            locked_at: null,
            finished_at: new Date().toISOString(),
            duration_ms: j.started_at ? (Date.now() - new Date(j.started_at).getTime()) : null
          }
        }
      );
      console.log(`[watchdog] job ${j.id} -> ${willRetry ? 'failed (retry scheduled)' : 'dead'}`);
    }
  } catch (e) {
    console.error('[watchdog] reclaimStuckJobs error:', e && e.message ? e.message : String(e));
  }
}


function killChildAndTree(child) {
  try {
    if (!child || typeof child.pid !== 'number') return;
    // graceful first
    try { child.kill('SIGTERM'); } catch (e) {}
    if (process.platform === 'win32') {
      try {
        const tk = spawn('taskkill', ['/PID', String(child.pid), '/T', '/F']);
        tk.on('error', ()=>{});
      } catch (e) {}
    } else {
      // ensure kill after short delay if still running
      setTimeout(()=>{ try{ child.kill('SIGKILL'); }catch(e){} }, 2000);
    }
  } catch (e) {  }
}


async function runJob(job) {
  const timeoutSec = (await getConfig('job_timeout_sec')) ?? 300;
  const cancelPollSec = (await getConfig('cancel_check_interval_sec')) ?? 2;
  const logFile = path.join(logsDir(), `${job.id}.log`);

  
  try { fs.mkdirSync(path.dirname(logFile), { recursive: true }); } catch (e) {}
  try { fs.appendFileSync(logFile, `[${nowISO()}] claimed ${job.id}\n`); } catch (e) {}

  const logStream = fs.createWriteStream(logFile, { flags: 'a' });
  function writeLog(data) {
    if (!data) return;
    try { logStream.write(String(data) + '\n'); } catch (e) {}
  }

  console.log(`[worker ${process.pid}] starting job ${job.id} (timeout ${timeoutSec}s)`);

  async function finalizeResult(exitCode, tail, errMsg) {
    try {
      const db = await getDB();
      const fresh = await db.collection('jobs').findOne({ id: job.id }, { projection: { state: 1, attempts: 1 } });
      if (fresh && fresh.state === 'canceled') {
        const now = nowISO();
        await db.collection('jobs').updateOne(
          { id: job.id },
          { $set: { state: 'pending', attempts: fresh.attempts ?? 0, locked_by: null, locked_at: null, updated_at: now, next_run_at: now } }
        );
        writeLog(`[${nowISO()}] job ${job.id} canceled during run -> returned to pending`);
        return;
      }
    } catch (e) {
      console.error(`[worker ${process.pid}] finalizeResult: error checking canceled state`, e && e.message ? e.message : e);
    }

    try {
      if (exitCode === 0) {
        await completeJob(job.id, 0, tail);
      } else {
        const msg = tail || errMsg || `Exited with code ${exitCode}`;
        await failJob(job.id, msg, exitCode);
      }
    } catch (e) {
      console.error(`[worker ${process.pid}] finalizeResult: error updating job result`, e && e.message ? e.message : e);
    }
  }

  return new Promise(async (resolve) => {
    let timedOut = false;
    let child = null;
    let cancelPollTimer = null;

    // inline file commands
    async function runFileCommand() {
      try {
        const parts = job.command.trim().split(/\s+/);
        const cmd = parts[0].toLowerCase();

        if (cmd === 'file:read') {
          const fp = job.command.slice('file:read'.length).trim();
          const resolved = path.isAbsolute(fp) ? fp : path.join(process.env.HOME || process.env.USERPROFILE || '.', fp);
          writeLog(`[${nowISO()}] file:read -> ${resolved}`);
          try {
            const content = fs.readFileSync(resolved, 'utf8');
            writeLog(content.slice(-4000));
            await finalizeResult(0, content.slice(-4000));
          } catch (e) {
            const msg = `file read error: ${e && e.message ? e.message : String(e)}`;
            writeLog(msg);
            await finalizeResult(1, msg, msg);
          }
          return;
        }

        if (cmd === 'file:write') {
          const rest = job.command.slice('file:write'.length).trim();
          const m = rest.match(/^(\S+)\s+([\s\S]*)$/);
          if (!m) {
            const msg = 'file:write requires a path and content';
            writeLog(msg);
            await finalizeResult(1, msg, msg);
            return;
          }
          const fp = m[1];
          const content = m[2];
          const resolved = path.isAbsolute(fp) ? fp : path.join(process.env.HOME || process.env.USERPROFILE || '.', fp);
          writeLog(`[${nowISO()}] file:write -> ${resolved}`);
          try {
            fs.mkdirSync(path.dirname(resolved), { recursive: true });
            fs.writeFileSync(resolved, content, 'utf8');
            writeLog(`wrote ${content.length} bytes`);
            await finalizeResult(0, `wrote ${content.length} bytes`);
          } catch (e) {
            const msg = `file write error: ${e && e.message ? e.message : String(e)}`;
            writeLog(msg);
            await finalizeResult(1, msg, msg);
          }
          return;
        }

        writeLog(`unknown inline command: ${job.command}`);
        await finalizeResult(1, `unknown inline command`);
      } finally {
        try { logStream.end(); } catch (e) {}
        resolve();
      }
    }

    if (/^file:(read|write)\b/i.test(job.command.trim())) {
      runFileCommand();
      return;
    }

    
    try {
      child = spawn(job.command, { shell: true, stdio: ['ignore', 'pipe', 'pipe'] });
    } catch (err) {
      const msg = `spawn error (sync): ${err && err.message ? err.message : String(err)}`;
      writeLog(msg);
      console.error(`[worker ${process.pid}] spawn error for ${job.id}:`, msg);
      try { await failJob(job.id, msg, 1); } catch (e) {}
      try { logStream.end(); } catch (e) {}
      resolve();
      return;
    }

    if (child.stdout) child.stdout.on('data', (d) => { const s = d.toString(); writeLog(s); process.stdout.write(`[${job.id}] stdout: ${s}`); });
    if (child.stderr) child.stderr.on('data', (d) => { const s = d.toString(); writeLog(s); process.stderr.write(`[${job.id}] stderr: ${s}`); });

    async function checkCancel() {
      try {
        const db = await getDB();
        const fresh = await db.collection('jobs').findOne({ id: job.id }, { projection: { state: 1 }});
        if (fresh && fresh.state === 'canceled') {
          writeLog(`[${nowISO()}] detected cancel -> killing child pid=${child && child.pid}`);
          killChildAndTree(child);
        }
      } catch (e) {}
    }
    cancelPollTimer = setInterval(checkCancel, cancelPollSec * 1000);

    const timer = setTimeout(() => {
      timedOut = true;
      writeLog(`[${nowISO()}] timed out after ${timeoutSec}s -> killing pid=${child && child.pid}`);
      console.warn(`[worker ${process.pid}] job ${job.id} timed out after ${timeoutSec}s — killing child pid=${child && child.pid}`);
      killChildAndTree(child);
    }, timeoutSec * 1000);

    child.on('error', async (err) => {
      clearTimeout(timer);
      if (cancelPollTimer) clearInterval(cancelPollTimer);
      const msg = `spawn error: ${err && err.message ? err.message : String(err)}`;
      writeLog(msg);
      console.error(`[worker ${process.pid}] spawn error for ${job.id}:`, msg);
      try { await failJob(job.id, msg, 1); } catch (e) {}
      try { logStream.end(); } catch {}
      resolve();
    });

    child.on('close', async (code, signal) => {
      clearTimeout(timer);
      if (cancelPollTimer) clearInterval(cancelPollTimer);
      const exitCode = (typeof code === 'number') ? code : (timedOut ? 137 : 1);
      let tail = '';
      try {
        if (fs.existsSync(logFile)) {
          const raw = fs.readFileSync(logFile, 'utf8');
          tail = raw.slice(-4000);
        }
      } catch (e) {
        console.error(`[worker ${process.pid}] error reading log tail:`, e && e.message ? e.message : String(e));
      }

      try {
        if (exitCode === 0) {
          writeLog(`[${nowISO()}] child exited 0 pid=${child && child.pid}`);
          console.log(`[worker ${process.pid}] job ${job.id} completed (exit 0)`);
          await finalizeResult(0, tail);
        } else {
          const msg = tail || `Exited with code ${exitCode}${signal ? ` signal:${signal}` : ''}`;
          writeLog(`[${nowISO()}] child exited ${exitCode} pid=${child && child.pid} msg=${msg}`);
          console.warn(`[worker ${process.pid}] job ${job.id} failed (exit ${exitCode})`);
          await finalizeResult(exitCode, msg, msg);
        }
      } catch (err) {
        console.error(`[worker ${process.pid}] error updating job result for ${job.id}:`, err && err.message ? err.message : String(err));
      } finally {
        try { logStream.end(); } catch {}
        resolve();
      }
    });
  });
}


async function loop() {
  const pid = process.pid;
  await registerWorker(pid);

  process.on('SIGTERM', () => { console.log(`[worker ${pid}] SIGTERM`); stopping = true; });
  process.on('SIGINT', () => { console.log(`[worker ${pid}] SIGINT`); stopping = true; });

  let tick = 0;

  while (!stopping) {
    try {
      await heartbeat(pid);
    } catch (e) { console.error(`[worker ${pid}] heartbeat loop error:`, e && e.message ? e.message : String(e)); }

    if ((tick++ % 6) === 0) {
      try { await reclaimStuckJobs(); } catch (e) { console.error('[worker] reclaimStuckJobs error:', e && e.message ? e.message : String(e)); }
    }

    if (busy) {
      await new Promise(r => setTimeout(r, 200));
      continue;
    }

    let job = null;
    try {
      job = await claimNextJob(pid);
    } catch (e) {
      console.error(`[worker ${pid}] claimNextJob error (outer):`, e && e.message ? e.message : String(e));
      job = null;
    }

    if (job) {
      console.log(`[worker ${pid}] claimed job ${job.id} at ${new Date().toISOString()}`);
      busy = true;
      try {
        await runJob(job);
      } catch (e) {
        console.error(`[worker ${pid}] runJob uncaught error for ${job.id}:`, e && e.message ? e.message : String(e));
      } finally {
        console.log(`[worker ${pid}] finished handling ${job.id} at ${new Date().toISOString()}`);
        busy = false;
      }
      continue;
    }

    await new Promise(r => setTimeout(r, 500));
  }

  await unregisterWorker(process.pid);
}

if (require.main === module) {
  console.log(`[worker ${process.pid}] starting`);
  loop().catch(err => {
    console.error(`[worker ${process.pid}] fatal error:`, err && err.stack ? err.stack : String(err));
    process.exit(1);
  });
} else {
  module.exports = { loop };
}
