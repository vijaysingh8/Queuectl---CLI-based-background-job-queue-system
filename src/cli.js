#!/usr/bin/env node
const { Command } = require('commander');
const chalk = require('chalk');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn } = require('child_process');

const { enqueue, listJobsByState, counters, retryFromDLQ } = require('./queue');
const { getAllConfig, setConfig } = require('./config');
const { migrate } = require('./migrate');
const { dataDir } = require('./env');
const { getDB, closeDB } = require('./db');

(async () => {
  await migrate();

  const program = new Command();
  program
    .name('queuectl')
    .description('CLI background job queue (MongoDB + bonus features)')
    .version('0.3.0');

  function needsWindowsWrap(cmd) {
    if (process.platform !== 'win32') return false;
    if (!cmd || typeof cmd !== 'string') return false;
    const trimmed = cmd.trim();
    if (/^(cmd\s+\/c|powershell|pwsh|node|bash|sh|wsl)\b/i.test(trimmed)) return false;
    if (/[|><&"'`]/.test(trimmed)) return false;
    if (/^(echo|dir|type|copy|mkdir|rmdir|del|move|cls|ping|tasklist|whoami|ipconfig)\b/i.test(trimmed)) return true;
    return false;
  }


  program
    .command('enqueue')
    .argument('<json>', 'Job JSON')
    .description('Add a new job to the queue')
    .action(async (jsonStr) => {
      let obj;
      try { obj = JSON.parse(jsonStr); }
      catch (e) { console.error(chalk.red('Invalid JSON')); process.exit(1); }

      try {
        if (obj && typeof obj.command === 'string' && needsWindowsWrap(obj.command)) {
          obj.command = `cmd /c ${obj.command}`;
        }
      } catch (e) {}

      try {
        const id = await enqueue(obj);
        console.log(chalk.green(`Enqueued job ${id}`));
      } catch (e) {
        console.error(chalk.red(String(e)));
        process.exit(1);
      } finally {
        await closeDB();
      }
    });

  
  program
    .command('file:write')
    .argument('<filepath>')
    .argument('<content...>')
    .option('--id <id>')
    .description('Enqueue a file:write job (writes content to path)')
    .action(async (filepath, contentParts, opts) => {
      const id = opts.id || `file-write-${Date.now()}`;
      const content = Array.isArray(contentParts) ? contentParts.join(' ') : String(contentParts || '');
      const cmd = `file:write ${filepath} ${content}`;
      try {
        await enqueue({ id, command: cmd, priority: 0 });
        console.log(chalk.green(`Enqueued file:write job ${id} -> ${filepath}`));
      } catch (e) {
        console.error(chalk.red(String(e)));
        process.exit(1);
      } finally {
        await closeDB();
      }
    });

 
  program
    .command('file:read')
    .argument('<filepath>')
    .option('--id <id>')
    .description('Enqueue a file:read job (returns tail of file as output)')
    .action(async (filepath, opts) => {
      const id = opts.id || `file-read-${Date.now()}`;
      const cmd = `file:read ${filepath}`;
      try {
        await enqueue({ id, command: cmd, priority: 0 });
        console.log(chalk.green(`Enqueued file:read job ${id} -> ${filepath}`));
      } catch (e) {
        console.error(chalk.red(String(e)));
        process.exit(1);
      } finally {
        await closeDB();
      }
    });

 
  const worker = program.command('worker').description('Manage workers');

  worker
    .command('start')
    .option('--count <n>', 'Number of workers', '1')
    .description('Start N workers (detached)')
    .action(async (opts) => {
      const n = parseInt(opts.count, 10) || 1;
      const pids = [];
      for (let i = 0; i < n; i++) {
        const child = spawn(process.execPath, [path.join(__dirname, 'worker.js')], {
          stdio: 'ignore',
          detached: true,
        });
        child.unref();
        pids.push(child.pid);
      }
      savePIDs(pids, true);
      console.log(chalk.green(`Started ${n} worker(s): ${pids.join(', ')}`));
      await closeDB();
    });

 
  worker
    .command('stop')
    .description('Gracefully stop workers that were started via CLI')
    .action(async () => {
      let pids = readPIDs();

      
      if (!pids || pids.length === 0) {
        try {
          const db = await getDB();
          pids = (await db.collection('workers').find({}).project({ pid: 1 }).toArray()).map(w => w.pid).filter(Boolean);
          await closeDB();
        } catch (e) {}
      }

      if (!pids || pids.length === 0) { console.log('No worker PIDs recorded.'); return; }

      let stopped = 0;
      for (const pid of pids) {
        try { process.kill(pid, 'SIGTERM'); stopped++; } catch (e) { /* already dead or permission */ }
      }

     
      try {
        const db = await getDB();
        const now = new Date().toISOString();
        const releaseRes = await db.collection('jobs').updateMany(
          { state: 'processing', locked_by: { $in: pids } },
          { $set: { state: 'pending', locked_by: null, locked_at: null, updated_at: now, next_run_at: now } }
        );
        console.log(chalk.yellow(`Released ${releaseRes.modifiedCount} job(s) locked by stopped workers.`));
        const del = await db.collection('workers').deleteMany({ pid: { $in: pids } });
        console.log(chalk.yellow(`Deleted ${del.deletedCount} worker row(s) from DB.`));
        await closeDB();
      } catch (e) {
        console.error('Error releasing jobs / deleting workers in DB:', e && e.message ? e.message : String(e));
      }

      savePIDs([], false);
      console.log(chalk.yellow(`Sent SIGTERM to ${stopped} worker(s).`));
    });

 
  program
    .command('status')
    .description('Show summary of job states, metrics & active workers')
    .action(async () => {
      const c = await counters();
      console.log(chalk.cyan('Job counts & metrics:'));
      console.table(c);

      const db = await getDB();
      const workers = await db.collection('workers').find({}).sort({ started_at: 1 }).toArray();
      console.log(chalk.cyan('Workers:'));
      if (workers.length === 0) console.log('No active workers.');
      else console.table(workers.map(w => ({
        pid: w.pid,
        started_at: w.started_at,
        heartbeat_at: w.heartbeat_at
      })));
      await closeDB();
    });


  program
    .command('list')
    .option('--state <state>')
    .description('List jobs (optionally by state)')
    .action(async (opts) => {
      const db = await getDB();
      if (!opts.state) {
        const rows = await db.collection('jobs').find({}).sort({ priority: -1, created_at: 1 }).toArray();
        console.table(rows);
      } else {
        const rows = await listJobsByState(opts.state);
        console.table(rows);
      }
      await closeDB();
    });

  
  program
    .command('dlq:list')
    .description('List dead-letter queue jobs')
    .action(async () => {
      const rows = await listJobsByState('dead');
      if (rows.length === 0) console.log('DLQ empty.');
      else console.table(rows);
      await closeDB();
    });

  program
    .command('dlq:retry')
    .argument('<id>')
    .description('Retry a dead job by id (moves to pending with attempts=0)')
    .action(async (id) => {
      try {
        await retryFromDLQ(id);
        console.log(chalk.green(`Moved ${id} back to pending.`));
      } catch (e) {
        console.error(chalk.red(String(e)));
        process.exit(1);
      } finally {
        await closeDB();
      }
    });

  
  program
    .command('config:get')
    .description('Show config')
    .action(async () => {
      console.table(await getAllConfig());
      await closeDB();
    });

  program
    .command('config:set')
    .argument('<key>')
    .argument('<value>')
    .description('Set a configuration value (numbers auto-parsed)')
    .action(async (key, value) => {
      let parsed = value;
      if (/^-?\d+(\.\d+)?$/.test(value)) parsed = Number(value);
      await setConfig(key, parsed);
      console.log(chalk.green(`Set ${key}=${parsed}`));
      await closeDB();
    });

  
  program
    .command('maint:reclaim')
    .description('Reclaim stuck processing jobs (mark as failed with next_run_at=now)')
    .action(async () => {
      const db = await getDB();
      const res = await db.collection('jobs').updateMany(
        { state: 'processing' },
        {
          $set: {
            state: 'failed',
            locked_by: null,
            locked_at: null,
            updated_at: new Date().toISOString(),
            next_run_at: new Date().toISOString(),
          },
        }
      );
      console.log(`Reclaimed ${res.modifiedCount} stuck jobs.`);
      await closeDB();
    });


  program
    .command('cancel')
    .argument('<id>')
    .description('Cancel a job (mark as canceled, will not be picked by workers)')
    .action(async (id) => {
      const db = await getDB();
      const res = await db.collection('jobs').updateOne(
        { id },
        { $set: { state: 'canceled', updated_at: new Date().toISOString(), locked_by: null, locked_at: null } }
      );
      if (res.matchedCount === 0) console.log(chalk.yellow('No such job.'));
      else console.log(chalk.green(`Canceled job ${id}`));
      await closeDB();
    });

  
  program
    .command('maint:drain-once')
    .description('Claim one job and run it in the foreground (debug helper)')
    .action(async () => {
      const { claimNextJob, completeJob, failJob } = require('./queue');
      const execa = require('execa');
      const job = await claimNextJob(process.pid);
      if (!job) { console.log('No job to process.'); await closeDB(); return; }

      console.log('Running job:', job.id, job.command);
      try {
        const { stdout, stderr } = await execa(job.command, { shell: true, timeout: 60_000 });
        const out = [stdout, stderr].filter(Boolean).join('\n').slice(0, 4000);
        await completeJob(job.id, 0, out);
        console.log(chalk.green(`Completed: ${job.id}`));
      } catch (err) {
        const out = [err.stdout||'', err.stderr||'', err.shortMessage||err.message||String(err)]
          .filter(Boolean).join('\n').slice(0, 4000);
        await failJob(job.id, out, typeof err.exitCode === 'number' ? err.exitCode : 1);
        console.log(chalk.red(`Failed: ${job.id}`));
      } finally {
        await closeDB();
      }
    });

  await program.parseAsync(process.argv);
})();


function savePIDs(pids, append = false) {
  const f = path.join(dataDir(), 'workers.json');
  let current = [];
  if (append && fs.existsSync(f)) {
    try { current = JSON.parse(fs.readFileSync(f, 'utf8')); } catch {}
  }
  const out = append ? [...current, ...pids] : pids;
  fs.writeFileSync(f, JSON.stringify(out, null, 2));
}
function readPIDs() {
  const f = path.join(dataDir(), 'workers.json');
  if (!fs.existsSync(f)) return [];
  try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return []; }
}
