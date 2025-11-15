
const express = require('express');
const { enqueue, listJobsByState, counters, retryFromDLQ } = require('./queue');

const app = express();
app.use(express.json());

app.get('/', async (_req, res) => {
  const c = await counters();

  const states = ['pending', 'processing', 'completed', 'failed', 'dead'];
  const stateCards = states.map(s => {
    return `<div class="card"><h4>${s}</h4><iframe src="/jobs?state=${s}" width="100%" height="240" style="border:0"></iframe></div>`;
  }).join('\n');

  res.type('html').send(`
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8"/>
        <title>QueueCTL Dashboard</title>
        <style>
          body { font-family: sans-serif; margin: 2rem; }
          h1 { margin-top: 0; }
          table { border-collapse: collapse; width: 100%; }
          td, th { border: 1px solid #ddd; padding: 8px; }
          tr:nth-child(even){background-color: #f9f9f9;}
          .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; }
          .card { border: 1px solid #ddd; border-radius: 8px; padding: 1rem; overflow: auto; }
          textarea { width: 100%; font-family: monospace; }
          iframe { background: white; }
        </style>
      </head>
      <body>
        <h1>QueueCTL Dashboard</h1>
        <div class="grid">
          <div class="card">
            <h3>Metrics</h3>
            <pre>${JSON.stringify(c, null, 2)}</pre>
          </div>
          <div class="card">
            <h3>Actions</h3>
            <form id="enqueue">
              <label>Job JSON</label><br/>
              <textarea id="job" rows="6">{ "id": "demo", "command": "echo hello", "priority": 1 }</textarea><br/>
              <button type="submit">Enqueue</button>
            </form>
            <div id="last-result" style="margin-top:0.5rem;color:green"></div>
            <script>
              document.getElementById('enqueue').addEventListener('submit', async (e) => {
                e.preventDefault();
                try {
                  const text = document.getElementById('job').value;
                  const body = JSON.parse(text);
                  const r = await fetch('/enqueue', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                  });
                  const txt = await r.text();
                  document.getElementById('last-result').textContent = txt;
                  setTimeout(() => location.reload(), 700);
                } catch (err) {
                  alert('Invalid JSON or network error: ' + err.message);
                }
              });
            </script>
          </div>
        </div>

        <h3>Latest Jobs</h3>
        <div class="grid">
          ${stateCards}
        </div>
      </body>
    </html>
  `);
});

app.get('/metrics', async (_req, res) => {
  res.json(await counters());
});

app.get('/jobs', async (req, res) => {
  const state = String(req.query.state || 'pending');
  const jobs = await listJobsByState(state);
  const rows = jobs.slice(0, 200).map(j => `
    <tr>
      <td>${escapeHtml(j.id ?? '')}</td>
      <td>${j.priority ?? 0}</td>
      <td>${escapeHtml(j.state ?? '')}</td>
      <td>${j.attempts ?? 0}</td>
      <td>${j.exit_code ?? ''}</td>
      <td>${escapeHtml(j.created_at ?? '')}</td>
      <td>${escapeHtml(j.updated_at ?? '')}</td>
    </tr>
  `).join('\n');

  res.type('html').send(`
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8"/>
        <title>Jobs - ${escapeHtml(state)}</title>
        <style>
          body { font-family: sans-serif; margin: 0.5rem; }
          table { border-collapse: collapse; width: 100%; font-size: 13px; }
          td, th { border: 1px solid #ddd; padding: 6px; }
        </style>
      </head>
      <body>
        <table>
          <thead>
            <tr><th>id</th><th>priority</th><th>state</th><th>attempts</th><th>exit</th><th>created</th><th>updated</th></tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
      </body>
    </html>
  `);
});

app.post('/enqueue', async (req, res) => {
  try {
    const id = await enqueue(req.body || {});
    res.status(200).send(`Enqueued ${id}`);
  } catch (e) {
    res.status(400).send(String(e));
  }
});

app.post('/dlq/retry/:id', async (req, res) => {
  try {
    await retryFromDLQ(req.params.id);
    res.status(200).send('ok');
  } catch (e) {
    res.status(400).send(String(e));
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`QueueCTL dashboard on http://localhost:${PORT}`));


function escapeHtml(s) {
  if (typeof s !== 'string') return s;
  return s.replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
}
