🚀 queuectl – A Lightweight Background Job Queue (MongoDB + Node.js)
queuectl is a simple, production-ready CLI-based background job queue system built using Node.js + MongoDB.
Demo Video Link-https://drive.google.com/file/d/1MaVjH2VaWzMh4qDmnDuToQtuQCAHNlbT/view?usp=drivesdk
It supports:

Background job execution using worker processes
Automatic retries with exponential backoff
Dead-letter queue (DLQ)
File commands (file:read, file:write)
Job cancellation
Worker management
Debug tools
Job timeout handling
Job priority queues
Job output logging
Minimal web dashboard for monitoring
Scheduled/delayed jobs (run_at)
Windows-friendly command execution

📁 Project Structure
src/
 ├── cli.js           # Main CLI
 ├── worker.js        # Worker process loop
 ├── queue.js         # Job queue logic (enqueue, claim, fail, complete...)
 ├── db.js            # MongoDB connection
 ├── config.js        # System config
 ├── migrate.js       # DB migrations + indexes
 ├── env.js           # Paths, logs, dataDir, MONGODB_URI
 ├── server.js        #web dashboard

 🛠️ Installation
 1️⃣ Clone the project
 git clone <https://github.com/vijaysingh8/Queuectl---CLI-based-background-job-queue-system>
cd queuectl
2️⃣ Install dependencies
npm install
3️⃣ Ensure MongoDB is running
set MONGODB_URI=mongodb://127.0.0.1:27017/queuectl
4️⃣ Run DB migrations
node src/migrate.js

🚀 Usage
All commands start with:
queuectl <command>
Full CLI Command Reference
1️⃣ Enqueue Jobs
JSON job enqueue:
node .\src\cli.js enqueue --% "{\"id\":\"job3\",\"command\":\"echo Hello from QueueCTL\"}"
we can enqueue our job with priority,max_retries,backoff_base
2️⃣ File Commands
Write to a file:
queuectl file:write ".queuectl/test.txt" "hello world" --id file-write-1

Read from a file:
queuectl file:read ".queuectl/test.txt" --id file-read-1

3️⃣ Worker Management
Start workers (detached background processes):
queuectl worker start --count N
N=1,2,...

Stop running workers:
queuectl worker stop

Stop does 3 things:
Sends SIGTERM to worker PIDs
Releases jobs locked by dead workers
Removes worker rows from DB

4️⃣ Job Listing
List all jobs:
queuectl list

List jobs by state:
queuectl list --state pending
queuectl list --state processing
queuectl list --state completed
queuectl list --state failed
queuectl list --state dead
queuectl list --state canceled

5️⃣ DLQ (Dead Letter Queue)
Show dead jobs:
queuectl dlq:list

Retry a DLQ job:
queuectl dlq:retry <job-id>

6️⃣ Config Management:
Show all config:
queuectl config:get

Set config:
queuectl config:set job_timeout_sec 120
queuectl config:set max_retries 5

7️⃣ Maintenance Tools
Reclaim all stuck processing jobs:
queuectl maint:reclaim

Cancel a job:
queuectl cancel <id>

Run a single job in foreground (debug):
queuectl maint:drain-once


📌 Worker Logic

Workers:
Poll jobs every 500ms
Claim only 1 job at a time (PID locking)
Create a log under ~/.queuectl/logs/<id>.log
Auto-retry using exponential backoff
Move job to dead after max retries
Watchdog reclaims stuck jobs
Kill child processes properly (Windows & Linux)

🚀 queuectl – Simple Web Dashboard
A lightweight monitoring web dashboard for the queuectl background job system.
Start command for web dashboard:
npm run serve

--------Thank You-----------


 
