
const fs = require('fs');
const os = require('os');
const path = require('path');

function dataDir() {
  const dir = path.join(os.homedir(), '.queuectl');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function pidFile() {
  return path.join(dataDir(), 'workers.json');
}

function logsDir() {
  const dir = path.join(dataDir(), 'logs');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function mongoUri() {
  return process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/queuectl';
}

module.exports = { dataDir, pidFile, logsDir, mongoUri };
