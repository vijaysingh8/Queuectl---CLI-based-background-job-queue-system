
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

node "$ROOT/src/cli.js" config set backoff_base 2
node "$ROOT/src/cli.js" enqueue --% "{\"id\":\"job30\",\"command\":\"echo Hello from QueueCTL\"}"
node "$ROOT/src/cli.js" enqueue --% "{\"id\":\"job4\",\"command\":\"echo Hello from QueueCTL\"}"

node "$ROOT/src/cli.js" worker start --count 2

echo "Waiting 7 seconds..."
sleep 7

node "$ROOT/src/cli.js" status
node "$ROOT/src/cli.js" list --state completed || true
node "$ROOT/src/cli.js" list --state failed || true
node "$ROOT/src/cli.js" dlq list || true

node "$ROOT/src/cli.js" worker stop
