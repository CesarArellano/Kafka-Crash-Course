#!/usr/bin/env bash
# Generates a SCRAM-SHA-256 credential for the production Kafka listener and
# writes it into .env.production. Re-run any time to rotate the password —
# the old credential is overwritten both on the broker and in the env file.
#
# Usage: ./scripts/generate-production-credentials.sh [username]
set -euo pipefail

CONTAINER_NAME="${KAFKA_CONTAINER_NAME:-kafka}"
BOOTSTRAP="${KAFKA_ADMIN_BOOTSTRAP_SERVER:-localhost:9092}"
USERNAME="${1:-orders-service}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env.production"
EXAMPLE_FILE="$ROOT_DIR/.env.production.example"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  echo "Kafka container '$CONTAINER_NAME' isn't running. Start it first with: docker compose up -d" >&2
  exit 1
fi

# 32 random bytes, base64-encoded: cryptographically secure and long enough that
# brute-forcing SCRAM's salted hash isn't practical.
PASSWORD="$(openssl rand -base64 32)"

echo "Creating SCRAM-SHA-256 credentials for user '$USERNAME' on the broker..."
docker exec "$CONTAINER_NAME" kafka-configs \
  --bootstrap-server "$BOOTSTRAP" \
  --alter \
  --add-config "SCRAM-SHA-256=[iterations=8192,password=${PASSWORD}]" \
  --entity-type users \
  --entity-name "$USERNAME"

if [ ! -f "$ENV_FILE" ]; then
  cp "$EXAMPLE_FILE" "$ENV_FILE"
fi

python3 - "$ENV_FILE" "$USERNAME" "$PASSWORD" <<'PY'
import re
import sys

path, username, password = sys.argv[1:4]
with open(path) as f:
    lines = f.readlines()

def set_var(lines, key, value):
    pattern = re.compile(rf"^{key}=.*$")
    for i, line in enumerate(lines):
        if pattern.match(line.rstrip("\n")):
            lines[i] = f"{key}={value}\n"
            return
    lines.append(f"{key}={value}\n")

set_var(lines, "KAFKA_SASL_USERNAME", username)
set_var(lines, "KAFKA_SASL_PASSWORD", password)

with open(path, "w") as f:
    f.writelines(lines)
PY

echo "Wrote credentials to $ENV_FILE (gitignored)."
echo "  KAFKA_SASL_USERNAME=$USERNAME"
echo "  KAFKA_SASL_PASSWORD=<hidden, see $ENV_FILE>"
