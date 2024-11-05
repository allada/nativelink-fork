#!/usr/env bash
sudo docker run --net host -d --name redis-stack-server -p 6379:6379 redis/redis-stack-server:latest || sudo docker start redis-stack-server
sudo docker run --net host -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=password postgres || sudo docker start postgres
sudo docker run --rm -d --net host --name pgadmin -e PGADMIN_DEFAULT_EMAIL=user@domain.com -e PGADMIN_DEFAULT_PASSWORD=password dpage/pgadmin4

sudo docker exec -e PGPASSWORD=password postgres psql -d postgres -U postgres <<<"CREATE DATABASE IF NOT EXISTS nativelink" || true
sudo docker exec -e PGPASSWORD=password postgres psql -d nativelink -U postgres <<<"CREATE TABLE build_data (
    build_id SERIAL PRIMARY KEY,
    build VARCHAR(255) NOT NULL,
    time NUMERIC(6, 2) NOT NULL,
    cache_ratio NUMERIC(5, 2) NOT NULL CHECK (cache_ratio BETWEEN 0 AND 100),
    start_time TIMESTAMP NOT NULL,
    remote_execution BOOLEAN NOT NULL,
    status BOOLEAN DEFAULT NULL
);"



cat <<EOF > /tmp/nativelink.json
{
  "servers": [
    {
      "listener": {
        "http": {
          "advanced_http": {
            "experimental_http2_keep_alive_timeout": 1200
          },
          "compression": {
            "accepted_compression_algorithms": [
              "gzip"
            ],
            "send_compression_algorithm": "gzip"
          },
          "socket_address": "0.0.0.0:50081"
        }
      },
      "services": {
        "experimental_bep": {
          "store": "BEP_STORE"
        },
        "health": {}
      }
    }
  ],
  "stores": {
    "BEP_STORE": {
      "redis_store": {
        "addresses": [
          "redis://127.0.0.1:6379"
        ],
        "experimental_pub_sub_channel": "BEP",
      }
    }
  }
}
EOF

cargo run --bin nativelink -- /tmp/nativelink.json

# REDIS_SUBSCRIBE_CHANNEL=BEP \
# PG_USER=postgres \
# PG_DATABASE=nativelink \
# PG_PASSWORD=password \
# PG_PORT=5432 \
# REDIS_URL=redis://127.0.0.1:6379 \
# bun run ./index.ts
