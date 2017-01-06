#!/usr/bin/env bash

echo "wal_level = logical" >> /var/lib/postgresql/data/postgresql.conf
echo "max_replication_slots = 3" >> /var/lib/postgresql/data/postgresql.conf

cat
