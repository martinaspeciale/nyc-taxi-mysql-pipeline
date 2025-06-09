#!/bin/bash

# Run eda_views/views.sql against your MySQL DB

# Load environment variables from .env (assumes .env is in project root)
set -a
source ../.env
set +a

echo "Running eda_views/views.sql ..."

mysql -u $MYSQL_USER -p$MYSQL_PASSWORD -h $MYSQL_HOST $MYSQL_DATABASE < views.sql

echo "Done."
