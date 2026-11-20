#!/usr/bin/env bash
set -u
rm -f "$HOME/hudi-bigdata/metastore_db/db.lck" "$HOME/hudi-bigdata/metastore_db/dbex.lck"
DERBY_JAR=$(ls "$HOME"/.local/lib/python3.*/site-packages/pyspark/jars/derby-*.jar | head -1)
echo "derby jar: $DERBY_JAR"
"$HOME/tools/jdk-17.0.21+9/bin/java" -cp "$DERBY_JAR" "$HOME/hudi-bigdata/sql/HmsQuery.java"
