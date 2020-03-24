#!/bin/bash
for file in ../data/memes_comments*
do
  python3 bigquery_integration.py $file
  sleep 5
done

for file in ../data/memes_infections*
do
  python3 bigquery_integration.py $file
  sleep 5
done