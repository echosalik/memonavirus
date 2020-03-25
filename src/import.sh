#!/bin/bash
for file in ../data/memes_comments*
do
  if grep -Fxq "$file" addedinlist.log
  then
    continue
  else
    python3 bigquery_integration.py $file
    echo $file >> addedinlist.log
    sleep 5
  fi
done

for file in ../data/memes_infections*
do
  if grep -Fxq "$file" addedinlist.log
  then
    continue
  else
    python3 bigquery_integration.py $file
    echo $file >> addedinlist.log
    sleep 5
  fi
done