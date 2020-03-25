# load all data into BigQuery
import glob
from google.cloud import bigquery
import sys
import csv
import os
import time

bq = bigquery.Client()

base_path = os.getcwd()

def big_query_save():
  file_path = sys.argv[1]
  print("FILE PATH ARG: {}".format(file_path))
  infected = True if file_path.find("infections") != -1 else False
  print("INFECTIONS: {}".format("Yes" if infected else "No"))
  file = os.path.abspath(base_path+"/"+file_path)
  print("ABS FILE PATH: {}".format(file))
  with open(file) as dfile:
    rd = csv.reader(dfile, delimiter="\t", quotechar='"')
    i = 0
    for row in rd: 
      query = ""
      if infected:
        query = """INSERT INTO memonavirus.infections (timestamp, comment_author, comment_id, infected_by_author, infected_by_id, comment, added_on) 
        VALUES('{}', '{}', '{}', '{}', '{}', {}, CURRENT_TIMESTAMP())""".format(row[0], row[1], row[2], row[3], row[4], "true" if row[5] == "C" else "false")
      else:
        query = """INSERT INTO memonavirus.comments (timestamp, comment_author, comment_id, parent_author, parent_id, comment, infected. added_on) 
        VALUES('{}', '{}', '{}', '{}', '{}', {}, {}, CURRENT_TIMESTAMP())""".format(row[0], row[1], row[2], row[3], row[4], "true" if row[5] == "C" else "false", "true" if row[6] == "I" else "false")
      try:
        bq.query(query).result()
      except Exception as identifier:
        print("Exception occured: {}".format(identifier))
      i += 1
      if i % 100 == 0:
        print("SLEEPING. ROWS DONE: {}".format(i))
        time.sleep(10)
big_query_save()

def bigquery_save_comments(timestamp, comment_author, comment_id, parent_author, parent_id, comment, infected):
  query = """INSERT INTO memonavirus.comments (timestamp, comment_author, comment_id, parent_author, parent_id, comment, infected, added_on) 
    VALUES('{}', '{}', '{}', '{}', '{}', {}, {}, CURRENT_TIMESTAMP())""".format(timestamp, comment_author, comment_id, parent_author, parent_id, "true" if comment == "C" else "false", "true" if infected == "I" else "false")
  bq.query(query).result()

def bigquery_save_infected(timestamp, comment_author, comment_id, infected_by_author, infected_by_id, comment):
  query = """INSERT INTO memonavirus.infections (timestamp, comment_author, comment_id, infected_by_author, infected_by_id, comment, added_on) 
  VALUES('{}', '{}', '{}', '{}', '{}', {}, CURRENT_TIMESTAMP())""".format(timestamp, comment_author, comment_id, infected_by_author, infected_by_id, "true" if comment == "C" else "false")
  bq.query(query).result()