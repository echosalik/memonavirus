# load all data into BigQuery
import glob
from google.cloud import bigquery
import csv
import os
import time

bq = bigquery.Client()
base_path = os.getcwd()

for file in glob.glob(pathname=os.path.abspath(base_path+'/../data')+'/memes_comments*.log', recursive=False):
  with open(file) as dfile:
    rd = csv.reader(dfile, delimiter="\t", quotechar='"')
    i = 0
    for row in rd:
      query = "INSERT INTO memonavirus.comments (timestamp, comment_author, comment_id, parent_author, parent_id, comment, infected) VALUES('{}', '{}', '{}', '{}', '{}', {}, {})".format(row[0], row[1], row[2], row[3], row[4], "true" if row[5] == "C" else "false", "true" if row[6] == "I" else "false")
      result = bq.query(query).result()
      i += 1
      if i % 5 == 0:
        time.sleep(5)
      
