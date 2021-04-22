from subprocess import PIPE, Popen
import os
from os import listdir
from os.path import isfile, join
import json 
import pandas as pd 
from pandas.io.json import json_normalize 
from datetime import datetime
from urllib.parse import urlparse
import shutil
import argparse
import time
start_time = time.time()

parser = argparse.ArgumentParser()
parser.add_argument("path", help="Enter your directory path")
parser.add_argument("-u", action="store_true", dest="format", default=False,help="Change time format")
args = parser.parse_args()
files = [item for item in listdir(args.path) if isfile(join(args.path, item)) if "json" in item]
checksums = {}
duplicates = []
for filename in files:
    with Popen(["md5sum", filename], stdout=PIPE) as proc:
        checksum = proc.stdout.read().split()[0]
        if checksum in checksums:
            os.remove(filename)
            duplicates.append(filename)
        checksums[checksum] = filename

scanned_items = os.scandir(args.path)
files = []
for item in scanned_items:
    if item.is_file() and '.json' in item.name and '_checked' not in item.name:
        files.append(item.name)     
for file in files:
    records = [json.loads(line) for line in open (file) if '_heartbeat_' not in json.loads(line )]
    df = json_normalize(records)
    df=df.dropna()
    df['web_browser'] = df['a'].str.split('(').str[0]
    df['operating_sys'] = df['a'].str.split('(').str[1]
    df['from_url'] = df.apply(lambda row: urlparse(row['r']).netloc if 'http' in row['r'] else row['r'] , axis = 1)
    df['to_url'] = df.apply(lambda row: urlparse(row['u']).netloc if 'http' in row['u'] else row['u'] , axis = 1)
    df['city'] = df['cy']
    df[['longitude','latitude']]= pd.DataFrame(df.ll.tolist(), index= df.index)
    df['time_zone']=df['tz']
    df['time_in']=df['t']
    df['time_out']=df['hc']
    df['time_in_format'] = pd.to_datetime(df['time_in'])
    df['time_out_format'] = pd.to_datetime(df['time_out'])
    if args.format:
       df['time_in_format'] = pd.to_datetime(df['time_in'])
       df['time_out_format'] = pd.to_datetime(df['time_out'])
       df = df[['web_browser','operating_sys','from_url','to_url','city','longitude','latitude','time_zone','time_in','time_out']]
       name = file.split('.json')
       df.to_csv(args.path+'target/'+name[0]+'.csv')
       os.rename(file,name[0]+'_checked.json')
       num_lines = sum(1 for line in open(args.path+'target/'+name[0]+'.csv'))
       print(num_lines)
       print(args.path+'target/'+name[0]+'.csv')
    else :
      df = df[['web_browser','operating_sys','from_url','to_url','city','longitude','latitude','time_zone','time_in','time_out','time_in_format','time_out_format']]
      name = file.split('.json')
      df.to_csv(args.path+'target/'+name[0]+'.csv')
      os.rename(file,name[0]+'_checked.json')
      num_lines = sum(1 for line in open(args.path+'target/'+name[0]+'.csv'))
      print(num_lines)
      print(args.path+'target/'+name[0]+'.csv')
print("--- %s seconds ---" % (time.time() - start_time))
