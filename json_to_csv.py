from subprocess import PIPE, Popen
from datetime import datetime
from pathlib import Path
from os import remove
from os.path import isfile, join
import pandas as pd
import argparse
import json
import glob



time = datetime.now() # start time

#One positional argument (the directory path have the files)
parser = argparse.ArgumentParser()
parser.add_argument("dir_path", help = " Please Enter your directory path")

#One optional argument -u. If passed then maintain UNIX format of timpe stamp, if not, the time stamps will be converted.
parser.add_argument("-u", action="store_true", default = False, dest="unixFormat")
args = parser.parse_args()


basePath = Path(args.dir_path)

files = [file for file in glob.iglob(f'{basePath}/*.json')]

#check Duplicates
checksum_ouputs = {}
duplicates_files = []

for filename in files:
    with Popen (["md5sum", filename], stdout = PIPE ) as proc:
        checksum = proc.stdout.read().split()[0]
        
        if checksum in checksum_ouputs:
            duplicates_files.append(filename)
        checksum_ouputs[checksum] = filename

print (f"Found {len(duplicates_files)} of duplicates: {duplicates_files}")

#remove duplicates files
for filename in duplicates_files:
        remove(filename)
        files.remove(filename)
        

#transformation
for file in files:
    records = [json.loads(line) for line in open(file,'r')]
    df = pd.json_normalize(records)
    df=df[['a','r','u','cy','ll','tz','t','hc']]
    df['web_browser'] = df['a'].str.split('/',expand=True)[0]
    df['operating_system']= df['a'].str.split(')',expand=True)[0]
    df['operating_system']= df['operating_system'].str.split(';',expand=True)[0]
    df['operating_system']= df['operating_system'].str.split('(',expand=True)[1]
    df['from_url'] = df['r'].str.replace('http://', '')
    df['from_url'] = df['from_url'].str.split('/',expand=True)[0]
    df['to_url'] = df['u'].str.replace('http://', '')
    df['to_url'] = df['to_url'].str.split('/',expand=True)[0]
    df['city'] = df['cy']
    df['longitude'] = df['ll'].str[0]
    df['latitude'] = df['ll'].str[1]
    df['time zone'] =df['tz']
    df['time_in'] = df['t']
    df['time_out']= df['hc']
    df = df.dropna()
    if(args.unixFormat):
        df['time_in'] = df['time_in']
        df['time_out'] = df['time_out']
    else:
        df['time_in'] = pd.to_datetime(df['time_in'])
        df['time_out'] =pd.to_datetime(df['time_out'])
    df = df.drop(['a','r','u','cy','ll','tz','t','hc'],axis=1)   
    print('The number of rows transformed from file', file,'is',df.shape[0], 'and directory path is',args.dir_path)
    csv_file = str(file).replace('.json',' ')
    csv_file = csv_file.split('/')[1]
    df.to_csv(args.dir_path+"/MyTarget/" + csv_file+'.csv')
    

    
    
total_excutation_time = (datetime.now() - time)
print('Total Execuation Time {}'.format(total_excutation_time))
    