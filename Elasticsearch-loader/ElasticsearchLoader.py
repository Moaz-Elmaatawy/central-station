import os
import time
from os import listdir
from os.path import isfile, join
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def find_text_files(directory):
    text_files = []
    for (root, dirs, files) in os.walk(directory):
            
            text_files += [root+'/'+ item for item in files if item.endswith(".parquet")]
            
    return text_files

#function to return files in a directory
def fileInDirectory(my_dir: str):
    files = os.listdir(my_dir)
    onlyfiles = [f for f in listdir(my_dir) if isfile(join(my_dir, f))]
    return(onlyfiles)

#function comparing two lists

def listComparison(OriginalList: list, NewList: list):
    differencesList = [x for x in NewList if x not in OriginalList] #Note if files get deleted, this will not highlight them
    return(differencesList)

def loadToElasticesearch(files: list):
    succeeded=[]
    es = Elasticsearch(os.environ.get("ELASTIC_HOST"))

    for file in files:
        print("==========>",file)
        df = pd.read_parquet(file)

        # Convert DataFrame to list of dicts
        docs = df.to_dict(orient='records')

        # Index documents in Elasticsearch
        index_name = 'weather_statuses'
        doc_type = 'parquet'
        actions = [{'_index': index_name, '_source': d} for d in docs]
        bulk(es, actions)

    es.indices.refresh(index='weather_statuses')

    return files

def appendIndexedFiles(indexedFiles: list):
    with open("Elasticsearch-loader/indexedFiles.txt", "a") as f:
        for filename in indexedFiles:
            f.write(filename + '\n')

def readIndexedFiles():
    indexedFiles=[] 
    with open("Elasticsearch-loader/indexedFiles.txt", "r") as f:
        indexedFiles = f.read().split("\n")

    return indexedFiles


def fileWatcher(watchDirectory: str, pollTime: int):
    print("Elasticsearch loader started....!")
    indexedFiles=readIndexedFiles()

    while True:
        
        
        newFileList = find_text_files(watchDirectory)

        fileDiff = listComparison(indexedFiles, newFileList)

        if len(fileDiff) == 0:
            continue

        fileDiff = loadToElasticesearch(fileDiff)

        indexedFiles = indexedFiles + fileDiff
        appendIndexedFiles(fileDiff)

        time.sleep(pollTime)


watchDirectory = "./parquet_files"
pollTime = 50 #in seconds


fileWatcher(watchDirectory, pollTime)