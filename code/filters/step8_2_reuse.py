#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import operator
import codecs
import re
import json
import numpy

# import nltk
import time
# from nltk.tokenize.punkt import PunktSentenceTokenizer
# tokenizer = PunktSentenceTokenizer()

# /opt/spark/bin/spark-submit --master yarn --conf spark.network.timeout=1800s --driver-memory 48g --executor-memory 48g clueweb12_check_reuse.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None

sparkConf = SparkConf().setAppName("PySpark clueWeb09 doc collect")
sc = SparkContext(conf=sparkConf)


topDir = '/mnt/nfs/webis20/data-in-progress/qile1864/'

# anchorFile = topDir + 'anchor1_clueweb09/all_part-00000.5.lsh'
anchorFile = topDir + 'anchor1_clueweb12/all_part-00000.5.lsh'

#1: load the anchor context dic
time1 = time.time()
anchorDic = {}
tarIDs = []
with codecs.open(anchorFile, 'r' ,'utf-8') as f:
    for line in f:
        if line.strip():
            record = tryJson(line.strip())
        else:
            continue
        if not record:
            continue 
        url = record[0]
        before = record[1]
        anchor = record[2]
        after = record[3]
        target = record[4]
        id = record[5]
        label = record[6]
        tarID = record[7]
         
        if tarID in anchorDic:
            anchorDic[tarID].append((url,before,anchor,after,target,id,label,tarID))
        else:
            anchorDic[tarID] = [(url,before,anchor,after,target,id,label,tarID)]
            tarIDs.append(tarID)
 
time2 = time.time()
print('load dic1', time2-time1)
 
 
maxAnchor = 400000
num_dics = int(len(tarIDs)/maxAnchor)+1
anchorDics = [{}]*num_dics
for tid, tarID in enumerate(tarIDs):
    dicID = int(tid/maxAnchor)   
    anchorDics[dicID][tarID] = anchorDic[tarID]
 
anchorDicBC = sc.broadcast(anchorDics)
 
time3 = time.time()
print('load dic2', time3-time2)

# rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_frags/frags_9_anchored_doc/part-[0-2]*',
# rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_frags/frags_9_anchored_doc/part-[3-5]*',
# rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_frags/frags_9_anchored_doc/part-*',
# rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_frags/frags_12_anchored_doc/part-[0-2]*',
rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_frags/frags_12_anchored_doc/part-[3-4]*',
# rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_frags/frags_12_anchored_doc/part-*',
                           'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                           'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.Text')

# reuse = 0
def process(webpage):
    if webpage:
        record = tryJson(webpage)
    else:
#         return 'None1'
        return None
    if not record:
#         return 'None2'
        return None
    if not record[1]:
        return None
    try:
        tarID = record[1]['metadata']['WARC-TREC-ID']
        targetContent = record[1]['payload']['parsed_content']
        targetContent = re.sub("\s\s+" , " ", targetContent) # remove multiple space
        
    #     return 'none3'
    #     return targetContent[:100]
        goodAnchors = []
        for anchorDic in anchorDicBC.value: 
            if tarID in anchorDic:
                for anchorContext in anchorDic[tarID]:  
                    if targetContent.find(anchorContext[1] + anchorContext[2] + anchorContext[3]) < 0:
                        goodAnchors.append(anchorContext)
    #                 else:
    #                     reuse += 1
          
        return goodAnchors
    except:
        return None
lsh_rdd = rdd1.map(lambda kv: (json.dumps((kv[0], process(kv[1])))))
# lsh_rdd.saveAsTextFile("anchor_frags/frags_9_reuse_part1")
# lsh_rdd.saveAsTextFile("anchor_frags/frags_9_reuse_part2")
# lsh_rdd.saveAsTextFile("anchor_frags/frags_9_reuse")
# lsh_rdd.saveAsTextFile("anchor_frags/frags_12_reuse_part1")
lsh_rdd.saveAsTextFile("anchor_frags/frags_12_reuse_part2")
# lsh_rdd.saveAsTextFile("anchor_frags/frags_12_reuse")

# print('reuse',reuse)
