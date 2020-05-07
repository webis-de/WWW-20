#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import operator
import codecs
import numpy as np
# import re
import json
# import nltk
# import time
# from nltk.tokenize.punkt import PunktSentenceTokenizer
# tokenizer = PunktSentenceTokenizer()

# /opt/spark/bin/spark-submit --master yarn --driver-memory 48g --executor-memory 48g clueweb12_check_short_content.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return



sparkConf = SparkConf().setAppName("PySpark clueWeb09 doc split")
sc = SparkContext(conf=sparkConf)

dataDir = '/mnt/nfs/webis20/data-in-progress/snippet-generation/'
# dataOpts1 = ['clueweb09','clueweb12']
dataOpts1 = ['clueweb12']
dataOpts2 = ['single','multiple']
# dataOpts2 = ['single']
minLen = 100


for dataOpt1 in dataOpts1:
    totalLen = 0
    for dataOpt2 in dataOpts2:
        if dataOpt1 == 'clueweb09':
            rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/clueweb09_en_maincontent_swr_indexed_' + dataOpt2 + '/part-*',
#             rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/clueweb09_en_maincontent_swr_indexed_single/part-00002',
                                       'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                                       'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.Text')
        else:
            rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/clueweb12_en_maincontent_swr_indexed_' + dataOpt2 + '/part-*',
                                       'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                                       'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.Text')
#         def checkDoc(record):
#             if not record:
#                 return 
#             try:
#                 record = tryJson(record)
#                 if not record:
#                     return
#                 doc = record['doc']
#                 docLen = np.sum(np.sum([[len(x.strip().split(' ')) for x in xx] for xx in doc]))
#                 if docLen < minLen:
#                     return 
#                 else: 
#                     return json.dumps(record)
# #                 return 'catchhhhhhhh'
#             except:
#                 return
# 
#         url_docid = rdd1.map(lambda kv: checkDoc(kv[1])).filter(lambda x: x is not None)
#         url_docid.saveAsTextFile(dataOpt1 + '_en_maincontent_swr_indexed_' + dataOpt2 + '_weblen')
        
        def checkLen(record):
            if not record:
                return 0
            try:
                record = tryJson(record)
                if not record:
                    return 0
                doc = record['doc']
                docLen = np.sum(np.sum([[len(x.strip().split(' ')) for x in xx] for xx in doc]))
                if docLen < minLen:
                    return 0
                else:
                    return 1
            except:
                return 0
#         success = url_docid.map(lambda x: len(x)).collect()
        
#         success = rdd1.map(lambda kv: checkDoc(kv[1])).filter(lambda x: x is not None).map(lambda x: len(x)).collect()
        success = rdd1.map(lambda kv: checkLen(kv[1])).collect()
        
        print('ind Len:',dataOpt1, dataOpt2, sum(success))
#         print(success)
        totalLen +=  sum(success)
    
    print('totalLen:',dataOpt1,totalLen)

