#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import operator
import codecs
import html2text_nm
# import re
import json
# import nltk
# import time
# from nltk.tokenize.punkt import PunktSentenceTokenizer
# tokenizer = PunktSentenceTokenizer()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None

# /opt/spark/bin/spark-submit --master yarn --driver-memory 32g --executor-memory 32g clueweb12_anchored_doc_collect.py

sparkConf = SparkConf().setAppName("PySpark clueWeb09 anchored doc collect")
sc = SparkContext(conf=sparkConf)

dicFile = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb09/urls.uniq'
# dicFile = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb12/urls.uniq'
docDic = {}
with codecs.open(dicFile, 'r', 'utf-8') as f:
    lines = [line.strip() for line in f]

for line in lines:
    url = line.split('\t')[0]
    docDic[url] = len(docDic)

# print(docDic)

docDicBC = sc.broadcast(docDic)

# CW 09
rdd1 = sc.newAPIHadoopFile('hdfs:///corpora/corpora-thirdparty/corpus-clueweb/09-mapfile/data-r-*',
# CW 12
# rdd1 = sc.newAPIHadoopFile('hdfs:///corpora/corpora-thirdparty/corpus-clueweb/12-mapfile/data-r-*',
# rdd1 = sc.newAPIHadoopFile('hdfs:///corpora/corpora-thirdparty/corpus-clueweb/12-mapfile/data-r-00000',
                'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat',
                           'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.Text')
maxHtml = 50000

def checkDoc(data):
    if not data:
#         print('not data')
#         return 'not data'
        return
    try:
#     record = json.loads(data)
        record = tryJson(data)
#         print(str(record)[:100])
        if not record:
#             print('not record')
#             return 'not record'
            return
        content = record['payload']['body']
        WARC_Target_URI = record['metadata']['WARC-Target-URI']
        if WARC_Target_URI not in docDicBC.value:
#             return WARC_Target_URI
            return
#         print('find2')
        if len(content)> maxHtml:
            content = content[:maxHtml]
#         print('find3')
        h = html2text_nm.HTML2Text()
        h.no_markdown=True
        parsed_content = h.handle(content)
#         print('find4')
        record['payload']['parsed_content'] = parsed_content
    #         sys.stderr.write('reporter:counter:meta,collected_document,1\n')
#         print('find1')
        return record
    except:
        return
#         print('exp')
#         return 'exp'
# url_docid = rdd1.map(lambda kv: (json.dumps(kv[0],checkDoc(kv[1]))))
url_docid = rdd1.map(lambda kv: (json.dumps([kv[0],checkDoc(kv[1])])))
url_docid.saveAsTextFile("anchor_frags/frags_9_anchored_doc")
# url_docid.saveAsTextFile("anchor_frags/frags_12_anchored_doc")
