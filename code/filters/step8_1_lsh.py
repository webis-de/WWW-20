#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import operator
import codecs
# import re
import json
import numpy
from datasketch import MinHash, MinHashLSH
from nltk import ngrams

# import nltk
# import time
# from nltk.tokenize.punkt import PunktSentenceTokenizer
# tokenizer = PunktSentenceTokenizer()

# /opt/spark/bin/spark-submit --master yarn --conf spark.network.timeout=1800s --driver-memory 48g --executor-memory 48g clueweb12_LSH_filter.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None


sparkConf = SparkConf().setAppName("PySpark clueWeb09 doc collect")
sc = SparkContext(conf=sparkConf)

# rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_frags/frags_9_all_hadoop_t3/all_part-00000.4.preLSH',
rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_frags/frags_12_all_hadoop_t9/all_part-00000.4.preLSH',
                           'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                           'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.Text')


mergeSim = 0.9
hash_len = 128
hash_ngram = 3

def process(anchors):
    if not anchors:
        return None
#     try:
    record = tryJson(anchors)
    if not record:
        return None
    
    if len(record) == 1:
        return record
    
    anchor_contexts = [a[1]+a[2]+a[3] for a in record]
    
    lsh = MinHashLSH(threshold=mergeSim, num_perm=hash_len)
    minhashes = {}
    for c, i in enumerate(anchor_contexts):
        minhash = MinHash(num_perm=hash_len)
        for d in ngrams(i, hash_ngram):
            minhash.update(''.join(d).encode('utf-8','ignore'))
        lsh.insert(c, minhash)
        minhashes[c] = minhash
    
    anchorLen = len(anchor_contexts)
    removed = [False] * anchorLen
#         removeCount = 0

    for i in range(anchorLen):
        if not removed[i]:
            result = lsh.query(minhashes[i])
            for r in result:
                if r != i:
                    removed[r] = True
#                         removeCount += 1
    
#         all_remove_count += removeCount
    
    return [record[idx] for idx in range(anchorLen) if not removed[idx]]

lsh_rdd = rdd1.map(lambda kv: json.dumps((kv[0], process(kv[1]))))
# lsh_rdd.saveAsTextFile("anchor_frags/frags_9_lsh")
lsh_rdd.saveAsTextFile("anchor_frags/frags_12_lsh")


