#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import operator
import codecs
# import re
import json
import numpy
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')
stopWords = set(stopwords.words('english'))
nltk.download('punkt')
from nltk import ngrams


_sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')


# /opt/spark/bin/spark-submit --master yarn --driver-memory 4g --executor-memory 4g clueweb12_extractive_sum.py 

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None


sparkConf = SparkConf().setAppName("PySpark clueWeb09 doc collect")
sc = SparkContext(conf=sparkConf)
sc.addPyFile('dep_ext.zip')

from extractor.sentenceselection import sentence_selection
maxSen = 100
minTitle = 10

for fid in ['','_single']:
    for txt in ['test','train']:
# for fid in ['_single']:
# for fid in ['']:
#     rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_snippet_query/anchor_snippet_query_train_part/part' + str(fid) + '.json',
        rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/anchor_snippet_query/anchor_snippet_query' + fid + '.' + txt + '.json',
                               'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
                               'org.apache.hadoop.io.Text', 'org.apache.hadoop.io.Text')
    
    
        rdd1 = rdd1.repartition(2000)
        def process(anchors):
            if not anchors:
                return None
            record = tryJson(anchors)
            if not record:
                return None
            
            doc = record['doc']
            query = record['query']
            title = _sent_detector.tokenize(doc)[0]
            if len(title.split(' ')) < minTitle:
                title = ' '.join(_sent_detector.tokenize(doc)[:2])
                
            title = query + ' ' + title
            sents = _sent_detector.tokenize(doc)
            sents = sents[:min(maxSen,len(sents))]
            
            desiredSummaryLength = 10
            
            isCstOn = False #optional
            selection_results = sentence_selection.summarize([doc], [title], desiredSummaryLength, isCstOn, stopWords, sents)
            sr = [s[0] for s in selection_results]
            
            record['doc'] = ' '.join(sr)
            
            return record
        
        lsh_rdd = rdd1.map(lambda kv: json.dumps(process(kv[1])))
        # lsh_rdd.saveAsTextFile("anchor_frags/frags_9_lsh")
        lsh_rdd.saveAsTextFile('anchor_snippet_query/q_ext_sum_' + txt + fid + '_t1')
# lsh_rdd.saveAsTextFile("anchor_snippet_query/ext_sum_test_t2")


