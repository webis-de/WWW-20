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


# /opt/spark/bin/spark-submit --master yarn --driver-memory 4g --executor-memory 4g clueweb12_extractive_sum_dmoz.py 

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None


sparkConf = SparkConf().setAppName("PySpark clueWeb09 ext sum dmoz")
sc = SparkContext(conf=sparkConf)
sc.addPyFile('dep_ext.zip')

from extractor.sentenceselection import sentence_selection
maxSen = 100
minTitle = 10

rdd1 = sc.newAPIHadoopFile('hdfs:///user/qile1864/dmoz_out/',
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
    doc_url = record['doc_url']
    
    sens = doc.split('\n')
    sens = sens[:min(maxSen,len(sens))]
    title = sens[0]
    if len(title.split(' ')) < minTitle:
        title = ' '.join(sens[:2])
    
    
    desiredSummaryLength = 10
    
    isCstOn = False #optional
    selection_results = sentence_selection.summarize([doc], [title], desiredSummaryLength, isCstOn, stopWords, sens)
    sr = [s[0] for s in selection_results]
    
    record['doc'] = ' '.join(sr)
    
    return record

lsh_rdd = rdd1.map(lambda kv: json.dumps(process(kv[1])))
# lsh_rdd.saveAsTextFile("anchor_frags/frags_9_lsh")
lsh_rdd.saveAsTextFile('dmoz/dmoz_ext_t3')


