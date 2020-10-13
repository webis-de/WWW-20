#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

import argparse
import logging
import operator
import re
import json
import time
import nltk
import codecs
import os 
import numpy
# from sklearn.feature_extraction.text import CountVectorizer
# from sklearn.feature_extraction.text import TfidfTransformer
# from nltk import word_tokenize
# from nltk.util import ngrams
# from rake_nltk import Rake

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None

topDir = '/mnt/nfs/webis20/data-in-progress/qile1864/'
# topDir = '../../corpus/'

# cb = 9
cb = 12

if cb == 9:
    anchorFile = topDir + 'anchor1_clueweb09/all_part-00000.6.tf'
    anchorDocFile = topDir + 'anchor1_clueweb09/anchored_doc_filter.txt'
    anchorFileOut = topDir + 'anchor1_clueweb09/all_part-00000.7'
else:
    anchorFile = topDir + 'anchor1_clueweb12/all_part-00000.6.tf'
    anchorDocFile = topDir + 'anchor1_clueweb12/anchored_doc_filter.txt'
    anchorFileOut = topDir + 'anchor1_clueweb12/all_part-00000.7'

collected = {}
fail = 0
with codecs.open(anchorDocFile, 'r' ,'utf-8') as f:
    for line in f:
        if line.strip():
            record = tryJson(line.strip())
        else:
            continue
        if not record:
            continue 
        try:
            tarID = record[1]['metadata']['WARC-TREC-ID']
            collected[tarID] = len(collected)
        except:
            fail += 1

with codecs.open(anchorFile, 'r' ,'utf-8') as f, codecs.open(anchorFileOut, 'w' ,'utf-8') as fo:
    for line in f:
        if line.strip():
            record = tryJson(line.strip())
        else:
            continue
        if not record:
            continue 
        tarID = record[7]
        
        if tarID in collected:
            fo.write(line)
