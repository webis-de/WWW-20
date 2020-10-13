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

# Anchors should not contain “click, read, mail”
# Anchors should not in menu, foot, sidebar

# nltk.download('averaged_perceptron_tagger')

# cb = 9
cb = 12

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None

def cleanurl(url):
    r = url[1:-1]
    
    r = re.split('http://|https://',r)[-1]
    r = re.split('\s',r)[0]
    return r.strip()

# urlDic = {}
# if cb == 9:
#     urlDicFile = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb09/frags_9_doc_ids_part'
# else:
#     urlDicFile = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb12/frags_12_doc_ids_part'
# 
# 
# time1 = time.time()
# 
# for i in range(10):
#     files = os.listdir(urlDicFile+str(i))
#     for file in files:
#         if file != '_SUCCESS':
#             with open(urlDicFile+str(i)+'/'+file,'r') as f:
#                 for line in f:
#                     s = ', '.join(line.strip().split(', ')[1:])[:-1]
#                     if s != 'None':
#                         record = tryJson(s[1:-1])
#                         if record:
#                             docid, docurl, trecid = record
#                             urlDic[docurl] = docid, trecid
                            
urlDic = {}
if cb == 9:
    urlDicFile = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb09/urls.exist'
else:
    urlDicFile = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb12/urls.exist'

time1 = time.time()

with codecs.open(urlDicFile,'r','utf-8') as f:
    for line in f:
        if line.strip():
            record = tryJson(line.strip()[1:-1])
            if record:
                docid, docurl, trecid = record
                urlDic[docurl] = docid, trecid 

time2 = time.time()
print 'load url',time2-time1

if cb == 9:
    txtDir = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb09/'
else:
    txtDir = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb12/'
txtFile = txtDir + 'all_part-00000'

txtOutFile = txtDir + 'all_part-00000.1'

in_count = 0
out_count = 0
emptyCount = 0
nonexistCount = 0
nohttpCount = 0
unkCount = 0
with codecs.open(txtFile, 'r', 'utf-8') as fi, codecs.open(txtOutFile, 'w', 'utf-8') as f:
    for line in fi:
        
        if line.strip():
            record = tryJson(line.strip())
        else:
            unkCount += 1
            continue
        if not record:
            unkCount += 1
            continue 
        
        in_count += 1
        
        url = record[0]
        before = record[1]
        anchor = record[2]
        after = record[3]
        target = record[4]
        id = record[5]
        
        if not url[1:-1].strip():
            emptyCount += 1
            continue
        
        url = cleanurl(url)
        
        if len(url) < 5:
            nohttpCount += 1
            continue
        else:
            url = 'http://'+url
        
        if url not in urlDic:
            nonexistCount += 1
            continue
            
        out_count += 1
        
        newRecord = [url, before, anchor, after, target, id]
        f.write(json.dumps(newRecord)+'\n')

print 'empty:', emptyCount
print 'no http/https:', nohttpCount
print 'unknown count:', unkCount

print 'in count:', in_count
print 'out count:', out_count
print 'kill count:', in_count-out_count
print 'rate:', out_count / float(in_count)
