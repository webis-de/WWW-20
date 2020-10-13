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

# check if the anchor text is in a long text

# nltk.download('averaged_perceptron_tagger')

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

# txtDir = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb09/'
txtDir = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb12/'
txtFile = txtDir + 'all_part-00000'

txtOutFile = txtDir + 'urls.all'

emptyCount = 0
nohttpCount = 0
inCount = 0
outCount = 0
unkCount = 0
urls = {}
index = 0
time1 = time.time()
with codecs.open(txtFile, 'r', 'utf-8') as fi, codecs.open(txtOutFile, 'w', 'utf-8') as f:
    for line in fi:
        inCount += 1
        
        if line.strip():
            record = tryJson(line.strip())
        else:
            unkCount += 1
            continue
            
        if not record:
            unkCount += 1
            continue 

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

        outCount += 1
        
        if url not in urls:
            urls[url] = 1
        else:
            urls[url] += 1
        
        index += 1
        
        if index%100000 == 0:
            print index
            time2 = time.time()
            print 'time', time2-time1
            time1 = time.time()
    for k,v in urls.iteritems():
        f.write(k + '\t' + str(v) + '\n')
    
    

print 'empty:', emptyCount
print 'no http/https:', nohttpCount
print 'in count:', inCount
print 'out count:', outCount
print 'unknown count:', unkCount
            
            