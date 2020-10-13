#!/usr/bin/python

import sys


import argparse
import logging
import operator
import re
import json
import time
import nltk
import codecs

nltk.data.path = ['./nltk_data']
nltk.download('punkt', download_dir='./nltk_data')

def tokenize(s):    
    return nltk.word_tokenize(s)

def pos_tag(s):
    nltk.data.path = ['./nltk_data']
    nltk.download('averaged_perceptron_tagger', download_dir='./nltk_data')
    return nltk.pos_tag(s)

def removeList(text):
    r = ''
    listFlag = False
    itemStart = -1
    for i in range(len(text)):
        if listFlag:
            if text[i] == '\n':
                listFlag = False
            
        else:
            if i != len(text):
                if text[i:i+1] == '* ':
                    listFlag = True
                    itemStart = i
                else:
                    r = r + text[i]
            else:
                r = r + text[i]
    return r

def cleanText(text,loc):
    r = text.replace('\ufffd',' ') # special token 'unchangeable' 
    r = r.replace('**','') # special markdown token '<strong>'
    r = r.replace('__','') # special markdown token '<strong>'
    r = r.replace('*','') # special markdown token '<em>'
    r = r.replace('_','') # special markdown token '<em>'
    r = r.replace('#','') # special markdown token heading
#     r = r.replace('^Y','\'') # special token singal quotation
#     r = r.replace('<96>','-')
    r = r.replace('\n',' ') 
    r = re.sub("\s\s+" , " ", r) # remove multiple space
    
    sents = nltk.sent_tokenize(r)
    if loc == 'b' and len(sents) > 1:
        return ' '.join(sents[1:])
    if loc == 'a' and len(sents) > 1:
        return ' '.join(sents[:-1])
    else:
        return r

def isSpam(anchor):
    isSpamFlag = False
    
    spamtext = ['click','read','mail']
    tokens = nltk.word_tokenize(anchor)
    for token in tokens:
        if token.lower() in spamtext:
            isSpamFlag = True
            break
            
    return isSpamFlag

def isSteps(text):
    isStepsFlag = False
    
    maxSteps = 5
    
    nowSteps = 0
    for t in text:
        if t in ['>','|','\\','/']:
            nowSteps += 1
        
        if nowSteps >= maxSteps:
            isStepsFlag = True
            break
    
    return isStepsFlag

# import zipimport
# import html2text
# importer = zipimport.zipimporter('dep.zip')
# html2text = importer.load_module('html2text')
# 
# maxHtml = 50000
# shift = 1500
# minDis = 50
# scriptPat = re.compile('<script(.*?)</script>')
# anchorPat = re.compile('(\[.*?\])(\(.*?\))',re.DOTALL)
# headingPat = re.compile('#+')
# relUrlPat = re.compile('^/')
# absUrlPat = re.compile('^[a-z]+://')
# noIndexHTML = re.compile('/$|/index\\.[a-z][a-z][a-z][a-z]?$')
# markPat = re.compile('<!--(.*\s?)-->')
# htmlTag = re.compile('<[^>]+>|[ \n\t\r]+')


# run the first part
# hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar 
#             \-D mapreduce.map.memory.mb=6000 
#             \-D mapreduce.reduce.memory.mb=6000 
#             \-conf /opt/hadoop/etc/hadoop/hdfs-site.xml 
#             \-input '/corpora/corpora-thirdparty/corpus-clueweb/09-mapfile/data-r-00000' 
#             \-inputformat org.apache.hadoop.mapred.SequenceFileInputFormat 
#             \-file clueweb12_anchor_frags_extractor_hadoop_test5.py dep.zip 
#             \-mapper clueweb12_anchor_frags_extractor_hadoop_test5.py  
#             \-output anchor_frags/frags_9_all_hadoop_t1

# run the all parts CB09
# hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.6.jar 
#             \-D mapreduce.map.memory.mb=6000 
#             \-D mapreduce.reduce.memory.mb=6000
#             \-D mapreduce.reduce.shuffle.input.buffer.percent=0.1
#             \-D mapred.child.java.opts=-Xmx400m 
#             \-D mapreduce.task.timeout=1200000
#             \-conf hdfs-site.xml 
#             \-input '/user/qile1864/anchor_frags/frags_9_all_hadoop_t3/all_part-00000.2'  
#             \-file clueweb12_anchor_frags_retrieve_good_stop_hadoop_streaming.py dep.zip 
#             \-mapper clueweb12_anchor_frags_retrieve_good_stop_hadoop_streaming.py  
#             \-output anchor_frags/frags_9_all_hadoop_stop

# run the all parts CB12
# hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.6.jar 
#             \-D mapreduce.map.memory.mb=6000 
#             \-D mapreduce.reduce.memory.mb=6000
#             \-D mapreduce.reduce.shuffle.input.buffer.percent=0.1
#             \-D mapred.child.java.opts=-Xmx400m 
#             \-D mapreduce.task.timeout=1200000
#             \-conf hdfs-site.xml 
#             \-input '/user/qile1864/anchor_frags/frags_12_all_hadoop_t9/all_part-00000.2'  
#             \-file clueweb12_anchor_frags_retrieve_good_stop_hadoop_streaming.py dep.zip 
#             \-mapper clueweb12_anchor_frags_retrieve_good_stop_hadoop_streaming.py  
#             \-output anchor_frags/frags_12_all_hadoop_stop
 
# line: [url, before, anchor text, after]

def main(argv):
    line = sys.stdin.readline();
    try:
        while line:
            
            
            if line.strip():
                record = tryJson(line)
                if not record:
                    line = sys.stdin.readline()
                    continue 
                
                sys.stderr.write('reporter:counter:meta,processed_anchor,1\n')
                
                url = record[0]
                before = record[1]
                anchor = record[2]
                after = record[3]
                target = record[4]
                id = record[5]
                label = record[6]
                tarID = record[7]
                
                before = before.split('\n\n')[-1]
                after == before.split('\n\n')[0]
                
                before = removeList(before)
                after = removeList(after)
                       
                before = cleanText(before,'b')
                anchor = cleanText(anchor,'i')
                after = cleanText(after,'a')
                
                if isSteps(before) or isSteps(after):
                    line = sys.stdin.readline()
                    sys.stderr.write('reporter:counter:meta,menu_anchor,1\n')
                    continue
                if not anchor.strip():
                    line = sys.stdin.readline()
                    sys.stderr.write('reporter:counter:meta,empty_anchor,1\n')
                    continue
                if isSpam(anchor):
                    line = sys.stdin.readline()
                    sys.stderr.write('reporter:counter:meta,stop_anchor,1\n')
                    continue
                
                
                before = before.lstrip()
                after = after.rstrip()
                
                if before:
                    if before[-1] != ' ' and anchor[0] != ' ':
                        before += ' '
                
                newRecord = [url, before, anchor, after, target, id, label, tarID]
                print json.dumps(newRecord) + '\n'
                    
                sys.stderr.write('reporter:counter:meta,processed_good_anchor,1\n')
            line = sys.stdin.readline()
      
                    
    except "end of file":
        return None


def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None

if __name__ == "__main__":
     main(sys.argv)
