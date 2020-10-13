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
nltk.download('stopwords', download_dir='./nltk_data')
nltk.download('averaged_perceptron_tagger', download_dir='./nltk_data')

from nltk.corpus import stopwords
stops = stopwords.words('english')
stopDic = {}
for stop in stops:
    stopDic[stop] = len(stopDic)


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

def cleanurl(url):
    r = url[1:-1]
    
    r = re.split('http://|https://',r)[-1]
    r = re.split('\s',r)[0]
    return r.strip()


# run the all parts CB09
# hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.6.jar 
#             \-D mapreduce.map.memory.mb=6000 
#             \-D mapreduce.reduce.memory.mb=6000
#             \-D mapreduce.reduce.shuffle.input.buffer.percent=0.1
#             \-D mapred.child.java.opts=-Xmx400m 
#             \-D mapreduce.task.timeout=1200000
#             \-conf hdfs-site.xml 
#             \-input '/user/qile1864/anchor_frags/frags_9_all_hadoop_stop/part-00000'  
#             \-file clueweb12_anchor_frags_retrieve_good_prop_hadoop_streaming.py dep.zip 
#             \-mapper clueweb12_anchor_frags_retrieve_good_prop_hadoop_streaming.py  
#             \-output anchor_frags/frags_9_all_hadoop_prop

# run the all parts CB12
# hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.6.jar 
#             \-D mapreduce.map.memory.mb=6000 
#             \-D mapreduce.reduce.memory.mb=6000
#             \-D mapreduce.reduce.shuffle.input.buffer.percent=0.1
#             \-D mapred.child.java.opts=-Xmx400m 
#             \-D mapreduce.task.timeout=1200000
#             \-conf hdfs-site.xml 
#             \-input '/user/qile1864/anchor_frags/frags_12_all_hadoop_stop/part-00000'  
#             \-file clueweb12_anchor_frags_retrieve_good_prop_hadoop_streaming.py dep.zip 
#             \-mapper clueweb12_anchor_frags_retrieve_good_prop_hadoop_streaming.py  
#             \-output anchor_frags/frags_12_all_hadoop_prop
 
# line: [url, before, anchor text, after]

minLen = 50
anchorMax = 10
anchorMin = 1
anchorFragMin = 10
minStop = 0.1
maxStop = 0.7
vDic = ['VB','VBD','VBG','VBN','VBP','VBZ']

def main(argv):
    line = sys.stdin.readline();
    try:
        while line:
            
            if line.strip():
                record = tryJson(line.strip())
            else:
                line = sys.stdin.readline()
                continue
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
            
            if not anchor.strip():
                line = sys.stdin.readline()
                sys.stderr.write('reporter:counter:meta,empty_anchor,1\n')
                continue
            
            token_before = nltk.word_tokenize(before)
            token_anchor = nltk.word_tokenize(anchor)
            token_after = nltk.word_tokenize(after)
            
            if len(token_anchor) >= anchorMin and len(token_anchor) <= anchorMax:
                flag_anchor_len = True
            else:
                flag_anchor_len = False
                sys.stderr.write('reporter:counter:meta,long_anchor,1\n')
                
                line = sys.stdin.readline()
                continue
            
            
            if len(token_before) + len(token_after) > minLen:   # inside long text
                flag_inside = True
            else:
                flag_inside = False
                sys.stderr.write('reporter:counter:meta,inside_long,1\n')
                line = sys.stdin.readline()
                continue
            
                
            if before:
                bb = nltk.sent_tokenize(before)[-1]
            else:
                bb = ''
            if after:
                aa = nltk.sent_tokenize(after)[0]
            else:
                aa = ''
            miniAnchor = bb + anchor + aa
            sents = nltk.sent_tokenize(miniAnchor)
    #         print anchor
            anchorP = re.compile('.*'+re.escape(anchor)+'.*',re.DOTALL)
            anchor_sen = miniAnchor
            for sent in sents:
                if anchorP.findall(sent):
                    anchor_sen = sent
                    break
            
            token_anchor_sen = nltk.word_tokenize(anchor_sen)
            
            if len(token_anchor_sen) > anchorFragMin:
                flag_anchor_frag_len = True
            else:
                flag_anchor_frag_len = False
                sys.stderr.write('reporter:counter:meta,sent_less,1\n')
                line = sys.stdin.readline()
                continue
                
                
            flag_frags_verb = False
            POS = nltk.pos_tag(token_anchor_sen)
            for p in POS:
                if p[1] in vDic:
                    flag_frags_verb = True
                    break
            
            if not flag_frags_verb:
                sys.stderr.write('reporter:counter:meta,contain_verb,1\n')
                line = sys.stdin.readline()
                continue
            
            
            stop_count = 0
            for t in token_anchor_sen:
                if t.lower() in stopDic:
                    stop_count += 1
            stop_rate = stop_count / float(len(token_anchor_sen))
            if stop_rate > minStop and stop_rate < maxStop:
                flag_stop_rate = True
            else:
                flag_stop_rate = False
                sys.stderr.write('reporter:counter:meta,stop_ratio,1\n')
                line = sys.stdin.readline()
                continue
    
            if flag_inside and flag_anchor_len and flag_anchor_frag_len and flag_frags_verb and flag_stop_rate:
                if label == 'nw' or label == 'w':
                    sys.stderr.write('reporter:counter:meta,white_anchor,1\n')
                sys.stderr.write('reporter:counter:meta,good_anchor,1\n')
                newRecord = [url, before, anchor, after, target, id, label, tarID]
                print json.dumps(newRecord) + '\n'
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
