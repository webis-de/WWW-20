import sys

import argparse
import logging
import operator
import re
import json
import time
import os
import pickle
# import nltk
import codecs

# spam value

# nltk.download('averaged_perceptron_tagger')

# cb = 9
cb = 12

def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None
if cb == 9:
    txtDir = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb09/'
else:
    txtDir = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb12/'

txtFile = txtDir + 'all_part-00000.1'

txtOutFileLVS = txtDir + 'all_part-00000.2'


minSpam = 70
time1 = time.time()

# CB 09
if cb == 9:
    spam = '/mnt/nfs/webis20/data-in-progress/qile1864/spam/clueweb09spam.Fusion'
    spamDic = {}
    with open(spam,'r') as f:
        for line in f:
            if line.strip():
                vs = line.strip().split(' ')
                value = int(vs[0])
                id = vs[1]
                spamDic[id] = value
             
else:
# CB 12
    spamDir = '/mnt/nfs/webis20/data-in-progress/qile1864/spam/waterloo-spam-cw12-decoded/'
    spamDic = {}
    files = os.listdir(spamDir)
    for file in files:
        with open(spamDir+file,'r') as f:
            for line in f:
                if line.strip():
                    vs = line.strip().split(' ')
                    value = int(vs[0])
                    id = vs[1]
                    spamDic[id] = value

whitelist = {}
whiteDir = '/mnt/nfs/webis20/data-in-progress/qile1864/qrel/qrels.diversity'

if cb == 9:
    whiteyears = ['09','10','11','12']  # CB 09
else:
    whiteyears = ['13','14','15','16']  # CB 12
for whiteyear in whiteyears:
    with open(whiteDir+whiteyear, 'r') as f:
        for line in f:
            if whiteyear == '15' or whiteyear == '16':
                topicid, subtopicid,docid,rel, _ = line.strip().split(' ')
            else:  
                topicid, subtopicid,docid,rel = line.strip().split(' ')
            if docid not in whitelist:
                whitelist[docid] = len(whitelist)
                
if cb == 9:
    whiteyears = ['10','11','12']  # CB 09
else:
    whiteyears = ['13','14']  # CB 12

whiteDir2 = '/mnt/nfs/webis20/data-in-progress/qile1864/qrel/'
for whiteyear in whiteyears:
    with open(whiteDir2+whiteyear+'.session-qrels', 'r') as f:
        for line in f: 
            topicid, subtopicid, docid, rel = line.strip().split(' ')
            if docid not in whitelist:
                whitelist[docid] = len(whitelist)
                

# takes 6 hours loading

# urlDic = {}
# if cb == 9:
#     urlDicFile = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb09/frags_9_doc_ids_part'
#     pickleSave = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb09/urls.pickle'
# else:
#     urlDicFile = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb12/frags_12_doc_ids_part'
#     pickleSave = '/mnt/nfs/webis20/data-in-progress/qile1864/anchor1_clueweb12/urls.pickle'
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

# with open(pickle,'w') as f:
#     pickle.dump(urlDic,f)


time2 = time.time()
print 'finish loading dics'
print time2-time1
in_count = 0
out_count = 0
targetSpamCount = 0
targetExpCount = 0
sourceSpamCount = 0
targetNonEnCount = 0
sourceNonEnCount = 0
unkCount = 0
whiteCount = 0
nwhiteCount = 0
nonspamCount = 0
# idx = 0
with codecs.open(txtFile, 'r', 'utf-8') as fi, codecs.open(txtOutFileLVS, 'w', 'utf-8') as f:
    for line in fi:
        
        
        if line.strip():
            record = tryJson(line)
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
        
        if cb == 9:
            lan = id.split('-')[1][:2]
        else:
            lan = 'en'
        
        if lan != 'en':
            sourceNonEnCount += 1
            continue
        
        if cb == 9:
            target_lan = urlDic[url][1].split('-')[1][:2]
        else:
            target_lan = 'en'
        
        if target_lan != 'en':
            targetNonEnCount += 1
            continue
        
        
        if id in spamDic:
            if spamDic[id] >= minSpam:
                if urlDic[url][1] in spamDic:
                    tarID = urlDic[url][1]
                    if spamDic[urlDic[url][1]] >= minSpam and urlDic[url][1] in whitelist:
                        label = 'nw'
                        newRecord = [url, before, anchor, after, target, id, label, tarID]
                        f.write(json.dumps(newRecord)+'\n')
                        out_count += 1
                        nwhiteCount += 1
                    elif spamDic[urlDic[url][1]] < minSpam and urlDic[url][1] in whitelist:
                        label = 'w'
                        newRecord = [url, before, anchor, after, target, id, label, tarID]
                        f.write(json.dumps(newRecord)+'\n')
                        out_count += 1
                        targetExpCount += 1
                        whiteCount +=1
                    elif spamDic[urlDic[url][1]] >= minSpam and urlDic[url][1] not in whitelist:
                        label = 'n'
                        newRecord = [url, before, anchor, after, target, id, label, tarID]
                        f.write(json.dumps(newRecord)+'\n')
                        out_count += 1
                        nonspamCount += 1
                    else:
                        targetSpamCount += 1
                        continue
            else:
                sourceSpamCount += 1
                continue
        
        
print 'in:', in_count
print 'out:', out_count
print 'targetNonEnCount', targetNonEnCount

if cb == 9:
    print 'rate ENG:', targetNonEnCount / float(440605425)
else:
    print 'rate ENG:', targetNonEnCount / float(514337093)

print 'kill count:', in_count-out_count-targetNonEnCount
if cb == 9:
    print 'rate Spam:', (in_count-out_count-targetNonEnCount) / float(440605425)
else:
    print 'rate Spam:', (in_count-out_count-targetNonEnCount) / float(514337093)

print 'targetSpamCount', targetSpamCount
print 'sourceSpamCount', sourceSpamCount

print 'targetExpCount', targetExpCount
print 'unkCount', unkCount


print 'white', whiteCount
print 'nonSpamCount', nonspamCount
print 'nonspam and white', nwhiteCount