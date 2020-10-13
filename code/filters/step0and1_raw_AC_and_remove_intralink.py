#!/usr/bin/python

import sys


import argparse
import logging
import operator
import re
import json
import time
import zipimport
# import html2text
importer = zipimport.zipimporter('dep.zip')
html2text = importer.load_module('html2text')

maxHtml = 50000
shift = 1500
minDis = 50
scriptPat = re.compile('<script(.*?)</script>')
anchorPat = re.compile('(\[.*?\])(\(.*?\))',re.DOTALL)
headingPat = re.compile('#+')
relUrlPat = re.compile('^/')
absUrlPat = re.compile('^[a-z]+://')
noIndexHTML = re.compile('/$|/index\\.[a-z][a-z][a-z][a-z]?$')
markPat = re.compile('<!--(.*\s?)-->')
htmlTag = re.compile('<[^>]+>|[ \n\t\r]+')

def getDomain(url):
    return re.split(r'http[s]*://',url)[1].split('/')[0]

# run the first part
# hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.4.jar 
#             \-D mapreduce.map.memory.mb=6000 
#             \-D mapreduce.reduce.memory.mb=6000 
#             \-conf /opt/hadoop/etc/hadoop/hdfs-site.xml 
#             \-input '/corpora/corpora-thirdparty/corpus-clueweb/09-mapfile/data-r-00000' 
#             \-inputformat org.apache.hadoop.mapred.SequenceFileInputFormat 
#             \-file clueweb12_anchor_frags_extractor_hadoop_streaming.py dep.zip 
#             \-mapper clueweb12_anchor_frags_extractor_hadoop_streaming.py  
#             \-output anchor_frags/frags_9_all_hadoop_t1

# run the all parts 09
# hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.6.jar 
#             \-D mapreduce.map.memory.mb=6000 
#             \-D mapreduce.reduce.memory.mb=6000 
#             \-D mapreduce.reduce.shuffle.input.buffer.percent=0.1
#             \-D mapred.child.java.opts=-Xmx2024m
#             \-conf /opt/hadoop/etc/hadoop/hdfs-site.xml 
#             \-input '/corpora/corpora-thirdparty/corpus-clueweb/09-mapfile/data-r-*' 
#             \-inputformat org.apache.hadoop.mapred.SequenceFileInputFormat 
#             \-file clueweb12_anchor_frags_extractor_hadoop_streaming.py dep.zip 
#             \-mapper clueweb12_anchor_frags_extractor_hadoop_streaming.py  
#             \-output anchor_frags/frags_9_all_hadoop_t3

# run the all parts 12
# hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.6.jar 
#             \-D mapreduce.map.memory.mb=6000 
#             \-D mapreduce.reduce.memory.mb=6000 
#             \-D mapreduce.reduce.shuffle.input.buffer.percent=0.1
#             \-D mapred.child.java.opts=-Xmx2024m (faster, but might cause out of memory errors)
#             \-conf /opt/hadoop/etc/hadoop/hdfs-site.xml 
#             \-input '/corpora/corpora-thirdparty/corpus-clueweb/12-mapfile/data-r-*' 
#             \-inputformat org.apache.hadoop.mapred.SequenceFileInputFormat 
#             \-file clueweb12_anchor_frags_extractor_hadoop_streaming.py dep.zip 
#             \-mapper clueweb12_anchor_frags_extractor_hadoop_streaming.py  
#             \-output anchor_frags/frags_12_all_hadoop_t9
 
# line: 26ac8398-b7e1-5d59-a595-b997b64ac78d    {"metadata":{"WARC-Identified-Payload-Type":"","WARC-TREC-ID":"
# this is the last file
# last save in : anchor_frags/frags_9_small_hadoop_t7
def main(argv):
    line = sys.stdin.readline();
    try:
        while line:
            sys.stderr.write('reporter:counter:meta,processed_document,1\n')
#             print line[:100]
            try:
                data = '\t'.join(line.split('\t')[1:])
            except:
                line = sys.stdin.readline()
                continue
#             if data:
#                 print data[:100]
            record = tryJson(data)
            if not record:
                line = sys.stdin.readline()
                continue 
#             print record.keys()
#             print str(record)[:100]
#             #         time0 = time.time()
            
            try:
                content = record['payload']['body']
                id = record['metadata']['WARC-TREC-ID']
#                 lan = record['metadata']['WARC-TREC-ID'].split('-')[1][:2]
#                 if lan != 'en':
#                     line = sys.stdin.readline()
#                     continue
                WARC_Target_URI = record['metadata']['WARC-Target-URI']
                target_domain = getDomain(WARC_Target_URI)
#                 print content[:100]
                if len(content)> maxHtml:
                    content = content[:maxHtml]
                h = html2text.HTML2Text()
                h.ignore_images = True
                content = h.handle(content)
#                 print content[:100]
                m = list(anchorPat.finditer(content,re.DOTALL))
                for imm,mm in enumerate(m):
                    before = max([0,mm.start()-shift])
                    after = min([mm.end()+shift,len(content)-1])
                    
                    prefix = mm.group(2)[1:-1][:4]
                    if prefix != 'http': # skip outlink1
                        continue
                    url = mm.group(2)
                    this_domain = getDomain(url)
                    
                    if target_domain == this_domain: # skip outlink2
                        continue
                    
                    text = mm.group(1)[1:-1]
                    if text == '': # remove empty anchor (figure buttons)
                        continue
                       
                    if imm != 0:
                        pre_dis = mm.start()-m[imm-1].end()
                    else:
                        pre_dis = 999
                       
                    if imm != len(m)-1:
                        post_dis = m[imm+1].start()-mm.end()
                    else:
                        post_dis = 999
                       
                    if post_dis < minDis or pre_dis < minDis:
                        continue
                       
                       
                    b = content[before:mm.start()]
                    b_pure = getPure(b,anchorPat)
                    a = content[mm.end():after]
                    a_pure = getPure(a,anchorPat)
           
                    anchor = b_pure + text + a_pure
                       
                    anchor_pair = [url,b_pure,text,a_pure,WARC_Target_URI,id]
        #             time2 = time.time()
        #             print('time12',time2-time1)
        #             time1 = time.time()
                    p = json.dumps(anchor_pair)
                    if p.strip():
                        print p + '\n'
                        sys.stderr.write('reporter:counter:meta,generated_anchor,1\n')
            except:
                line = sys.stdin.readline()
                continue
            line = sys.stdin.readline()
            
            
            
    except "end of file":
        return None


def tryJson(data):
    try:
        return json.loads(data)
    except:
        return None



def getPure(c,p):
    s = p.search(c)
    while s:
        text = s.group(1)[1:-1]
        c = c.replace(s.group(0),text)
        s = p.search(c)
    return c

def makeAbsoluteUrl(targetUrl, relativeUrl):
    targetUrl = absUrlPat.sub('', targetUrl)
    relativeUrl = re.sub(r'[ \n\r\t]','', relativeUrl)
    m = relUrlPat.search(relativeUrl)
    if m:
        absUrl = re.sub(r'/.*$', '', targetUrl)
    else:
        m = absUrlPat.search(relativeUrl)
        if m:
            absUrl = absUrlPat.sub('', relativeUrl)
        else:
            absUrl = re.sub(r'/[^/]+$', '', targetUrl) + '/' + relativeUrl
    return 'http://' + noIndexHTML.sub('', re.sub(r'/.[^/]+/\\.\\./|//', '/', absUrl))

if __name__ == "__main__":
     main(sys.argv)
