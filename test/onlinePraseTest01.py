# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 16:29:20 2015

@author: Administrator
"""

from lxml import etree
import MySQLdb

myCon=MySQLdb.connect(host='172.16.100.223',
                      user='root',passwd='root',db='job_db',port=3306)
myCon.set_character_set('utf8')
cur=myCon.cursor()
sql="""SELECT DISTINCT job_url from job_url where  LENGTH(job_url)>0 limit 3000 """
count=cur.execute(sql)
print 'total records is %d !'%count
processID=0
hparser = etree.HTMLParser(encoding='utf-8')
for rowid in range(1,count+1):
    processID+=1
    if divmod(processID,200)[1]==0:
        disp0='%.2f%%'%(100*processID/float(count))+' completed!'
        print disp0
    result=cur.fetchone()
    url=result[0]
    # url='http://jobs.zhaopin.com/338267514250453.htm'
    htree = etree.parse(url,hparser)
    FileName=url.replace('://','___').replace('/','_')
    FullFileName='F:/DM/Python-work/ScrapyJob/htmls/'+FileName
    try:
        htree.write(FullFileName,encoding='utf-8')
    except:
        pass
myCon.close()