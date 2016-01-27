# -*- coding: utf-8 -*-
"""
Created on Wed Dec 02 09:08:42 2015

@author: Administrator
"""
#在线解析入库

import sys,os
try:
    os.chdir('F:/DM/Python-work/ScrapyJob')
except Exception:
    pass

global os
from methods import *
import pandas as pd
import MySQLdb
from lxml import etree
import time

reload(sys)
sys.setdefaultencoding('utf8')

#初始化数据库连接
myCon=MySQLdb.connect(host='172.16.100.223',
                      user='root',passwd='root',db='job_db',port=3306)
myCon.set_character_set('utf8')
cur=myCon.cursor()
sql="""SELECT DISTINCT job_url from job_url_tmp where  LENGTH(job_url)>0 """
count=cur.execute(sql)
print 'total records is %d !'%count
processID=0
hparser = etree.HTMLParser(encoding='utf-8')
webName='智联'
keywords='大数据 数据分析 数据挖掘 hadoop'
for rowid in range(1,count+1):
    processID+=1
    if divmod(processID,200)[1]==0:
        disp0='%.2f%%'%(100*processID/float(count))+' completed!'
        print disp0
    result=cur.fetchone()
    url=result[0]
    try:
	   iJobInfo1,iJobInfo2,iJobInfo3,iJobInfo4=zhilianParser(url,1)
    
	   #iJobInfo1Frame
	   iJobInfo1Dict={'JobInfo1':['JobUrl','职位名称','公司名称','公司标签','招聘链接']}
	   iJobInfo1Frame=pd.DataFrame(iJobInfo1Dict)
	   JobUrl=url
	   iJobInfo1TmpDict={
	                    'JobUrl':JobUrl,
	                    '招聘链接':iJobInfo1.CompHref,
	                    '公司标签':iJobInfo1.CompLable,
	                    '公司名称':iJobInfo1.CompName,
	                    '职位名称':iJobInfo1.JobTitle}
	   iJobInfo1Frame['value']=iJobInfo1Frame['JobInfo1'].map(iJobInfo1TmpDict)

	   #iJobInfo2Frame
	   iJobInfo2Dict={'JobInfo2':[u'职位月薪',u'工作地点',u'发布日期',u'工作性质',
	                               u'工作经验',u'最低学历',u'招聘人数',u'职位类别']}
	   iJobInfo2Frame=pd.DataFrame(iJobInfo2Dict)
	   iJobInfo2TmpDict=dict(zip(iJobInfo2.JobInfoName,iJobInfo2.JobInfoValue))
	   iJobInfo2Frame['value']=iJobInfo2Frame['JobInfo2'].map(iJobInfo2TmpDict)

	   #iJobInfo3Frame
	   #'CompIntr', 'JobDesc', 'WorkAdd'
	   iJobInfo3Dict={'JobInfo3':['职位描述','公司简介','上班地址']}
	   iJobInfo3Frame=pd.DataFrame(iJobInfo3Dict)
	   iJobInfo3TmpDict={
	                    '职位描述':iJobInfo3.JobDesc,
	                    '公司简介':iJobInfo3.CompIntr,
	                    '上班地址':iJobInfo3.WorkAdd}
	   iJobInfo3Frame['value']=iJobInfo3Frame['JobInfo3'].map(iJobInfo3TmpDict)

	   #iJobInfo4Frame
	   #'CompInforName', 'CompInforValue'
	   iJobInfo4Dict={'JobInfo4':[u'公司规模',u'公司性质',u'公司行业',u'公司主页',u'公司地址']}
	   iJobInfo4Frame=pd.DataFrame(iJobInfo4Dict)
	   iJobInfo4TmpDict=dict(zip(iJobInfo4.CompInforName,iJobInfo4.CompInforValue))
	   iJobInfo4Frame['value']=iJobInfo4Frame['JobInfo4'].map(iJobInfo4TmpDict)

	   #merge iJobInfo1Frame-->iJobInfo4Frame
	   t1=iJobInfo1Frame.T
	   t2=iJobInfo2Frame.T
	   t3=iJobInfo3Frame.T
	   t4=iJobInfo4Frame.T

	   t1.columns=iJobInfo1Frame['JobInfo1']
	   t2.columns=iJobInfo2Frame['JobInfo2']
	   t3.columns=iJobInfo3Frame['JobInfo3']
	   t4.columns=iJobInfo4Frame['JobInfo4']

	   t1.index=[0,1]
	   t2.index=[0,1]
	   t3.index=[0,1]
	   t4.index=[0,1]

	   t1['key']=[0,1]
	   t2['key']=[0,1]
	   t3['key']=[0,1]
	   t4['key']=[0,1]

	   iJobInfoAll=pd.merge(pd.merge(t1,t2,on='key'),pd.merge(t3,t4,on='key'),on='key')
	   iJobInfoAll=iJobInfoAll.drop(0)

	   tmpTime=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
	   lastPd=pd.DataFrame({'招聘网站':[webName],
                            '关键字':[keywords],
                            '数据日期':[tmpTime]},
                           columns=['招聘网站','关键字','数据日期'])
	   lastPd['key']=[1]
	   iJobInfoAll=pd.merge(iJobInfoAll,lastPd,on='key')
	   iJobInfoAll=iJobInfoAll.drop(['key'],axis=1)
	   iJobInfoAll.index=[processID]
	   #data insert into mysql
	   if len(iJobInfoAll)>0:
	        try:
	            iJobInfoAll.to_sql('job_info_01_tmp', myCon, flavor='mysql',
	               schema='job_db', if_exists='append',
	               index=True, index_label=None, chunksize=None, dtype=None)
	            disp1='. insert '+url+' successed '
	            print disp1
			  myCon.commit()
	        except MySQLdb.Error,e:
	            errormsg="Mysql Error %d: %s" % (e.args[0], e.args[1])
	            disp1='. insert '+url+' Faild '
	            print('faild!',errormsg)
	            print disp1
	   else:
	        disp1=url+' iJobInfoAll data empty!'
	        print disp1
    except:
        ErrorMsg=url+' Faild to Pharse !'
myCon.close()