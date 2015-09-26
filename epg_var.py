#coding:utf8

import MeCab
import mysql.connector

cnx=mysql.connector.connect(
    host='localhost',
    user='root',
    database='skei_like',
    password='phdfromlse',
    buffered=True,
)

cursor=cnx.cursor(buffered=True)

#functions

def get_talent_var(doc,terms,weights):
    i=0
    flaglist=[0]
    weightlist=[0]
    talent_property=[]

    for term in terms:
        if term in doc:
            term_flag=1
            weightlist[i]=weights[i]
            talent_property.append(term)
        else:
            term_flag=0

        flaglist[i]=term_flag
        flaglist.append(0)
        weightlist.append(0)
        i+=1

    flaglist=map(str,flaglist[0:(len(flaglist)-1)])
    weightlist=weightlist[0:(len(weightlist)-1)]
    talent_property=",".join(talent_property)

    googlemax=max(weightlist)
    googlesum=sum(weightlist)

    epgvar={"talent":"/".join(flaglist),"googlemax":str(googlemax),"googlesum":str(googlesum),"talent_ui":talent_property}

    return epgvar


def pccluster(objtxt,clst):

    mt=MeCab.Tagger("mecabrc")
    res = mt.parseToNode(objtxt)
    surface_v=""
    while res:
        if "形容" in res.feature:
            surface_v=surface_v+"/"+res.surface
        res = res.next
    adjs=surface_v
    pclist=[]
    for cluster in clst:
        termlist=cluster.split("/")
        count=0
        for term in termlist:
            if term in adjs:
                count+=1
        if count>0:
            count=1
        pclist.append(count)
        percept_cluster="/".join(map(str,pclist))

    return {"adj":adjs,"pcclust":percept_cluster}



#weight

f=file("/Users/fujita/Desktop/human_weight.txt","r")
header=f.readline()
terms=f.readlines()

namelist=[]
weightlist=[]
for term in terms:
    term_v=term.split('"""')
    name=term_v[0].strip("\t")
    weight=term_v[2].strip("\r\n").strip("\t")
    namelist.append(name)
    weightlist.append(weight)
weightlist=map(int, weightlist)

f_pclst=file("/Users/fujita/Desktop/視聴率予測/watchrate_test/perl-scripts/clust_term.txt")
clst=f_pclst.readlines()



#dataset

query="""SELECT id,
synopsis,
description
FROM skei_sept.epg_bangumi
WHERE pccluster<=>NULL
"""
cursor.execute(query)
datalist=cursor.fetchall()


EPGquery="""UPDATE skei_sept.epg_bangumi
SET
morph=%(adj)s,
pccluster=%(pcclust)s,
talent=%(talent)s,
googlemax=%(googlemax)s,
googlesum=%(googlesum)s,
talent_ui=%(talent_ui)s
WHERE id=%(id)s
"""

j=0
insertlist=[0]
for data in datalist:
    id={"id":data[0]}
    objtxt=data[1]+data[2]
    objtxt=objtxt.encode("utf8")
    x=get_talent_var(objtxt,namelist,weightlist)
    y=pccluster(objtxt,clst)
    x.update(y)
    x.update(id)
    insertlist[j]=x
    j+=1
    insertlist.append(0)
    if j%100==0:
        print j


insertlist=insertlist[0:(len(insertlist)-1)]
cursor.executemany(EPGquery,insertlist)
cnx.commit()

cursor.close()
cnx.close()
