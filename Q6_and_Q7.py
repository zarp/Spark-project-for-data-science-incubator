#Q6 an Q7. Q7 is the same as Q6 but on the full dataset

from pyspark import SparkContext 
sc = SparkContext("local[*]", "temp")

posts_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allPosts/*.xml")
users_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allUsers/*.xml")

import xml.etree.ElementTree as ET
import numpy as np
from operator import add
from collections import Counter
from datetime import timedelta, datetime

def parse_posts(line):
    line=line.encode("ascii", "ignore")
    if '<row' in line:
        try:
            root = ET.fromstring(line)
            if root != '':
                if ("OwnerUserId" in root.attrib) and ("CreationDate" in root.attrib): 
                    return (root.get("OwnerUserId"),root.get("CreationDate"))
                else:
                    return "ignore"
            else:
                return "ignore"
        except: 
            return "ignore" 
    else:
        return "ignore"

def parseAddPost(line): 
    line=line.encode("utf-8", "ignore") 
    if '<row' in line:
        try:
            root = ET.fromstring(line)
            if root != '':
                if ("OwnerUserId" in root.attrib) and ("CreationDate" in root.attrib) and ("ViewCount" in root.attrib) and ("Score" in root.attrib) and ("FavoriteCount" in root.attrib) and ("AnswerCount" in root.attrib): 
                    return (root.get("OwnerUserId"),root.get("CreationDate"),root.get("ViewCount"),root.get("Score"),root.get("FavoriteCount"),root.get("AnswerCount")) 
                else:
                    return "ignore"
            else:
                return "ignore"
        except: 
            return "ignore"
    else:
        return "ignore"



def parseUser(line):
    line=line.encode("utf-8", "ignore") 
    if '<row' in line:
        try:
            root = ET.fromstring(line) 
            if root != '':
                if ("Id" in root.attrib) and ("CreationDate" in root.attrib):
                    return(root.attrib["Id"], root.attrib["CreationDate"])
                else:
                    return "ignore"
            else:
                return "ignore"
        except:
            return "ignore" 
    else:
        return "ignore"

def construct_post(line): 
    try:
        data = ET.fromstring(line)
    except:
        return None
    return (data.get('OwnerUserId'), data.get('CreationDate'),
                data.get('ViewCount'), data.get('Score'), data.get('FavoriteCount'), data.get('AnswerCount')
               )

add_posts = posts_lines.map(parseAddPost).filter(lambda x: x!= 'ignore')
posts = posts_lines.map(parse_posts).filter(lambda x: x!= 'ingore')
users=users_lines.map(parseUser).filter(lambda x: x!= 'ignore')

TD100 = timedelta(days=100)
TD150 = timedelta(days=150)

vets=(posts.map(lambda x: (x[0],  datetime.strptime(x[1],'%Y-%m-%dT%H:%M:%S.%f')))
        .join(users)
        .map(lambda (myid, (postDate,userDate)):
            (myid, (postDate, (postDate >= datetime.strptime(userDate,'%Y-%m-%dT%H:%M:%S.%f') + TD100) and (postDate <= datetime.strptime(userDate,'%Y-%m-%dT%H:%M:%S.%f') + TD150))) )
        .reduceByKey(lambda x,y: (min(x[0],y[0]), x[1] or y[1]))
        .map(lambda (myid, (postDate,ifVet)): ((myid,postDate),ifVet))
        .join(add_posts.map(lambda x: ( (x[0], datetime.strptime(x[1],'%Y-%m-%dT%H:%M:%S.%f')),(float(x[2]),float(x[3]),float(x[4]),float(x[5]),1))))
        .map(lambda (x,y): y)
        .reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1],x[2]+y[2],x[3]+y[3],x[4]+y[4]))
        .map(lambda (x,y): (x,map(lambda t: float(t)/float(y[4]), y[:4]))) 
      .collect()
      )

print "Q6 ANSWER=",vets
