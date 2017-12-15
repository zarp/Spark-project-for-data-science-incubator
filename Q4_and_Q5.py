#Q5 is the same as Q4 but on the full StackOverflow dataset
from pyspark import SparkContext
sc = SparkContext("local[*]", "temp") 

posts_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/datums/allPosts/*.xml")
votes_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/datums/allVotes/*.xml")
users_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/datums/allUsers/*.xml")

import xml.etree.ElementTree as ET
import numpy as np
from operator import add
from collections import Counter
from datetime import timedelta, datetime


def parsePost(line): #Q4/Q5 version of this function
    if '  <row'in line:
        try:
            root = ET.fromstring(line)
        except:
            pass
            return ("Empty")
            
        if root != '':
            if ("CreationDate" in root.attrib) and ("Id" in root.attrib):
                return (root.attrib["Id"],root.attrib["CreationDate"])
            else:
                return("Empty")
        else:
            return("Empty")
    else:
        return("Empty")

def parseAccAns(line): #Q4/Q5 version of this function
    if '  <row'in line:
        try:
            root = ET.fromstring(line)
        except:
            pass
            return ("Empty")
            
        if root != '':
            if "AcceptedAnswerId" not in root.attrib:
                return("Empty")
            if "CreationDate" and "Id" in root.attrib:
                return (root.attrib["AcceptedAnswerId"], root.attrib["CreationDate"])
            else:
                return("Empty")
        else:
            return("Empty")
    else:
        return("Empty")

acc_ans_posts = posts_lines.map(parseAccAns).filter(lambda x: x!= 'Empty')
posts=posts_lines.map(parsePost).filter(lambda x: x!= 'Empty')

QUICK_DT = timedelta(hours=3)

q4=(acc_ans_posts.join(posts).map(lambda (myid,(CreationDate,AcceptDate)): (datetime.strptime(CreationDate,'%Y-%m-%dT%H:%M:%S.%f').hour, datetime.strptime(AcceptDate,'%Y-%m-%dT%H:%M:%S.%f')-datetime.strptime(CreationDate,'%Y-%m-%dT%H:%M:%S.%f')<=QUICK_DT))
    .groupByKey().map(lambda (x,y): (x, 1.0*sum(y)/len(y))).sortByKey().map(lambda x:x[1]).collect()
    )

print "Q4 ANSWER =", q4
