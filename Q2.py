from pyspark import SparkContext
sc = SparkContext("local[*]", "temp")

posts_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allPosts/*.xml")
votes_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allVotes/*.xml")
users_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allUsers/*.xml")

import xml.etree.ElementTree as ET
import numpy as np
from operator import add
from collections import Counter

def parsePost(line): #Q2 version of this function
    if '  <row'in line:
        try:
            root = ET.fromstring(line)
        except:
            pass
            return ("Empty")
            
        if root != '':
            if "PostTypeId" and "OwnerUserId" in root.attrib:
                return(root.attrib["OwnerUserId"], root.attrib["PostTypeId"])
            else:
                return("Empty")
        else:
            return("Empty")
    else:
        return("Empty")

def parseUser(line): 
    if '  <row'in line:
        try:
            root = ET.fromstring(line)
        except:
            pass
            return ("Empty")
        
        if root != '':
            if "Id" and "Reputation" in root.attrib: 
                return(root.attrib["Id"], int(root.attrib["Reputation"]))
            else:
                return("Empty")
        else:
            return("Empty")
    else:
        return("Empty")


    
def parseVote(line): 
    if '  <row'in line:
        try:
            root = ET.fromstring(line)
        except:
            pass
            return("Empty")
        if root != '':
            if "PostId" and "VoteTypeId" in root.attrib:
                vote = root.attrib['VoteTypeId']
                if vote == '2':
                    votes = [1.,0.,1.]
                else:
                    if vote == '3':
                        votes = [0.,1.,1.]
                    else: return("Empty")
                return (root.attrib['PostId'], votes)
            else:
                return("Empty")
        else:
            return("Empty")
    else:
        return("Empty")

posts = posts_lines.map(parsePost).filter(lambda x: x!= 'Empty')

users = users_lines.map(parseUser).filter(lambda x: x!= 'Empty')

votes = votes_lines.map(parseVote).filter(lambda x: x != 'Empty')\
        .reduceByKey(lambda x, y: np.add(x,y))

selected_users = users.takeOrdered(100, key = lambda x: -x[1]) #"-" because descending
topuseridsonly=zip(*selected_users)[0]

q2 = (posts.filter(lambda x: x[0] in topuseridsonly and (x[1] == '1' or x[1] == '2')).map(lambda x:(x[0],x[1])).groupByKey().map(lambda (x,y): (x, Counter(y)))
.map(lambda (x,y): (int(x), 1.0*y['2']/(y['1']+y['2'])))
.collect())

print "Q2 ANSWER=",q2
