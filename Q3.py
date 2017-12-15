#Q3 - user reputation by tenure
from pyspark import SparkContext 
sc = SparkContext("local[*]", "temp") 

posts_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allPosts/*.xml") 
users_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allUsers/*.xml")

import xml.etree.ElementTree as ET
import numpy as np
from operator import add
from collections import Counter

def parse_posts(line):
    line=line.encode("ascii", "ignore")
    if '<row' in line:
        try:
            root = ET.fromstring(line)
            if root != '':
                if "OwnerUserId" in root.attrib:
                    return root.attrib["OwnerUserId"] 
                else:
                    return "ignore"
            else:
                return "ignore"
        except:
            return "ignore"
    else:
        return "ignore"

def parse_users(line): 
    if '<row' in line:
        try:
            root = ET.fromstring(line)
        except:
            return ("ignore")
        
        if root != '': 
            if ("Id" in root.attrib) and ("Reputation" in root.attrib):
                return(root.attrib["Id"], int(root.attrib["Reputation"]))
            else:
                return("ignore")
        else:
            return("ignore")
    else:
        return("ignore")

posts = posts_lines.map(parse_posts).filter(lambda x: x!= 'ignore')
users = users_lines.map(parse_users).filter(lambda x: x!= 'ignore')

q3=posts.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).join(users).map(lambda (userid,(postCounts,Reputation)): (postCounts,Reputation)).groupByKey().map(lambda (x,y): (x,np.mean(list(y)))).takeOrdered(100, key = lambda x: -x[0])

print 'Q3 ANS =',q3
print 'MEAN =',np.average(zip(*q3)[0])
