from pyspark import SparkContext
sc = SparkContext("local[*]", "temp")

posts_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allPosts/*.xml") #create an RDD from file
print posts_lines
posts_lines.count()

votes_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/STATS_DATA/allVotes/*.xml")
print votes_lines
votes_lines.count()

import xml.etree.ElementTree as ET
import numpy as np
from operator import add


def parsePost(line):
    if '  <row'in line:
        try:
            root = ET.fromstring(line)
        except:
            pass
            return ("Empty")
            
        if root != '':
            if "Id" and "FavoriteCount" in root.attrib:
                return(root.attrib["Id"], int(root.attrib["FavoriteCount"]))
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

votes = votes_lines.map(parseVote).filter(lambda x: x != 'Empty')\
        .reduceByKey(lambda x, y: np.add(x,y))

joint = posts.join(votes).map(lambda x: (x[1][0], x[1][1]))\
        .reduceByKey(lambda x, y: np.add(x,y)).map(lambda x: (x[0], x[1][0]/x[1][2]))\
        .sortByKey().collect()

print joint[:50]
