#Q8: word2vec

import xml.etree.ElementTree as ET
import numpy as np
from operator import add
from collections import Counter
from datetime import timedelta, datetime

import re
from pyspark import SparkContext
sc = SparkContext("local[*]", "temp")

#Word2vec implementation in Spark MLlib: https://spark.apache.org/docs/latest/mllib-feature-extraction.html
from pyspark.ml.feature import Word2Vec
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


posts_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/datums/allPosts/*.xml")
votes_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/datums/allVotes/*.xml")
users_lines = sc.textFile("file:///home/vagrant/miniprojects/spark/datums/allUsers/*.xml")



def parsePost(line): 
    if '  <row'in line:
        try:
            root = ET.fromstring(line)
        except:
            pass
            return ("Empty")
            
        if root != '':
            if ("OwnerUserId" in root.attrib) and ("CreationDate" in root.attrib): 
                return (root.attrib["OwnerUserId"],root.attrib["CreationDate"])  
            else:
                return("Empty")
        else:
            return("Empty")
    else:
        return("Empty")

def parseAddPost(line):
    if '  <row'in line:
        try:
            root = ET.fromstring(line)
        except:
            pass
            return ("Empty")
            
        if root != '':
            if ("Tags" in root.attrib): 
                return root.attrib["Tags"]
            else:
                return("Empty")
        else:
            return("Empty")
    else:
        return("Empty")

full_posts = posts_lines.map(parseAddPost).filter(lambda x: x!= 'Empty')

df_for_q8=full_posts.map(lambda line: ([s for s in re.split("<|>", line) if s], 1)).toDF(['text', 'score'])

w2v = Word2Vec(inputCol="text", outputCol="vectors")
w2v.setSeed(long('42L'))
w2v.setVectorSize(100) 
model = w2v.fit(df_for_q8)

q8=model.findSynonyms("ggplot2", 25)

print "Q8 ANSWER = ", q8.show(25, False) #returns array of (word, cosineSimilarity); .show() by default shows only top 20 values => Need to disable truncation by specifying .show(25,False)
