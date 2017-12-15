Data science incubator miniproject: using Spark to analyze StackOverflow user comments dataset

## Accessing the data

There are three subfolders: allUsers, allPosts, and allVotes which contain
chunked and gzipped xml. Some rows are split across multiple lines; these can be discarded.  A
full schema can be found
[here](https://ia801500.us.archive.org/8/items/stackexchange/readme.txt),
which originates from [this](https://archive.org/details/stackexchange).

## Parsing the data

You will need to handle xml parsing yourself using the \ selector in
Scala or something like [lxml.etree](http://lxml.de/index.html) in PySpark.

To make your code more flexible, it's also recommended to handle command-line
arguments that specify the location of the input data and where output
should be written.

The goal should be to have a parsing function that can be applied to the input
data to access any XML element desired. It is suggested to use a class
structure so that you can create RDDs of Posts, Votes, Users, etc.

### PySpark workflow
Class definitions need to be written in a separate file and then included
at runtime.

1. Edit source code in your main.py file, classes in a separate classes.py
2. Run locally using eg.
`$SPARK_HOME/bin/spark-submit --py-files src/classes.py src/main.py data/stats results/stats/`
3. Run on GCP once your development is done.

### Scala workflow
1. Edit source code in Main.scala
2. Run the command `sbt package` from the root directory of the project
3. Use
   [spark-submit](https://spark.apache.org/docs/latest/submitting-applications.html)
   locally: this means adding a flag like `--master local[2]` to the
   spark-submit command.
4. Run on GCP once your development is done.

# Submission
Replace the default values in `__init__.py` with your answers.

# Questions

##Q1: upvote_percentage_by_favorites

Each post on StackExchange can be upvoted, downvoted, and favorited. One
"sanity check" we can do is to look at the ratio of upvotes to downvotes
(referred to as "UpMod" and "DownMod" in the schema) as a function of
how many times the post has been favorited.  Using post favorite counts
as the keys for your mapper, calculate the average percentage of upvotes
(upvotes / (upvotes + downvotes)) for the first 50 keys (starting from
the least favorited posts).

Do the analysis on the stats.stackexchange.com dataset.

##Q2: user_answer_percentage_by_reputation:

Investigate the correlation between a user's reputation and the kind of posts
they make. For the 100 users with the highest reputation, single out posts which
are either questions or answers and look at the percentage of these posts that
are answers: (answers / (answers + questions)).  Return a tuple of their user
ID and this fraction.

Again, you only need to run this on the statistics overflow set.

##Q3: user_reputation_by_tenure:
If we use the total number of posts made on the site as a metric for tenure, we
can look at the differences between "younger" and "older" users. You can
imagine there might be many interesting features - for now just return the top
100 post counts (of all types of posts) and the average reputation for every
user who has that count.

##Q4: quick_answers_by_hour
How long do you have to wait to get your question answered? Look at the set of
ACCEPTED answers which are posted less than three hours after question
creation. What is the average number of these "quick answers" as a function of
the hour of day the question was asked?  You should normalize by how many total
accepted answers are garnered by questions posted in a given hour, just like
we're counting how many quick accepted answers are garnered by questions posted
in a given hour, eg. (quick accepted answers when question hour is 15 / total
accepted answers when question hour is 15).

Return a list, whose ith element correspond to ith hour (e.g. 0 -> midnight, 1
-> 1:00, etc.)

**Note:** When using Scala's SimpleDateFormat class, it's important to account
for your machine's local time zone.  Our policy will be to use GMT:
`hourFormat.setTimeZone(TimeZone.getTimeZone("GMT"))`

Question: What biases are present in our result, that we don't account for? How
would we handle this?

##Q5: quick_answers_by_hour_full
Same as above, but on the full StackOverflow dataset.

##Q6: identify_veterans_from_first_post_stats
It can be interesting to think about what factors influence a user to remain
active on the site over a long period of time.  In order not to bias the
results towards older users, we'll define a time window between 100 and 150
days after account creation. If the user has made a post in this time, we'll
consider them active and well on their way to being veterans of the site; if
not, they are inactive and were likely brief users.

*Question*: What other parameterizations of "activity" could we use, and how
would they differ in terms of splitting our user base?

*Question*: What other biases are still not dealt with, after using the above
approach?

Let's see if there are differences between the first ever question posts of
"veterans" vs. "brief users". For each group separately, average the score,
views, number of answers, and number of favorites of users' first question.

*Question*: What story could you tell from these numbers? How do the numbers
support it?

##Q7: identify_veterans_from_first_post_stats_full
Same thing on the full StackOverflow dataset.

##Q8: word2vec
[Word2Vec](https://code.google.com/p/word2vec/) is an alternative approach for
vectorizing text data. The vectorized representations of words in the
vocabulary tend to be useful for predicting other words in the document,
hence the famous example "vector('king') - vector('man') + vector('woman')
~= vector('queen')".

Let's see how good a Word2Vec model we can train using the tags of each
StackOverflow post as documents (this uses the full dataset). Use Spark ML's
implementation of Word2Vec (this will require using DataFrames) to return a
list of the top 25 closest synonyms to "ggplot2" and their similarity score
in tuple format ("string", number).

### Parameters
The dimensionality of the vector space should be 100.
The random seed should be `42L`.

## K-means (ungraded)
From your trained Word2Vec model, pass the vectors into a K-means clustering
algorithm. Create a plot of the sum of squared error by calculating the
square root of the sum of the squared distances for each point and its assigned
cluster. For an independent variable use either the number of clusters k or
the dimension of the Word2Vec vectorization.

