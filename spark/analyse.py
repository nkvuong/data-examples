from pyspark import SparkContext, SparkConf
import re
import nltk
import os
from nltk.corpus import stopwords

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3.6"

# print(os.environ["PYSPARK_PYTHON"])
# print(os.environ["PYSPARK_DRIVER_PYTHON"])


def wordclean(x):
    return re.sub("[^a-zA-Z0-9\s]+", "", x).lower().strip()


conf = SparkConf().setAppName("textAnalyseProject").setMaster("local[*]")
sc = SparkContext.getOrCreate(conf)

# define input file
bibleInput = "./data/bible.txt"
quranInput = "./data/quran.txt"

# step by step
bibleRDD = sc.textFile(bibleInput)
quranRDD = sc.textFile(quranInput)

print('The number of lines in the Bible text file is {}'.format(bibleRDD.count()))
print('The number of lines in the Quran text file is {}'.format(quranRDD.count()))

# clean all punctuations and convert to lowercase
bibleRDDList = bibleRDD.map(lambda x: wordclean(x))
quranRDDList = quranRDD.map(lambda x: wordclean(x))

# split each element into a list of words
bibleRDDwords = bibleRDDList.flatMap(lambda x: x.split(" "))
quranRDDwords = quranRDDList.flatMap(lambda x: x.split(" "))

# create a map of (word, 1) for each word
bibleRDDwordPairs = bibleRDDwords.map(lambda x: (x, 1))
quranRDDwordPairs = quranRDDwords.map(lambda x: (x, 1))

# find the frequency of each word
bibleRDDwordCount = bibleRDDwordPairs.reduceByKey(lambda a, b: a + b)
quranRDDwordCount = quranRDDwordPairs.reduceByKey(lambda a, b: a + b)

# list of english stopwords
stopwords = stopwords.words('english')

# remove all stopwords
bibleRDDwordCount = bibleRDDwordCount.filter(lambda x: x[0] not in stopwords)
quranRDDwordCount = quranRDDwordCount.filter(lambda x: x[0] not in stopwords)

unique_words_bible = bibleRDDwordCount.count()
unique_words_quran = quranRDDwordCount.count()

print(" The total number of unique words in the bible is {} while the unique number of words in the Quran is {}".
      format(unique_words_bible, unique_words_quran))

total_words_bible = bibleRDDwordCount.map(lambda a: a[1]) \
    .reduce(lambda a, b: a + b)

print("Total number of words in the Bible: {}".format(total_words_bible))

total_words_quran = quranRDDwordCount.map(lambda a: a[1]) \
    .reduce(lambda a, b: a + b)

print("Total number of words in the Quran: {}".format(total_words_quran))

Average_word_count_bible = total_words_bible/unique_words_bible

Average_word_count_quran = total_words_quran/unique_words_quran

print('Average word frequency in the Bible is  {} while the average word frequency in the Quran is {}'.
      format(round(Average_word_count_bible, 1), round(Average_word_count_quran, 1)))

# everything in a pipeline
bibleRDDwordCount = sc.textFile(bibleInput) \
    .map(lambda x: wordclean(x)) \
    .flatMap(lambda x: x.split(" ")) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda x: x[0] not in stopwords)

unique_words_bible = bibleRDDwordCount.count()

print(" The total number of unique words in the bible is {}".
      format(unique_words_bible))
