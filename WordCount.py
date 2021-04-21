from pyspark import SparkConf, SparkContext
from visualize import visualize
import  pandas as pd

SRCPATH = '/home/hadoop/project/data/'
conf = SparkConf().setAppName("project").setMaster("local")
sc = SparkContext(conf=conf)


def wordcount(isvisualize=False):
    """
    对所有答案进行
    :param visualize: 是否进行可视化
    :return: 将序排序结果RDD
    """

    words_list =read_data('file:///'+SRCPATH+'wordcount.csv')

    # 词频统计
    wordsRdd = sc.parallelize(words_list)

    # wordcount：去除停用词等同时对最后结果按词频进行排序
    # 完成SparkRDD操作进行词频统计
    # 提示：你应该依次使用
    #      1.filter函数分别去除长度=1的词汇
    #      2.map进行映射，如['a','b','a'] --> [('a',1),('b',1),('a',1)]
    #      3.reduceByKey相同key进行合并 [('a',2),('b',1)]
    #      4.sortBy进行排序，注意应该是降序排序
    # 【现在你应该完成下面函数编码】
    resRdd = wordsRdd.filter(lambda word:len(word)!=0 )\
        .map(lambda word:(word,1)) \
        .reduceByKey(lambda a, b:a+b) \
        .sortBy(ascending=False, numPartitions=None, keyfunc=lambda x:x[1])\
        # 可视化展示
    if isvisualize:
        v = visualize()
        # 饼状图可视化
        pieDic = v.rdd2dic(resRdd, 10)
        v.drawPie(pieDic)
        # 词云可视化"
        wwDic = v.rdd2dic(resRdd, 50)
        v.drawWorcCloud(wwDic)
    return resRdd
def read_data(file):
    dataRDD=sc.textFile(file)
    words=dataRDD.reduce(lambda x,y:x+' '+y)
    words_list=words.split(' ')
    return words_list

if __name__ == '__main__':
    # 进行词频统计并可视化
    resRdd = wordcount(isvisualize=True)
    print(resRdd.take(10))  # 查看前10个
