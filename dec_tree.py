from pyspark import SparkConf,SparkContext
from pyspark.sql.types import *
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
import numpy as np
import datetime

DATAPATH='hdfs://master:9000/bigdata/project/data_tree.csv'
SAVAPATH='/home/hadoop/project/result'

conf=SparkConf().setAppName("project").setMaster("spark://master:7077")
sc=SparkContext(conf=conf)
sc.setLogLevel("WARN")
spark=SparkSession(sc)



def one_hot(line,positionMap,cityMap,experienceMap,educationMap):
    '''
    :param lines: 字段行
    :param map: 离散属性对应的字典
    :param pos:需要编码的属性所处的位置
    :return:
    '''
    index1=cityMap[line[1]]
    index2=experienceMap[line[2]]
    index3=educationMap[line[3]]
    OneHot1=np.zeros(len(cityMap))
    OneHot2=np.zeros(len(experienceMap))
    OneHot3=np.zeros(len(educationMap))
    OneHot1[index1]=1
    OneHot2[index2]=1
    OneHot3[index3]=1
    return np.concatenate(([positionMap[line[0]]],[cityMap[line[1]]],[experienceMap[line[2]]],[educationMap[line[3]]]))

#对读入的文件进行简单处理
def pretreat():
    '''
    :return: 字段行,离散数据的数据字典
    '''
    read_data=sc.textFile(DATAPATH)
    #去除头部
    header=read_data.first()
    rdata=read_data.filter(lambda x:x!=header)
    lines=rdata.map(lambda x:x.split(","))
    #去除空数据
    lines=lines.filter(lambda x:len(x[1])!=0 and len(x[2])!=0 and len(x[3])!=0 and len(x[4])!=0)
    #对离散数据设计数据字典
    cityMap = lines.map(lambda field:field[1]).distinct().zipWithIndex().collectAsMap()
    experienceMap=lines.map(lambda field: field[2]).distinct().zipWithIndex().collectAsMap()
    educationMap=lines.map(lambda field: field[3]).distinct().zipWithIndex().collectAsMap()
    positionMap=lines.map(lambda field: field[0]).distinct().zipWithIndex().collectAsMap()

    return lines,cityMap,experienceMap,educationMap,positionMap
def LabelPoint(lines,cityMap,experienceMap,educationMap,positionMap):
    labelpointRDD = lines.map(lambda line:LabeledPoint(line[4], one_hot(line, positionMap,cityMap, experienceMap,educationMap)))
    return labelpointRDD
def ModelAccuracy(model, testData):
    ## 计算模型的准确率
    predict = model.predict(testData.map(lambda p:p.features))
    predict = predict.map(lambda p: float(p))
    ## 拼接预测值和实际值
    predict_real = predict.zip(testData.map(lambda p: p.label))
    matched = predict_real.filter(lambda p:p[0]==p[1])
    accuracy =  float(matched.count()) / float(predict_real.count())
    return accuracy
#def train_accurcay(trainData, testData,impurity,maxDepth,maxBins):
#    print("模型开始训练")
#    model = DecisionTree.trainClassifier(trainData, numClasses=3, categoricalFeaturesInfo={}, impurity=impurity,
#                                         maxDepth=maxDepth, maxBins=maxBins)
#    print("模型训练完毕")
#    accuracy = ModelAccuracy(model, testData)
#    print("准确度为：",accuracy)
#    return model,accuracy
#def find_best(trainData, testData):
#    impurityList = ["gini", "entropy"]
#    maxDepthList = [3, 5, 10, 20, 25, 30]
#    maxBinsList = [5, 10, 50, 100, 200]
#    best_model = [train_accurcay(trainData,testData, impurity, maxDepth, maxBins)
#               for impurity in impurityList
#               for maxDepth in maxDepthList
#               for maxBins in maxBinsList]
#    sorted_model = sorted(best_model, key=lambda x: x[1], reverse=True)
#    best=sorted_model[0]
#    print("训练所得的最佳模型的准确度为：",best[1])
#    return best[0]
if __name__ == '__main__':
    lines, cityMap, experienceMap, educationMap, positionMap=pretreat()
    labelpointRDD=LabelPoint(lines,cityMap,experienceMap,educationMap,positionMap)
    ## 划分训练集、验证集和测试集
    (trainData, testData) = labelpointRDD.randomSplit([7, 3])
    trainData.persist()
    testData.persist()
    print("训练集样本个数：" + str(trainData.count()) + " 测试集样本个数：" + str(testData.count()))
    print("模型开始训练")
    start = datetime.datetime.now()
    model = DecisionTree.trainClassifier(trainData, numClasses=3,categoricalFeaturesInfo={}, impurity="gini", maxDepth=20,maxBins=200)
    delta = (datetime.datetime.now() - start).total_seconds()
    print("模型训练训练结束，用时：{:.3f}s".format(delta))
    accuracy = ModelAccuracy(model, testData)
    print("准确度为",accuracy)#model=find_best(trainData, testData)
    #position=str(input("请输入您的预期岗位:")  )
    #city=str(input("请输入您的预期工作城市:")  )
    #experience=str(input("请输入您的经验:")   )
    #education=input("请输入您的学历:")
    print("请输入您的预期岗位:开发工程师")
    print("请输入您的预期工作城市:北京")
    print("请输入您的经验:5-7年经验")
    print("请输入您的学历:硕士")
    position="开发工程师"
    city="北京"
    experience="5-7年经验"
    education="硕士"
    feature=np.concatenate(([positionMap[position]],[cityMap[city]],[experienceMap[experience]],[educationMap[education]]))
    predict=model.predict(feature)
    #predict.map(lambda x:float(x))
    #print(predict)
    tree=model.toDebugString()
    print(tree)
    if predict=='0.0':
        print("预测您的薪资将较低")
    elif predict=='1.0':
        print("预测您的薪资将为中等水平")
    else:
        print("预测您的薪资水平将较高")



