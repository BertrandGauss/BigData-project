#----数据预处理----#
import pandas as pd
import re
import numpy as np
df=pd.read_csv("data/job_information.csv")
df.drop_duplicates(subset=["公司名称","职位"],inplace=True)#当公司名称和职位以及工资相同时则认为时同一条数据
print("去重后的数据条数为",df.shape)
df1=pd.DataFrame()

#对职位进行分类
position=['讲师','开发工程师','实习','算法','测试','架构师','运维','数据库','数据分析','主管','产品经理']
def fenlei(x):
    for i in position:
        if x.find(i)!=-1:
            return i
        elif x.find("培训老师")!=-1 or x.find("教师")!=-1:
            return '讲师'
        elif x.find("研发工程师")!=-1 or x.find("开发高级工程师")!=-1:
            return '开发工程师'
        elif x.find('leader')!=-1 or x.find("总监")!=-1:
            return '主管'
    return '其他'
df1["职位"]=df['职位'].apply(fenlei)

df1["城市"]=df["城市"]
df1.loc[:,'城市'] = df['城市'].str.split('-', expand=True)#将城市由城市-区格式转为城市

df1['经验'] = df['属性'].str.split('|', expand=True)[1]#在属性文字中提取处经验
df['学历'] = df['属性'].str.split('|', expand=True)[2]#在属性文字中提取处学历
df["学历"] = df["属性"].apply(lambda x:re.findall("本科|大专|高中|硕士|博士",x))
def func(x):
    if len(x) == 0:
        return np.nan
    elif len(x) == 1 or len(x) == 2:
        return x[0]
    else:
        return x[2]
df1["学历"] = df["学历"].apply(func)


#对工资进行处理
df["工资"].str[-1].value_counts()
df["工资"].str[-3].value_counts()

index1 = df["工资"].str[-1].isin(["年","月"])
index2 = df["工资"].str[-3].isin(["万","千"])
job_infor = df[index1 & index2]

def get_money_max_min(x):
    try:
        if x[-3] == "万":
            z = [float(i)*10000 for i in re.findall("[0-9]+\.?[0-9]*",x)]
        elif x[-3] == "千":
            z = [float(i) * 1000 for i in re.findall("[0-9]+\.?[0-9]*", x)]
        if x[-1] == "年":
            z = [i/12 for i in z]
        return z
    except:
        return x
salary = job_infor["工资"].apply(get_money_max_min)
job_infor["最低工资"] = salary.str[0]
job_infor["最高工资"]= salary.str[1]
job_infor["工资水平"] =job_infor[["最低工资","最高工资"]].mean(axis=1)
df1["工资水平"]=job_infor["工资水平"]
df2=pd.DataFrame()
df2["福利"]=df["福利"]
df1.to_csv("data/processed_data.csv", index=False, header=True)
df2.to_csv("data/wordcount.csv",index=False, header=False)