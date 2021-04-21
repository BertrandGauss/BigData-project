import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

df=pd.read_csv("data/processed_data.csv")
df.dropna()

position=['讲师','开发工程师','实习','算法','测试','架构师','运维','数据库','数据分析','主管','产品经理','其他']
salary1=[0 for i in range (len(position)) ]
experience=['在校生','无需经验','1年经验','2年经验','3-4年经验','5-7年经验','8-9年经验','10年以上经验']
salary2=[0 for i in range(len(experience))]

education=['高中','大专','本科','硕士','博士']
salary3=[0 for i in range (len(education)) ]

for i in range (len(position)):
    salary1[i]=df.loc[(df['职位']==position[i]),['工资水平']].工资水平.sum()/len(df.loc[(df['职位']==position[i])])

for i in range (len(experience)):
    salary2[i]=df.loc[(df['经验']==experience[i]),['工资水平']].工资水平.sum()/len(df.loc[(df['经验']==experience[i])])

for i in range (len(education)):
    salary3[i]=df.loc[(df['学历']==education[i]),['工资水平']].工资水平.sum()/len(df.loc[(df['学历']==education[i])])
plt.figure()
plt.bar(position,salary1)
plt.title('不同职位的工资水平')
plt.xlabel('职位')
plt.ylabel('平均工资')
plt.figure()
plt.bar(experience,salary2)
plt.title('不同经验的工资水平')
plt.xlabel('经验')
plt.ylabel('平均工资')
plt.figure()
plt.bar(education,salary3)
plt.title('不同学历的工资水平')
plt.xlabel('学历')
plt.ylabel('平均工资')
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.show()

