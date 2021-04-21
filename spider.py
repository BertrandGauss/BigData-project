#---爬取前程无忧网上的招聘信息----#
import datetime
import re
import pandas as pd
import asyncio
import aiohttp


start = datetime.datetime.now()
class Spider(object):
    def __init__(self):
        self.semaphore = asyncio.Semaphore(6)
        self.headers = {    #请求头
            'Connection': 'Keep-Alive',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Host': 'search.51job.com',
            'Referer': 'https://search.51job.com/list/000000,000000,0000,00,9,99,Python,2,1.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare=',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24'

        }

    async def scrape(self, url):
        async with self.semaphore:
            session = aiohttp.ClientSession(headers=self.headers)
            response = await session.get(url)
            await asyncio.sleep(1)
            result = await response.text()
            await session.close()
            return result

    async def scrape_index(self, page):
        url_begin= 'https://search.51job.com/list/000000,000000,0000,00,9,99,python,2,'
        url_end='.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare='
        url=url_begin+str(page)+url_end
        text = await self.scrape(url)
        await self.parse(text)
        await asyncio.sleep(1)

    async def parse(self, text):
        # 正则匹配提取数据
        try:
            position = re.findall('"job_name":"(.*?)",', text)          # 职位
            company_name = re.findall('"company_name":"(.*?)",', text)  # 公司名称
            salary = re.findall('"providesalary_text":"(.*?)",', text)
            salary = [i.replace('\\', '') for i in salary]              # 工资     去掉 \ 符号
            city = re.findall('"workarea_text":"(.*?)",', text)         # 城市
            job_welfare = re.findall('"jobwelf":"(.*?)",', text)        # 福利
            attribute = re.findall('"attribute_text":(.*?),"companysize_text"', text)
            attribute = ['|'.join(eval(i)) for i in attribute]
            companysize = re.findall('"companysize_text":"(.*?)",', text)  # 公司规模
            category = re.findall('"companyind_text":"(.*?)",', text)
            category = [i.replace('\\', '') for i in category]             # 公司所属行业  去掉 \ 符号
            datas = pd.DataFrame({'公司名称': company_name, '职位': position, '公司规模': companysize, '城市': city, '工资': salary, '属性': attribute, '公司所属行业': category, '福利': job_welfare})
            datas.to_csv('data\job_information.csv', mode='a+', index=False, header=True)
        except Exception as e:#如果有异常则抛出异常
            print(e)

    def main(self):
        # 爬取1000页的数据
        scrape_index_tasks = [asyncio.ensure_future(self.scrape_index(page)) for page in range(1, 1001)]
        loop = asyncio.get_event_loop()
        tasks = asyncio.gather(*scrape_index_tasks)
        loop.run_until_complete(tasks)


if __name__ == '__main__':
    spider = Spider()
    spider.main()
    delta = (datetime.datetime.now() - start).total_seconds()
    print("用时：{:.3f}s".format(delta))
    print("数据爬取完毕")

