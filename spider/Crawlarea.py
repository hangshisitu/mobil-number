import requests
from bs4 import BeautifulSoup
import re
import sqlite3
import time

home_url = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/"

create_sql='''
CREATE TABLE t_area_division(
  id INTEGER NOT NULL PRIMARY KEY autoincrement,
  code varchar(3) NOT NULL DEFAULT '',
  full_code varchar(11) NOT NULL DEFAULT '',
  name varchar(128) NOT NULL DEFAULT '',
  level tinyint(3) NOT NULL DEFAULT '0',
  ctime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  utime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
'''
task_sql='''
CREATE TABLE t_crawl_task(
  id INTEGER NOT NULL PRIMARY KEY autoincrement,
  url varchar(256) NOT NULL DEFAULT '',
  level tinyint(3) NOT NULL DEFAULT '0',
  status tinyint(3) NOT NULL DEFAULT '0',
  ctime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  utime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
'''


def crawl(url):
    print(url,flush=True)
    time.sleep(0.3)
    rsp = requests.get(url)
    try:
        context = rsp.content.decode("gb18030")
    except UnicodeDecodeError as e:
        print(rsp.content,flush=True)
        print(e,flush=True)
        raise Exception()
    return BeautifulSoup(context, "html.parser")

def CrawlProvince(url,cursor):
    # print(url)
    # rsp = requests.get(url)
    # context = rsp.content.decode("gb2312")
    bs = crawl(url)
    for uri in bs.find_all('tr',"citytr"):
        cityitems=uri.find_all('a')
        if(len(cityitems)<2):
            continue
        #print(cityitems[0]["href"],cityitems[0].string,cityitems[1].string)
        full_code = int(cityitems[0].string)//100000000
        code = full_code % 100
        parent_code = full_code // 100
        name = cityitems[1].string
        if(cursor != None):
            sql = "insert into t_area_division (code,full_code,level,name) values (" +"{:02d}".format(code) +","+ str(full_code) + ",2,'"+name+"')"
            print(sql,flush=True)
            cursor.execute(sql)
        #CrawlCity(home_url+cityitems[0]["href"],cursor)
        new_task = "insert into t_crawl_task (url,level) values ('" + cityitems[0]["href"] + "',2)"
        cursor.execute(new_task)

def CrawlCity(url,cursor):
    # print(url)
    # rsp = requests.get(url)
    # context = rsp.content.decode("gb2312")
    bs = crawl(url)
    for uri in bs.find_all('tr',"countytr"):
        countyitems=uri.find_all('a')
        if(len(countyitems)<2):
            continue
        #print(countyitems[0]["href"],countyitems[0].string,countyitems[1].string)
        if(cursor!=None):
            full_code = int(countyitems[0].string) // 1000000
            code = full_code % 100
            parent_code = full_code // 100
            name = countyitems[1].string
            if (cursor != None):
                sql = "insert into t_area_division (code,full_code,level,name) values (" + "{:02d}".format(code) + "," + str(full_code) + ",3,'" + name + "')"
                print(sql,flush=True)
                cursor.execute(sql)
                tmp = '{:0>2d}/{:s}'.format(parent_code//100,countyitems[0]["href"])
                new_task = "insert into t_crawl_task (url,level) values ('" + tmp + "',3)"
                cursor.execute(new_task)

def CrawlCounty(url,cursor):
    # print(url)
    # rsp = requests.get(url)
    # context = rsp.content.decode("gb2312")
    # bs = BeautifulSoup(context, "html.parser")
    bs = crawl(url)
    for uri in bs.find_all('tr',"towntr"):
        countyitems=uri.find_all('a')
        if(len(countyitems)<2):
            continue
        #print(countyitems[0]["href"],countyitems[0].string,countyitems[1].string)
        full_code = int(countyitems[0].string) // 1000
        code = full_code % 1000
        parent_code = full_code // 1000
        name = countyitems[1].string
        if (cursor != None):
            sql = "insert into t_area_division (code,full_code,level,name) values (" + "{:03d}".format(code) + "," + str(full_code) + ",4,'" + name + "')"
            print(sql,flush=True)
            cursor.execute(sql)
            tmp = '{:0>2d}/{:0>2d}/{:s}'.format(parent_code // 10000,(parent_code // 100)%100, countyitems[0]["href"])
            # CrawlTown(home_url + tmp, cursor)
            new_task = "insert into t_crawl_task (url,level) values ('" + tmp + "',4)"
            cursor.execute(new_task)

def CrawlTown(url,cursor):
    # print(url)
    # rsp = requests.get(url)
    # context = rsp.content.decode("gb2312")
    # bs = BeautifulSoup(context, "html.parser")
    bs = crawl(url)
    for uri in bs.find_all('tr', "villagetr"):
        countyitems = uri.find_all('td')
        if (len(countyitems) < 3):
            continue
        print(countyitems[0].string, countyitems[1].string, countyitems[2].string,flush=True)
        full_code = int(countyitems[0].string)
        code = full_code % 1000
        parent_code = full_code // 1000
        name = countyitems[2].string
        if (cursor != None):
            sql = "insert into t_area_division (code,full_code,level,name) values (" + "{:03d}".format(code) + "," + str(full_code) + ",5,'" + name + "')"
            print(sql,flush=True)
            cursor.execute(sql)


def CrawlChina():
    conn = sqlite3.connect('spider.db')
    cursor = conn.cursor()

    rsp = requests.get(home_url+"index.html")
    context = rsp.content.decode('gb2312')
    bs = BeautifulSoup(context,"html.parser")
    for uri in bs.find_all('a',href=re.compile("\d\d.html")):
        code = int(uri["href"].split(".")[0])
        name = uri.contents[0]
        sql = "insert into t_area_division (code,full_code,level,name) values (" +str(code) +","+ str(code) + ",1,'"+name+"')"
        cursor.execute(sql)
        CrawlProvince(home_url+uri["href"],cursor)
        break

    cursor.close()
    conn.commit()
    conn.close()


def GenTopTask():
    conn = sqlite3.connect('spider.db')
    cursor = conn.cursor()

    rsp = requests.get(home_url+"index.html")
    context = rsp.content.decode('gb2312')
    bs = BeautifulSoup(context,"html.parser")
    for uri in bs.find_all('a',href=re.compile("\d\d.html")):
        code = int(uri["href"].split(".")[0])
        name = uri.contents[0]
        sql = "insert into t_area_division (code,full_code,level,name) values (" +str(code) +","+ str(code) + ",1,'"+name+"')"
        cursor.execute(sql)
        new_task = "insert into t_crawl_task (url,level) values ('"+uri["href"]+"',1)"
        cursor.execute(new_task)
        #CrawlProvince(home_url+uri["href"],cursor)
        #break
    cursor.close()
    conn.commit()
    conn.close()

def ProcessTask():
    conn = sqlite3.connect('spider.db')
    while True:
        try:
            cursor = conn.cursor()
            tasks = cursor.execute("select * from t_crawl_task where status=0")
            if(tasks.arraysize==0):
                break
            for task in tasks:
                print(task)
                level = int(task[2])
                if(level == 1):
                    CrawlProvince(home_url+task[1],cursor)
                elif(level == 2):
                    CrawlCity(home_url+task[1],cursor)
                elif(level == 3):
                    CrawlCounty(home_url+task[1],cursor)
                elif(level == 4):
                    CrawlTown(home_url + task[1], cursor)
                else:
                    print("error",flush=True)
                cursor.execute("update t_crawl_task set status=1 where id="+str(task[0]))
            cursor.close()
            conn.commit()
        except Exception as e:
            print(e)
    conn.close()
    print("success complated")

def CreateTable():
    conn = sqlite3.connect('spider.db')
    cursor = conn.cursor()
    cursor.execute(create_sql)
    cursor.execute(task_sql)
    cursor.close()
    conn.commit()
    conn.close()

if __name__ == "__main__":
    #CrawlChina()
    # CrawlSheng("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/53.html")
    # CrawlCity("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/34/3413.html",None)
    # CrawlCounty("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/53/01/530102.html")
    # CrawlTown("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/53/01/02/530102001.html")
    # CreateTable()
    # GenTopTask()
    ProcessTask()






