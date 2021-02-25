import requests
from bs4 import BeautifulSoup
import re
import sqlite3

# conn = sqlite3.connect('test.db')
#
# cursor = conn.cursor()
# #执行一条SQL语句：创建user表
# cursor.execute('create table user(id varchar(20) primary key,name varchar(20))')
# #插入一条记录：
# cursor.execute('insert into user (id, name) values (\'1\', \'Michael\')')
# #通过rowcount获得插入的行数：
# print(cursor.rowcount) #reusult 1
# #关闭Cursor:
# cursor.close()
# #提交事务：
# conn.commit()
# #关闭connection：
# conn.close()

home_url="https://www.guisd.com"

mobile_prefix={
'134':{'op':'Mobile','net':'GSM'},
'135':{'op':'Mobile','net':'GSM'},
'136':{'op':'Mobile','net':'GSM'},
'137':{'op':'Mobile','net':'GSM'},
'138':{'op':'Mobile','net':'GSM'},
'139':{'op':'Mobile','net':'GSM'},
'150':{'op':'Mobile','net':'GSM'},
'151':{'op':'Mobile','net':'GSM'},
'152':{'op':'Mobile','net':'GSM'},
'157':{'op':'Mobile','net':'TD-SCDMA'},
'158':{'op':'Mobile','net':'GSM'},
'159':{'op':'Mobile','net':'GSM'},
'188':{'op':'Mobile','net':'TD-SCDMA'},
'130':{'op':'Unicom','net':'GSM'},
'131':{'op':'Unicom','net':'GSM'},
'132':{'op':'Unicom','net':'GSM'},
'155':{'op':'Unicom','net':'GSM'},
'156':{'op':'Unicom','net':'GSM'},
'185':{'op':'Unicom','net':'WCDMA'},
'186':{'op':'Unicom','net':'WCDMA'},
'133':{'op':'Telecom','net':'CDMA'},
'153':{'op':'Telecom','net':'CDMA'},
'189':{'op':'Telecom','net':'CDMA'}
}



create_sql='''
CREATE TABLE t_mobile_number(
  id INTEGER NOT NULL PRIMARY KEY autoincrement,
  number varchar(7) NOT NULL DEFAULT '',
  operators varchar(10) NOT NULL DEFAULT '0',
  area varchar(50) NOT NULL DEFAULT '',
  area_code varchar(20) NOT NULL DEFAULT '',
  net_type varchar(10) NOT NULL DEFAULT '',
  ctime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  utime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
'''
task_sql='''
CREATE TABLE t_crawl_task(
  id INTEGER NOT NULL PRIMARY KEY autoincrement,
  url varchar(256) NOT NULL DEFAULT '',
  area_name varchar(100) NOT NULL DEFAULT '',
  status tinyint(3) NOT NULL DEFAULT '0',
  ctime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  utime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
'''


def CrawlData():
    context =""
    try:
        with open("guisd.data",'r',encoding='utf-8') as f:
            context=f.read()
    except  FileNotFoundError:
        print("get from www")
        url="https://www.guisd.com/ss/"
        rsp = requests.get(url)
        context = rsp.content.decode('utf-8')
        with open("guisd.data",'w',encoding='utf-8') as wf:
            wf.write(context)

    bs = BeautifulSoup(context,"html.parser")
    for uri in bs.find_all('a',href=re.compile("^/ss/[a-z]+")):
        print(uri.string)
        item = uri['href'].split('/')
        if(len(item)==4):
            # print("省份:"+ item[2])
            continue
        elif (len(item)==5):
            print("市:" + item[3] + " " + uri["href"])

def CrawlCitiy(url):
    rsp = requests.get(url)
    context = rsp.content.decode('utf-8')
    bs = BeautifulSoup(context, "html.parser")
    list = []
    for uri in bs.find_all('a',href=re.compile("^/sj/\d\d\d\d\d\d\d/")):
        prefix = int(uri.string) // 10000
        print(uri.string,mobile_prefix[str(prefix)]["op"],mobile_prefix[str(prefix)]["net"])
        list.append((uri.string,mobile_prefix[str(prefix)]["op"],mobile_prefix[str(prefix)]["net"]))
    return list

def GenTask():
    conn = sqlite3.connect('guisd.db')
    cursor = conn.cursor()
    url = "https://www.guisd.com/ss/"
    rsp = requests.get(url)
    context = rsp.content.decode('utf-8')
    bs = BeautifulSoup(context, "html.parser")
    for uri in bs.find_all('a', href=re.compile("^/ss/[a-z]+")):
        #print(uri.string)
        item = uri['href'].split('/')
        if (len(item) == 4):
            # print("省份:"+ item[2])
            continue
        elif (len(item) == 5):
            #print("市:" + item[3] + " " + uri["href"])
            new_task = "insert into t_crawl_task (url,area_name) values ('" +uri["href"] +"','" + uri.string + "')"
            print(new_task)
            cursor.execute(new_task)
    cursor.close()
    conn.commit()
    conn.close()

def ProccessTask():
    conn = sqlite3.connect('guisd.db')
    cursor = conn.cursor()
    while True:
        try:
            tasks = cursor.execute("select * from t_crawl_task where status=0")
            if (tasks.arraysize == 0):
                break
            for task in tasks:
                print(task)
                cursor2 = conn.cursor()
                items = CrawlCitiy(home_url+task[1])
                for item in items:
                    sql = "insert into t_mobile_number (number,operators,area,net_type) values ('"+item[0]+"','"+item[1]+"','"+ task[2]+"','"+item[2]+"')"
                    print(sql)
                    cursor2.execute(sql)
                cursor2.execute("update t_crawl_task set status=1 where id=" + str(task[0]))
                cursor2.close()
                conn.commit()
        except Exception as e:
            print(e)
    cursor.close()
    conn.close()
    print("success complated")

def CreateTable():
    conn = sqlite3.connect('guisd.db')
    cursor = conn.cursor()
    cursor.execute(create_sql)
    #cursor.execute(task_sql)
    cursor.close()
    conn.commit()
    conn.close()


if __name__ == "__main__":
    #CrawlData()
    #CreateTable()
    #GenTask()
    #CrawlCitiy("https://www.guisd.com/ss/shandong/dongying/")
    ProccessTask()



