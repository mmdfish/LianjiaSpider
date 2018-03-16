'''
@author: mmdfish
'''

'''
最常见的是用urllib来做，但是在不用代理的情况下一定会被链家封IP,即使用Headers伪装也没有做到(至少我没有尝试成功)
也试过每访问多少次sleep一段时间，后来也失效了。所以采用了直接调用chrome的方式，会慢一些，但是用多线程可以节省一下时间。
现在链家的成交，近30天的成交价都必须用app来查看，所以只能进到每一个网页
'''
from bs4 import BeautifulSoup
import re
import time
from selenium import webdriver
import os

import threading 

home_url = u"http://bj.lianjia.com"
xiaoqu_url = u"http://bj.lianjia.com/xiaoqu"
chengjiao_url = u"https://bj.lianjia.com/chengjiao"

chromedriver = "C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe"  
os.environ["webdriver.chrome.driver"] = chromedriver  

class LJCJ_SPIDER(object):

    def __init__(self):
        self.last_date = "2017.09.26"
        self.current_date = "2017.09.26"
        self.get_date()
        self.count = 0
        self.isFirst = True

    def get_date(self):
        f = open("last_date.txt", "r")
        content = f.readline()
        dates = content.split()
        self.last_date = dates[0].strip()
        f.close()

    def save_date(self):
        f = open("last_date.txt", "w+")
        f.write(self.current_date + " " + self.last_date)
        f.close()

    def loop_chengjiao_spider(self):
        page_url = chengjiao_url
        self.chengjiao_list_page_spider_thread(page_url)
    
    def chengjiao_list_page_spider_thread(self, url_page=u"http://bj.lianjia.com/chengjiao/"):
        threadnumber = 5
        threads = [] #创建一个线程列表，用于存放需要执行的子线程
        urlmap = []

        for i in range(0, threadnumber):
            urlmap += [[]]
        for j in range(0, 99):
            urlmap[j % threadnumber] += [j]
            
        for i in range(0, threadnumber):
            t = threading.Thread(target=self.chengjiao_list_page_spider, args=(url_page,urlmap[i],))
            threads.append(t)
 
        for t in threads: #遍历线程列表
            t.setDaemon(True) #将线程声明为守护线程，必须在start() 方法调用之前设置，如果不设置为守护线程程序会被无限挂起
            t.start() #启动子线程
        for t in threads: #遍历线程列表
            t.join()

    def chengjiao_list_page_spider(self, url_page=u"http://bj.lianjia.com/chengjiao/", pages =[1,2,3,4,5]):
        browser = webdriver.Chrome(chromedriver)
        browser.get('http://bj.lianjia.com/chengjiao/')
        main_handle = browser.current_window_handle
    
        for i in pages:
            str_pg = "/pg%d" % i
            url_page_pg = url_page + str_pg
            result = self.chengjiao_list_url_spider(browser, main_handle, url_page_pg)
            if result == -1:
                return
            time.sleep(1)
        browser.close()
    def chengjiao_list_url_spider(self, browser, main_handle, url_page=u"http://bj.lianjia.com/chengjiao/pg1" ):
        self.count = self.count + 1
        print("parse url %s count %d" % (url_page, self.count))
        try:
            newwindow = 'window.open("%s");' % (url_page)
            browser.execute_script(newwindow)
            all_handles = browser.window_handles 
            browser.switch_to_window(all_handles[-1])
            plain_text = browser.page_source.encode('utf-8').decode()
            soup = BeautifulSoup(plain_text, "html5lib")
        except Exception as e:
            print(e)
            exit(-1)
        chengjiao_list = soup.findAll('div', {'class': 'info'})
        for cj in chengjiao_list:
            try:
                info_dict = {}
                title_info = cj.find('div', {'class': 'title'})
                link_info = title_info.find('a')
                link = link_info.get('href')
                info_dict.update({u'链接': link})
                info = re.match(chengjiao_url + "/(.+)\.html", link)
                cj_id = info.group(1)
                info_dict.update({u'编号': cj_id})
                info = title_info.text
                infos = info.split()
                info_dict.update({u'小区名称': infos[0]})
                info_dict.update({u'户型': infos[1]})
                info_dict.update({u'面积': infos[2]})

                house_info = cj.find('div', {'class': 'houseInfo'})
                info_dict.update({u'房屋信息': house_info.text.strip()})
                deal_info = cj.find('div', {'class': 'dealDate'})
                if(deal_info.text.strip() <= self.last_date):
                    self.save_date()
                    browser.close()
                    browser.switch_to_window(main_handle)
                    return -1
                info_dict.update({u'签约时间': deal_info.text.strip()})
                price_info = cj.find('div', {'class': 'totalPrice'})
                info_dict.update({u'签约总价': price_info.text.strip()})
                position_info = cj.find('div', {'class': 'positionInfo'})
                infos = position_info.text.split()
                info_dict.update({u'楼层': infos[0]})
                if(len(infos) > 1):
                    infos = infos[1].split(u'建')
                    info_dict.update({u'建造年份': infos[0]})
                    if(len(infos) > 1):
                        info_dict.update({u'楼型': infos[1]})
                price_info = cj.find('div', {'class': 'unitPrice'})
                info_dict.update({u'单价': price_info.text.strip()})

                house_deal_info = cj.find('div', {'class': 'dealHouseInfo'})
                if house_deal_info != None:
                    if(house_deal_info.text.find(u'年') != -1):
                        info_dict.update({u'房本时间': house_deal_info.text[:5]})

                deal_cycle_info = cj.find('div', {'class': 'dealCycleeInfo'})
                if deal_cycle_info != None:
                    index = deal_cycle_info.text.find(u'成交')
                    if(index == -1):
                        info_dict.update({u'挂牌价格': deal_cycle_info.text})
                    else:
                        info_dict.update(
                            {u'挂牌价格': deal_cycle_info.text[:index]})
                        info_dict.update(
                            {u'成交周期': deal_cycle_info.text[index:]})

                if(u'*' in info_dict.get(u'签约总价')):
                    self.chengjiao_url_parser(browser, link , info_dict)

                if self.isFirst:
                    self.current_date = info_dict.get(u'签约时间')
                    self.isFirst = False
                if(info_dict.get(u'签约时间') <= self.last_date):
                    print('finish this region')
                    self.save_date()
                    browser.close()
                    browser.switch_to_window(main_handle)
                    return -1
                command = self.gen_chengjiao_insert_command(info_dict)
                print(command)

            except Exception as e:
                print(e)
        browser.close()
        browser.switch_to_window(main_handle)    

    def chengjiao_url_parser(self, browser, url_page=u"https://bj.lianjia.com/chengjiao/101101505448.html", info_dict={}):
        self.count = self.count + 1
        print("parse url %s count %d" % (url_page, self.count))
        try:
            now_handle = browser.current_window_handle
            newwindow = 'window.open("%s");' % (url_page)
            browser.execute_script(newwindow)
            all_handles = browser.window_handles 
            browser.switch_to_window(all_handles[-1])
            plain_text = browser.page_source.encode('utf-8').decode()
            soup = BeautifulSoup(plain_text, "html5lib")
            time.sleep(1)
            browser.close()
            browser.switch_to_window(now_handle)
        except Exception as e:
            print(e)
            exit(-1)
        price_info = soup.find('div', {'class': 'price'})
        total_price_info = price_info.find('span', {'class': 'dealTotalPrice'})
        unit_price_info = price_info.find('b')
        info_dict.update({u'签约总价': total_price_info.text.strip()})
        info_dict.update({u'单价': unit_price_info.text.strip()})

        house_title_info = soup.find('div', {'class': 'house-title'})
        date_info = house_title_info.find('span')
        deal_date = date_info.text.split()[0].strip()
        info_dict.update({u'签约时间': deal_date})

    def gen_chengjiao_insert_command(self, info_dict):
        """
            生成小区数据库插入命令
        """
        info_list = [u'编号', u'链接', u'小区名称', u'建造年份', u'楼型', u'楼层', u'户型', u'面积', u'房屋信息',
                     u'签约时间', u'签约总价', u'单价', u'房本时间', u'挂牌价格', u'成交周期']
        t = []
        for il in info_list:
            if il in info_dict:
                t.append(info_dict[il])
            else:
                t.append('')
        t = tuple(t)
        command = (
            r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", t)
        return command
    
if __name__ == "__main__":
    spider = LJCJ_SPIDER()
    url_page = u"http://bj.lianjia.com/chengjiao/"
    spider.chengjiao_list_page_spider_thread(url_page)