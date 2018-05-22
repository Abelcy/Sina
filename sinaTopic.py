# -*- coding: utf-8 -*-
"""
Created on Sat Mar 24 15:32:25 2018

@author: 红笺小字
"""

import json
import requests
import time
import csv
import mysql.connector
import logging; logging.basicConfig(level=logging.INFO)

def Get_json(url): 
    '''
    获取json数据
    '''     
    response = requests.get(url,timeout=10)
 #   res = response.text.encode('latin-1').decode('unicode_escape')#将unicode处理成中文    
    try:
        res = json.loads(response.text)
    except:
        res = response.text
    return res

def handle(res):
    if res["ok"]==1:
       cards = res["data"]["cards"][0]
       since_id = res["data"]["pageInfo"]["since_id"]
       since_id = json.loads(since_id)
       last_since_id = since_id["last_since_id"]
       next_since_id = since_id["next_since_id"] 
       url = "https://m.weibo.cn/api/container/getIndex?from=feed&type=topic&value=%E5%8F%8D%E5%87%BB%E8%B4%B8%E6%98%93%E6%88%98&containerid=100808fe9b926ab9ea40108ec35bda0c50b05d&since_id=%7B%22last_since_id%22:{},%22res_type%22:1,%22next_since_id%22:{}%7D".format(last_since_id,next_since_id)
       for i in range(0,10):
           try:
               card_group = cards["card_group"]
               mblog = card_group[i]["mblog"]
    
               #评论信息
               mid = mblog["mid"]#信息id
               screen_name = mblog["user"]["screen_name"]#昵称
               created_at = mblog["created_at"]#创建时间
               source = mblog["source"]#来源
               text = mblog["text"]#文本               
               attitudes_count = mblog["attitudes_count"]#点赞数
               comments_count= mblog["comments_count"]#评论数
               reposts_count = mblog["reposts_count"]#转发数
               isLongText = mblog["isLongText"]#是否长文章
               textLength = mblog["textLength"]#文字字数
               #个人信息
               oid = mblog["user"]["id"]#id
               description = mblog["user"]["description"]#简介
               follow_count = mblog["user"]["follow_count"]#关注数
               follow_counters = mblog["user"]["followers_count"]#粉丝数
               gender = mblog["user"]["gender"]#性别
               statuses_count = mblog["user"]["statuses_count"]#发文数
               urank = mblog["user"]["urank"]#等级
               verified = mblog["user"]["verified"]#是否认证
               
               if verified == True:
                   verified_reason = mblog["user"]["verified_reason"]#认证原因          
               else:
                   verified_reason = 'None'
               verified_type = mblog["user"]["verified_type"]
               
               if verified_type != -1:
                   verified_type_ext = mblog["user"]["verified_type_ext"]
               else:
                   verified_type_ext = 'None'
                   
               Res.append([mid,screen_name,created_at,source,text,
                       attitudes_count,comments_count,reposts_count,
                       isLongText,textLength,oid,description,follow_count,
                       follow_counters,gender,statuses_count,urank,
                       verified,verified_reason,verified_type,verified_type_ext])
           except:
               break
       return url
    
def Mysql(Res):
    '''
    写入数据库
    '''
    conn = mysql.connector.connect(user='root', password='123456')
    cursor = conn.cursor()
    cursor.execute('create database if not exists Weibo_info')
    cursor.execute('ALTER DATABASE Weibo_info CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci')
    cursor.execute('use Weibo_info')
    cursor.execute('drop table if exists Weibo_info') 
    cursor.execute('create table Weibo_info (mid varchar(255), screen_name varchar(255), created_at varchar(255), source varchar(255), 文本 TEXT, attitudes_count varchar(255), comments_count varchar(255), reposts_count varchar(255), isLongText varchar(255), textLength varchar(255), oid varchar(255), description TEXT, follow_count varchar(255),followers_count varchar(255),gender varchar(255), statuses_count varchar(255), urank varchar(255), verified varchar(255), verified_reason varchar(255), verified_type varchar(255), verified_type_ext varchar(255))')      
    cursor.execute('ALTER TABLE Weibo_info CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci')
    cursor.execute('ALTER TABLE Weibo_info modify 文本 text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;')
    cursor.execute('set names utf8mb4')
    for row in Res:
            #这句怎么优化？
            cursor.execute('insert into Weibo_info (mid,screen_name,created_at,source,文本,attitudes_count,comments_count,reposts_count,isLongText,textLength,oid,description,follow_count,followers_count,gender,statuses_count,urank,verified,verified_reason,verified_type,verified_type_ext) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',[row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20]])
    conn.commit()
    cursor.close()

         
def fwrite(file,Res,Label):
    '''
    写入文件
    '''
    with open(file,'a',newline='',encoding='GB18030') as f:
        writer = csv.writer(f)        
        writer.writerow(Label)
        for row in Res:
            writer.writerow(row)

        

if __name__ == '__main__':
    Res = []
    file = 'WeiBo_Info.csv'
    Label = ['mid','screen_name','created_at','source','text',
                       'attitudes_count','comments_count','reposts_count',
                       'isLongText','textLength','oid','description','follow_count',
                       'followers_count','gender','statuses_count','urank',
                       'verified','verified_reason','verified_type','verified_type_ext']    
    url = 'https://m.weibo.cn/api/container/getIndex?from=feed&type=topic&value=%E5%8F%8D%E5%87%BB%E8%B4%B8%E6%98%93%E6%88%98'
    for i in range(1,700):
        time.sleep(0.1)       
        try:
            Json = Get_json(url)  
            url = handle(Json)
            logging.info("爬取第{}页成功".format(i))
        except:
            break
    fwrite(file,Res,Label)
    Mysql(Res)    
    logging.info("全部完成")