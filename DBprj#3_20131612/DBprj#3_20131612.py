#-*- coding: utf-8 -*-

import datetime
import time
import sys
import MeCab
import operator
from pymongo import MongoClient
from bson import ObjectId
from itertools import combinations

stop_word = {}
DBname = "db20131612"
conn = MongoClient("dbpurple.sogang.ac.kr")
db = conn[DBname]
db.authenticate(DBname, DBname)

def printMenu():
    print "0. CopyData"
    print "1. Morph"
    print "2. print morphs"
    print "3. print wordset"
    print "4. frequent item set"
    print "5. association rule"
    pass

"""
    TODO:
    CopyData news to news_freq
"""
def p0():
    col1 = db['news']
    col2 = db['news_freq']

    col2.drop()

    for doc in col1.find():
        contentDic = {}
        for key in doc.keys():
            if key != "_id":
                contentDic[key] = doc[key]
        col2.insert(contentDic)
    pass

"""
    TODO:
    Morph news and update news db
"""
def p1():
    for doc in db['news_freq'].find():
        doc['morph'] = morphing(doc['content'])
        db['news_freq'].update({"_id":doc['_id']}, doc)
    pass

"""
    TODO:
    input:  news url
    output: news morphs
"""
def p2(url):
    for doc in db['news_freq'].find():
        if doc['url'] == url:
            for morph in doc['morph']:
                print morph.encode('utf-8')
    pass
    
"""
    TODO:
    copy news morph to new db named news_wordset
"""
def p3():
    col1 = db['news_freq']
    col2 = db['news_wordset']
    col2.drop()
    for doc in col1.find():
        new_doc = {}
        new_set = set()
        for w in doc['morph']:
            new_set.add(w.encode('utf-8'))
        new_doc['word_set'] = list(new_set)
        new_doc['url'] = doc['url']
        col2.insert(new_doc)
    pass

"""
    TODO:
    input:  news url
    output: news wordset
"""
def p4(url):
    for doc in db['news_wordset'].find():
        if doc['url'] == url:
            for word in doc['word_set']:
                print word.encode('utf-8')
    pass

"""
    TODO:
    make frequent item_set
    and insert new dbs (dbname = candidate_L+"length")
    ex) 1-th frequent item set dbname = candidate_L1
"""
def p5():
    col1 = db['news_wordset']
    min_sup = col1.find().count() * 0.1

    candi_L1 = db['candidate_L1']
    candi_L1.drop()
    candi_L2 = db['candidate_L2']
    candi_L2.drop()
    candi_L3 = db['candidate_L3']
    candi_L3.drop()

    
    # candidate_L1
    candi_L1_count = dict()    
    for doc in col1.find():
        for word in doc['word_set']:
            count = candi_L1_count.get(word, 0)
            candi_L1_count[word] = count + 1

    tmpList1 = list()
    for key in candi_L1_count.keys():
        if candi_L1_count[key] >= min_sup:
            new_doc = dict()
            tmpList1.append(key)
            new_doc['item_set'] = key
            new_doc['support'] = candi_L1_count[key]
            candi_L1.insert(new_doc)
    
    # candidate_L2
    candi_L2_count = dict()
    for doc in col1.find():
        for i in range(len(tmpList1)):
            for j in range(i+1, len(tmpList1)):
                if tmpList1[i] in doc['word_set'] and tmpList1[j] in doc['word_set']:
                    word = frozenset([tmpList1[i], tmpList1[j]])
                    count = candi_L2_count.get(word, 0)
                    candi_L2_count[word] = count + 1
    
    tmpList2 = list()
    for key in candi_L2_count.keys():
        if candi_L2_count[key] >= min_sup:
            new_doc = dict()
            tmpList2.append(key)
            new_doc['item_set'] = list(key)
            new_doc['support'] = candi_L2_count[key]
            candi_L2.insert(new_doc)

    # candidate_L3
    candi_L3_count = dict()
    for doc in col1.find():
        for i in range(len(tmpList2)):
            for j in range(i+1, len(tmpList2)):
                item1 = list(tmpList2[i])
                item2 = list(tmpList2[j])
                if item1[0] == item2[0] and item1[1] != item2[1]:
                    if item1[0] in doc['word_set'] and item1[1] in doc['word_set'] and item2[1] in doc['word_set']:
                        word = frozenset([item1[0], item1[1], item2[1]])
                        count = candi_L3_count.get(word, 0)
                        candi_L3_count[word] = count + 1

    for key in candi_L3_count.keys():
        if candi_L3_count[key] >= min_sup:
            new_doc = dict()
            new_doc['item_set'] = list(key)
            new_doc['support'] = candi_L3_count[key]
            candi_L3.insert(new_doc)
    pass

"""
    TODO:
    make strong association rule
    and print all of strong rules
    by length-th frequent item set
"""
def p6():
    length = int(raw_input("input length of the frequent item:"))
    min_conf = 0.5
    candi_L1_count = dict()
    candi_L2_count = dict()
    candi_L3_count = dict()
    for doc in db['candidate_L1'].find():
        candi_L1_count[doc['item_set']] = doc['support']
    for doc in db['candidate_L2'].find():
        candi_L2_count[frozenset(doc['item_set'])] = doc['support']
    for doc in db['candidate_L3'].find():
        candi_L3_count[frozenset(doc['item_set'])] = doc['support']

    if length == 2:    
        for keys in candi_L2_count:
            index = 0
            for key in list(keys):
                index = not index
                res = float(candi_L2_count[keys]) / candi_L1_count[key]
                if res > min_conf:
                    print "%s =>%s\t%s" % (key.encode('utf-8'), list(keys)[index].encode('utf-8'), res)
    elif length == 3:
        for keys in candi_L3_count:
            item = list(keys)

            """
                0 -> set(1, 2)
                1 -> set(0, 2)
                2 -> set(0, 1)
                set(1, 2) -> 0
                set(0, 2) -> 1
                set(0, 1) -> 2
            """
            
            res = float(candi_L3_count[keys]) / candi_L1_count[ item[0] ]
            if res > min_conf:
                print "%s =>%s ,%s\t%s" % (item[0].encode('utf-8'), item[1].encode('utf-8'), item[2].encode('utf-8'), res)
            res = float(candi_L3_count[keys]) / candi_L1_count[ item[1] ]
            if res > min_conf:
                print "%s =>%s ,%s\t%s" % (item[1].encode('utf-8'), item[0].encode('utf-8'), item[2].encode('utf-8'), res)
            res = float(candi_L3_count[keys]) / candi_L1_count[ item[2] ]
            if res > min_conf:
                print "%s =>%s ,%s\t%s" % (item[2].encode('utf-8'), item[0].encode('utf-8'), item[1].encode('utf-8'), res)

            res = float(candi_L3_count[keys]) / candi_L2_count[frozenset([item[1], item[2]])]
            if res > min_conf:
                print "%s ,%s=>%s\t%s" % (item[1].encode('utf-8'), item[2].encode('utf-8'), item[0].encode('utf-8'), res)
            res = float(candi_L3_count[keys]) / candi_L2_count[frozenset([item[0], item[2]])]
            if res > min_conf:
                print "%s ,%s=>%s\t%s" % (item[0].encode('utf-8'), item[2].encode('utf-8'), item[1].encode('utf-8'), res)
            res = float(candi_L3_count[keys]) / candi_L2_count[frozenset([item[0], item[1]])]
            if res > min_conf:
                print "%s ,%s=>%s\t%s" % (item[0].encode('utf-8'), item[1].encode('utf-8'), item[2].encode('utf-8'), res)
    pass
            

def make_stop_word():
    f = open("wordList.txt", 'r')
    while True:
        line = f.readline()
        if not line: break
        stop_word[line.strip('\n')] = line.strip('\n')
    f.close()
    pass

def morphing(content):
    t = MeCab.Tagger('-d/usr/local/lib/mecab/dic/mecab-ko-dic')
    nodes = t.parseToNode(content.encode('utf-8'))
    MorpList = []
    while nodes:
        if nodes.feature[0] == 'N' and nodes.feature[1] == 'N':
            w = nodes.surface
            if not w in stop_word:
                try:
                    w = w.encode('utf-8')
                    MorpList.append(w)
                except:
                    pass
        nodes = nodes.next
    return MorpList

if __name__ == "__main__":
    make_stop_word()
    printMenu()
    selector = input()
    if selector == 0:
        p0()
    elif selector == 1:
        p1()
        p3()
    elif selector == 2:
        url = str(raw_input("input news url:"))
        p2(url)
    elif selector == 3:
        url = str(raw_input("input news url:"))
        p4(url)
    elif selector == 4:
        #length = int(raw_input("input length of the frequent item:"))
        p5()
    elif selector == 5:
        #length = int(raw_input("input length of the frequent item:"))
        p6()
    pass
