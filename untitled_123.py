# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 10:38:50 2020

@author: pc
"""


# -*- coding: utf-8 -*-
import codecs
import csv
import sys  
import sqlite3
#######
#for read all file 
import glob
import pandas as pd

files = glob.glob("*.csv")
dfs = [pd.read_csv(f, header=None, sep=";") for f in files]
salesdata = pd.concat(dfs,ignore_index=True)

#######
#for create table in DB
conn = sqlite3.connect('alaa_new.db')
print ("success conn")

c = conn.cursor()

c.execute('''CREATE TABLE Student
           (name text ,
           avg real,
           part int, 
           school int,
           city int,
           year int
           )''')
print("table Student  created ")

c.execute('''CREATE TABLE City
             ( city_id integer  , 
             city text
             
             )''')
           
print("table city  created ")

conn.commit()
####################

c.execute('''CREATE TABLE School 
             ( School_id integer ,
             School text
             
             )''')
print("table school  created ")

conn.commit()

######################
c.execute('''CREATE TABLE part 
             ( part_id integer , 
             part text 
             
             )''')
print("table part  created ")

conn.commit()
#####################


#c.execute('''CREATE TABLE Tawjihi_all
     #       (name text, avg real, part text, school text,city text,year real)''')
#print("table tawjihi  created ")


conn.commit()

#conn.commit()
#######
my_dict_city = {}
my_dict_School={}
my_dict_part={}

            
c.execute("select city,city_id from city")         
rowcity = c.fetchall()
print("from city")
#print (rowcity)

if rowcity != [] :
     
     my_dict_city[0]=rowcity[0]
     my_dict_city[1]=rowcity[1]
    
 #------------------------------
c.execute("select part,part_id from part")
rowpart = c.fetchall()
print("from part")
#print (rowpart)

if rowpart != [] :
     
     my_dict_part[0]=rowpart[0]
     my_dict_part[1]=rowpart[1]  
     
#--------------------------------   
c.execute("select school,school_id from School")
rowschool = c.fetchall()
print("from school")
#print (rowschool)

if rowschool != [] :
    
     my_dict_School[0]=rowschool[0]
     my_dict_School[1]=rowschool[1]

     
#--------------------------------  
#print conant of file on table 
     
#count1=0#city
#count3=0#school
#=0#part
for x in files :
     with open(x, encoding='utf-8' ) as csvfile:                                                     
        reader = csv.reader(csvfile)
        
        for (row) in reader:
            
            if not row[3] in my_dict_School.keys():
                 
                 dicSize=len(my_dict_School)
                 my_dict_School[row[3]]=dicSize
                 to_schooldb=[dicSize,row[3]]
                 #print(to_schooldb)
                 #c.execute("INSERT INTO part (part_id,part) VALUES (1,'شمشش'))
                 #c.excute("INSERT INTO part VALUES  (count2) , ")
                 c.execute("INSERT INTO School(School_id,School) VALUES (? ,?);" , to_schooldb)
                 #c.excute(string)
                 #c.execute("INSERT INTO part (part) VALUES (?);" , to_]partdb)
                 #count3=count3+1    
                 conn.commit()
                 
            if not row[4] in my_dict_city.keys():
                 dicSize=len(my_dict_city)
                 my_dict_city[row[4]]=dicSize
                 to_citydb=[dicSize,row[4]]
                 #print(to_citydb)
                 #c.excute("INSERT INTO part VALUES  (count2) , ")
                 c.execute("INSERT INTO City (city_id,city) VALUES (? ,?);" , to_citydb)
                 #c.excute(string)
                 #c.execute("INSERT INTO part (part) VALUES (?);" , to_]partdb)
                 #count1=count1+1    
                 conn.commit()
#print(my_dict_city) 
            
          
            if not row[2] in my_dict_part.keys():
                 dicSize=len(my_dict_part)
                 my_dict_part[row[2]]=dicSize
                 to_partdb=[dicSize,row[2]]
                 #print(to_partdb)
                 #c.excute("INSERT INTO part VALUES  (count2) , ")
                 c.execute("INSERT INTO part (part_id,part) VALUES (? ,?);" , to_partdb)
                 #c.excute(string)
                 #c.execute("INSERT INTO part (part) VALUES (?);" , to_]partdb)
                 #count2=count2+1    
                 conn.commit()
            #print(my_dict_School.values());
            
            #print(my_dict_School.keys())
            
           
            #c.execute("INSERT INTO Student  VALUES ('"+row[0]+"',"+row[1]+","+my_dict_part.get('count2')+","+my_dict_School.get('count3')+","+my_dict_city.get('count1')+",'"+row[5]+"');")
            
            c.execute("INSERT INTO student VALUES ( ?, ? ,? ,? ,? ,?) ",[row[0] ,row[1] ,my_dict_part.get(row[2] ) , my_dict_School.get(row[3] ) ,my_dict_city.get(row[4]) ,row[5]])
           #count3=count3+1
            #count3=count3+1
            #count2=count2+1
            #c.execute("INSERT INTO Student  VALUES ( row[0] , row[1] , my_dict_part.get('count2') , my_dict_School.get('count3'), my_dict_city.get('count1') , row[5]);")    
            #conn.commit()
#print(my_dict_part) 
                 
            #to_db = [ row[0], row[1], row[2], row[3], row[4] ,row[5] ]


