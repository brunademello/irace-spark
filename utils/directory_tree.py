import os  
import pathlib  
  
import seedir as sd

path = '/home/ubuntu/github/spark-wordcount-cassandra'

sd.seedir(path=path, style='lines', itemlimit=10, depthlimit=2, exclude_folders='.git') 

