# import os

# def list_files(startpath):
#     for root, dirs, files in os.walk(startpath):
#         level = root.replace(startpath, '').count(os.sep)
#         indent = ' ' * 4 * (level)
#         print('{}{}/'.format(indent, os.path.basename(root)))
#         subindent = ' ' * 4 * (level + 1)
#         for f in files:
#             print('{}{}'.format(subindent, f))

import os  
import pathlib  
  
import seedir as sd

path = r'C:\Users\bru-c\Documents\estudo_orientado\spark-wordcount-cassandra'

sd.seedir(path=path, style='lines', itemlimit=10, depthlimit=2, exclude_folders='.git') 

#tree = DirectoryTree(r'C:\Users\bru-c\Documents\estudo_orientado\spark-wordcount-cassandra')  
#tree.generate()  

#list_files(r'C:\Users\bru-c\Documents\estudo_orientado\spark-wordcount-cassandra')