import os

os.system('clear')

print('Final assignment of Distributed Computing course')
print('=========================================')
query = input('\nSelect which query to run: \n\n\
1: The 20 most viewed movies on Netflix\n\
2: The 10 most popular genres on Prime Video\n\
3: The number of released content in year 2001 on both platforms\n\
4: Most popular saga on both platforms\n\n\
5: Highest rating\n\n')

if query == '1':
    os.system('clear')
    os.system('spark-submit --master yarn QUERIES/Q1.py 2> /dev/null')
elif query == '2':
    os.system('clear')
    os.system('spark-submit --master yarn QUERIES/Q2.py 2> /dev/null')
elif query == '3':
    os.system('clear')
    os.system('spark-submit --master yarn QUERIES/Q3.py 2> /dev/null')
elif query == '4':
    os.system('clear')
    os.system('spark-submit --master yarn QUERIES/Q4.py 2> /dev/null')
elif query == '5':
    os.system('clear')
    os.system('spark-submit --master yarn QUERIES/Q5.py 2> /dev/null')
else:
    print('Invalid query')
