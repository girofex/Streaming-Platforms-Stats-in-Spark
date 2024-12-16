import os

os.system('clear')

print('Final assignment of Distributed Computing course\n')
print('=========================================')
query = input('\nQUERIES: \n\n\
1: Top 20 highest rated titles on Netflix\n\
2: Top 10 most popular genres on Prime Video\n\
3: The number of titles released in 2001 on both platforms\n\
4: Most popular saga on both platforms\n\
5: The tv show(s) that is (are) most distributed\n\n\
Select which query to run: ')

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
