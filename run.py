import os

os.system('clear')

print('Final assignment of Distributed Computing course\n')
print('=========================================')

query = input('\nQUERIES: \n\n\
1: Top 20 highest rated titles on Netflix\n\
2: Top 10 most popular genres on Prime Video\n\
3: The number of titles released in 2001 on both platforms\n\
4: Most popular movie present on both platforms\n\
5: The tv show(s) that is (are) most distributed\n\n\
Select which query to run: ')

query_paths = {
    '1': 'QUERIES/Q1.py',
    '2': 'QUERIES/Q2.py',
    '3': 'QUERIES/Q3.py',
    '4': 'QUERIES/Q4.py',
    '5': 'QUERIES/Q5.py'
}

master_configs = [
    "local[1]",
    "local[4]",
    "local[*]",
    "yarn"
]

if query not in query_paths:
    print('Invalid query')

else:
    os.system('clear')
    
    query_script = query_paths[query]
    print(f'\nRunning Q{query} with different configurations\n')
    
    for master in master_configs:
        print('=========================================')
        print(f'\nMaster = {master}:\n')
        
        spark_command = f'spark-submit --master {master} {query_script} 2> /dev/null'
        exit_code = os.system(spark_command)
        
        if exit_code != 0:
            print("Error")
