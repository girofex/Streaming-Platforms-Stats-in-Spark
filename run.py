import os, time as t

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

configs = [
    "local[1]",
    "local[4]",
    "local[*]",
    "cluster"
]

if query not in query_paths:
    print('Invalid query')

else:
    os.system('clear')
    
    query_script = query_paths[query]
    print(f'\nRunning Q{query} with different configurations\n')
    
    for mode in configs:
        print('=========================================')
        print(f'Mode = {mode}:\n')

        start = t.time()
        
        if (mode != "cluster"):
            spark_command = f'spark-submit --master {mode} {query_script} 2> /dev/null'

        else:
            spark_command = f'spark-submit --master yarn --deploy-mode cluster {query_script}'

        exit_code = os.system(spark_command)
        
        if exit_code != 0:
            print("Error")

        finish = t.time()

        time = finish - start
        print(f"Time spent: {time}")