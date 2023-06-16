import pyodbc
import datetime
import time

def save_data(capture_time, utilization, total_bytes):
    connection_string = "DRIVER={SQL Server};SERVER=EDELNMNONTRDB01;DATABASE=tempdb;UID=nuvama;PWD=nuvama@123"
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    insert_query = '''
    INSERT INTO bandwidth_utilization (capture_time, utilization, total_bytes)
    VALUES (?, ROUND(?, 2), ?)
    '''
    cursor.execute(insert_query, capture_time, utilization, total_bytes)
    connection.commit()

    cursor.close()
    connection.close()

while True:
    capture_time = datetime.datetime.now()
    utilization = 0.00
    total_bytes = 0.85 * (1024 ** 2)  # 0.85 MB to bytes

    save_data(capture_time, utilization, total_bytes)
    print(f"Data saved: Capture Time: {capture_time}, Bandwidth Utilization: {utilization:.2f}%, Total Bytes: {total_bytes / (1024 ** 3):.2f} GB / {total_bytes / (1024 ** 2):.2f} MB")

    time.sleep(5)
