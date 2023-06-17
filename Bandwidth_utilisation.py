import os
import pyodbc
import datetime
import time
import sys
import psutil

class BandwidthUtilization:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.max_reconnection_attempts = 3
        self.reconnection_delay = 5  # seconds
        self.last_flush_time = datetime.datetime.now()
        self.flush_interval_minutes = 30

        self.connection = None
        self.cursor = None
        self.reconnection_count = 0

    def connect(self):
        connection_string = f"DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
        try:
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            self.reconnection_count = 0  # Reset reconnection count
            print("Database connection successful.")
        except pyodbc.Error as e:
            error_message = "Error connecting to the database: " + str(e)
            print(error_message)
            sys.exit(1)

    def create_table(self):
        table_exists_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'bandwidth_utilization'"
        self.cursor.execute(table_exists_query)
        table_exists = self.cursor.fetchone()

        if table_exists:
            print("Table 'bandwidth_utilization' already exists in the database.")
        else:
            create_table_query = '''
            CREATE TABLE bandwidth_utilization (
                id INT IDENTITY(1,1) PRIMARY KEY,
                capture_time DATETIME,
                utilization FLOAT,
                total_bytes BIGINT
            )
            '''
            try:
                self.cursor.execute(create_table_query)
                self.connection.commit()
                print("Table 'bandwidth_utilization' created successfully.")
            except pyodbc.Error as e:
                error_message = "Error creating table: " + str(e)
                print(error_message)

    def save_data(self, capture_time, utilization, total_bytes):
        insert_query = '''
        INSERT INTO bandwidth_utilization (capture_time, utilization, total_bytes)
        VALUES (?, ROUND(?, 2), ?)
        '''
        self.cursor.execute(insert_query, capture_time, utilization, total_bytes)
        self.connection.commit()
        print(f"Data saved: Capture Time: {capture_time}, Bandwidth Utilization: {utilization:.2f}%, Total Bytes: {total_bytes / (1024 ** 3):.2f} GB / {total_bytes / (1024 ** 2):.2f} MB")

    def check_flush_database(self):
        current_time = datetime.datetime.now()
        time_difference_minutes = (current_time - self.last_flush_time).total_seconds() / 60

        if time_difference_minutes >= self.flush_interval_minutes:
            self.flush_database()
            self.last_flush_time = current_time

    def flush_database(self):
        truncate_query = "TRUNCATE TABLE bandwidth_utilization"
        self.cursor.execute(truncate_query)
        self.connection.commit()
        #print("Database flushed successfully.")

    def close_connection(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()

    def truncate_log_file(self, log_path, max_size_mb):
        if os.path.exists(log_path):
            log_size = os.path.getsize(log_path) / (1024 * 1024)
            if log_size > max_size_mb:
                with open(log_path, 'w') as log_file:
                    log_file.truncate()
                print(f"Log file truncated: {log_path}")

    def reconnect(self):
        self.close_connection()
        if self.reconnection_count < self.max_reconnection_attempts:
            self.reconnection_count += 1
            print(f"Reconnecting... (Attempt {self.reconnection_count})")
            time.sleep(self.reconnection_delay)
            self.connect()
        else:
            print("Maximum reconnection attempts reached. Failed to reconnect.")
            sys.exit(1)

if __name__ == '__main__':
    duration_minutes = 1
    ethernet_speed_mbps = 20000
    log_file_path = 'C:\\Program Files\\Network_Monitor\\logs\\bandwidth_utilisation.log'
    max_log_size_mb = 200

    utilization = BandwidthUtilization('127.0.0.1', 'tempdb', 'nuvama', 'nuvama@123')

    utilization.truncate_log_file(log_file_path, max_log_size_mb)

    log_file = open(log_file_path, 'a')

    try:
        utilization.connect()
        utilization.create_table()

        while True:
            bytes_sent_start = psutil.net_io_counters().bytes_sent
            bytes_received_start = psutil.net_io_counters().bytes_recv
            time.sleep(duration_minutes * 60)
            capture_time = datetime.datetime.now()
            bytes_sent_end = psutil.net_io_counters().bytes_sent
            bytes_received_end = psutil.net_io_counters().bytes_recv
            bytes_sent_total = bytes_sent_end - bytes_sent_start
            bytes_received_total = bytes_received_end - bytes_received_start
            bits_sent_total = bytes_sent_total * 8
            bits_received_total = bytes_received_total * 8
            duration_seconds = duration_minutes * 60
            ethernet_speed_bps = ethernet_speed_mbps * 1000000
            bandwidth_utilization = ((bits_sent_total + bits_received_total) / (duration_seconds * ethernet_speed_bps)) * 100
            total_bytes = bytes_sent_total + bytes_received_total
            utilization.save_data(capture_time, bandwidth_utilization, total_bytes)
            utilization.check_flush_database()
            total_bytes_gb = total_bytes / (1024 ** 3)
            total_bytes_mb = total_bytes / (1024 ** 2)
            output = f"Capture Time: {capture_time}, Bandwidth Utilization: {bandwidth_utilization:.2f}%, Total Bytes: {total_bytes_gb:.2f} GB / {total_bytes_mb:.2f} MB"
            print(output)
            log_file.write(output + '\n')
            log_file.flush()

    except Exception as e:
        error_message = "An error occurred: " + str(e)
        print(error_message)
        log_file.write(error_message + '\n')
        log_file.flush()

    finally:
        utilization.close_connection()
        log_file.close()
