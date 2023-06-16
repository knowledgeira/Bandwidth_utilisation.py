import pyodbc
import datetime
import time

class BandwidthUtilization:
    def __init__(self, server, database, username, password):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.max_reconnection_attempts = 3
        self.reconnection_delay = 5  # seconds

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

    def save_data(self, capture_time, utilization, total_bytes):
        insert_query = '''
        INSERT INTO bandwidth_utilization (capture_time, utilization, total_bytes)
        VALUES (?, ROUND(?, 2), ?)
        '''
        self.cursor.execute(insert_query, capture_time, utilization, total_bytes)
        self.connection.commit()
        print(f"Data saved: Capture Time: {capture_time}, Bandwidth Utilization: {utilization:.2f}%, Total Bytes: {total_bytes / (1024 ** 3):.2f} GB / {total_bytes / (1024 ** 2):.2f} MB")

    def reconnect(self):
        self.close_connection()
        if self.reconnection_count < self.max_reconnection_attempts:
            self.reconnection_count += 1
            print(f"Reconnecting... (Attempt {self.reconnection_count})")
            time.sleep(self.reconnection_delay)
            self.connect()
        else:
            print("Maximum reconnection attempts reached. Failed to reconnect.")

    def close_connection(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()

if __name__ == '__main__':
    utilization = BandwidthUtilization('EDELNMNONTRDB01', 'tempdb', 'nuvama', 'nuvama@123')
    utilization.connect()

    try:
        while True:
            capture_time = datetime.datetime.now()
            utilization_value = 0.00
            total_bytes = 0.85 * (1024 ** 2)  # 0.85 MB to bytes

            try:
                utilization.save_data(capture_time, utilization_value, total_bytes)
            except pyodbc.Error:
                print("Error occurred while saving data. Reconnecting...")
                utilization.reconnect()

            time.sleep(60)

    except Exception as e:
        error_message = "An error occurred: " + str(e)
        print(error_message)

    finally:
        utilization.close_connection()
