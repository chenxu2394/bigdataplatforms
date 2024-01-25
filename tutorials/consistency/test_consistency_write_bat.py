# CS-E4640
# Simple example for studying big data platforms
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
import argparse
import csv
from time import time

# Argument parser
parser = argparse.ArgumentParser()
parser.add_argument('--hosts', help='cassandra host "host1,host2,host3"')
parser.add_argument('--u', help='user name')
parser.add_argument('--p', help='password')
args = parser.parse_args()

# Split hosts and setup authentication
hosts = args.hosts.split(',')
auth_provider = PlainTextAuthProvider(username=args.u, password=args.p)

# Function to insert data into Cassandra
def insert_data(session, row, consistency_level):
    query = """
    INSERT INTO cs4640cx.bird1234 (country, duration_seconds, english_cname, id, species, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    statement = SimpleStatement(query, consistency_level=consistency_level)
    session.execute(statement, (row['country'], int(row['duration_seconds']), row['english_cname'], 
                                int(row['id']), row['species'], float(row['latitude']), float(row['longitude'])))


if __name__ == "__main__":
    # Create cluster and session
    cluster = Cluster(hosts, port=9042, auth_provider=auth_provider)
    session = cluster.connect()

    # Choose consistency level (uncomment the desired one)
    consistency_level = ConsistencyLevel.ONE
    # consistency_level = ConsistencyLevel.QUORUM
    # consistency_level = ConsistencyLevel.ALL

    # Read CSV file and insert data
    with open('./sampledata.csv', mode='r') as file:
        reader = csv.DictReader(file)
        start = time()
        for row in reader:
            print(row)
            insert_data(session, row, consistency_level)
        stop = time()
    
    print("Data insertion completed.")
    print("Total time:", stop - start, "seconds")

    # Close the session and cluster connection
    session.shutdown()
    cluster.shutdown()
