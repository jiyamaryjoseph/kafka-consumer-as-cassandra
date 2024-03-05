from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.auth import PlainTextAuthProvider
from src.entity.generic import instance_to_dict
import json
import os
import uuid

class CassandraOperation:
    def __init__(self):
        cloud_config = {
            'secure_connect_bundle': os.getenv('CASSENDRA_CONNECT_BUNDLE')
        }
        # print(cloud_config)
        # print(os.getenv("CASSENDRA_TOKEN_FILE"))
        with open(os.getenv("CASSENDRA_TOKEN_FILE")) as f:
            secrets = json.load(f)

        CLIENT_ID = secrets["clientId"]
        CLIENT_SECRET = secrets["secret"]

        auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
        self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = self.cluster.connect()

        self.keyspace_name = "sensor"  # Change this to your keyspace name
        self.tablenames=['sensordata1', 'sensordata2', 'sensordata3']
        batch = BatchStatement()
        batch_id = uuid.uuid4()
        
        
    


    def create_table_from_schema(self, schema_json):
        try:
            
            self.schema = json.loads(schema_json)

            # Dividing columns into three lists
            columns = list(self.schema["properties"].keys())
            num_columns = len(columns)
            num_columns_per_table = num_columns // 3 + (1 if num_columns % 3 != 0 else 0)
            print(num_columns_per_table)

            # Creating tables with dividing columns
            for table_name, start_index in zip(self.tablenames, [0, num_columns_per_table, num_columns_per_table * 2]):
                create_query = f"CREATE TABLE IF NOT EXISTS {self.keyspace_name}.{table_name} ("
                create_query += "id UUID PRIMARY KEY, "  # Primary key column
                for column in columns[start_index:start_index + num_columns_per_table]:
                    create_query += f"{column} text, "  # Hardcoded to 'text'
                    print(column)
                create_query = create_query[:-2]  # Remove the last comma and space
                create_query += ")"
                print(create_query)

                self.session.execute(create_query)
        except Exception as e:
            raise Exception(f"An error occurred: {str(e)}")
            
    
    def divide_and_insert_data(self, data):
        try:
            # print(data)
            # Check if the number of table names matches the number of indices
            if len(self.tablenames) != 3:
                raise ValueError("Number of table names must be 3")

            # Dividing columns into three lists
            columns = list(self.schema["properties"].keys())
            num_columns = len(columns)
            num_columns_per_table = num_columns // 3 + (1 if num_columns % 3 != 0 else 0)

            # Creating tables with dividing columns
            for table_name, start_index in zip(self.tablenames, [0, num_columns_per_table, num_columns_per_table * 2]):
                # Extracting data for this table
                table_data = [{k: v for k, v in list(d.items())[start_index:start_index+num_columns_per_table]} for d in data]

                # Inserting data into the Cassandra table
                self.insert_data(table_name, table_data)
        except Exception as e:
            raise Exception(f"An error occurred: {str(e)}")

            

    def insert_data(self, table_name, table_data):
        try:
            
            # Prepare the INSERT query
            query = f"INSERT INTO {self.keyspace_name}.{table_name} JSON ?"
            prepared_query = self.session.prepare(query)

            # Generate a unique ID for the records
            id_value = uuid.uuid4()

            # Execute the prepared query with each record
            for record in table_data:
                # Set the id field in each record
                record['id'] = str(id_value)

                # Convert the record to JSON string
                json_data = json.dumps(record)
                print('next.............', record)

                # Execute the prepared query with the JSON data
                self.session.execute(prepared_query, [json_data])
        except Exception as e:
            raise Exception(f"An error occurred: {str(e)}")


        
                
        
        
        
    
    