import requests
import json
import pandas as pd
import csv
import psycopg2

url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"100000"}

headers = {
	"x-rapidapi-key": "64114f9223mshf6ee7f915c727eep1a5d9djsnef3a9270cef8",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# print(response.json())
data = response.json()

# save data to json file
filename = 'PropertyRecords.json'
with open(filename, 'w') as file:
    json.dump(data, file, indent=4)

# Read into a DataFrame
propertyrecords_df = pd.read_json('PropertyRecords.json')

# Transformation layer
# 1st step - convert dictionary column to string
propertyrecords_df['features'] = propertyrecords_df['features'].apply(json.dumps)

# 2nd step - replace NaN values with appropriate defaults or remove  row/columns as necessary
propertyrecords_df.fillna({
    'assessorID': 'Unknown',
    'legalDescription': 'Not available',
    'squareFootage': 0,
    'subdivision': 'Not available',
    'yearBuilt': 0,
    'bathrooms': 0,
    'lotSize': 0,
    'propertyType': 'Unknown',
    'lastSalePrice': 0,
    'lastSaleDate': 'Not available',
    'features': 'None',
    'taxAssessment': 'Not available',
    'owner': 'Unknown',
    'propertyTaxes': 'Not available',
    'bedrooms': 0,
    'ownerOccupied': 0,
    'zoning': 'Unknown',
    'addressLine2': 'Not available',
    'formattedAddres': 'Not available',
    'county': 'Not available',
}, inplace = True)

#Create the Fact Table
fact_columns = ['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress', 'squareFootage', 'yearBuilt', 'bathrooms', 'bedrooms', 'lotSize', 'propertyType', 'longitude', 'latitude']
fact_table = propertyrecords_df[fact_columns]

# Create Location Dimension
location_dim = propertyrecords_df[['addressLine1', 'city', 'state', 'zipCode', 'county', 'longitude', 'latitude']].drop_duplicates().reset_index(drop=True)
location_dim.index.name = 'location_id'

# Create Sales dimension
sales_dim = propertyrecords_df[['lastSalePrice', 'lastSaleDate']].drop_duplicates().reset_index(drop=True)
sales_dim.index.name = 'sales_id'

# Create Property Features dimension
features_dim = propertyrecords_df[['features', 'propertyType', 'zoning']].drop_duplicates().reset_index(drop=True)
features_dim.index.name = 'features_id'

# saving the created fact and dimension table to csv file
fact_table.to_csv('property_fact.csv', index = False)
location_dim.to_csv('location_dimension.csv', index = True)
sales_dim.to_csv('sales_dimension.csv', index = True)
features_dim.to_csv('features_dimension.csv', index = True)

# Loading Layer
# develop a function to connect to pgadmin

def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = 'postgres',
        user = 'postgres',
        password = 'Favour@8282'
    )
    return connection

conn = get_db_connection()

# Creating Tables

def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''CREATE SCHEMA IF NOT EXISTS zipoestate;
                            
                            DROP TABLE IF EXISTS zipoestate.fact_table;
                            DROP TABLE IF EXISTS zipoestate.location_dim;
                            DROP TABLE IF EXISTS zipoestate.sales_dim;
                            DROP TABLE IF EXISTS zipoestate.features_dim;
                            
                            CREATE TABLE zipoestate.fact_table(
                                addressLine1 VARCHAR(255),
                                city varchar(100),
                                state VARCHAR(50),
                                zipCode INTEGER,
                                formattedAddress VARCHAR(255),
                                squareFootage FLOAT,
                                yearBuilt FLOAT,
                                bathrooms FLOAT,
                                bedrooms FLOAT,
                                lotSize FLOAT,
                                propertyType VARCHAR(100),
                                longitude FLOAT,
                                latitude FLOAT
                            );
                            
                            CREATE TABLE zipoestate.location_dim(
                                location_id SERIAL PRIMARY KEY,
                                addressLine1 VARCHAR(255),
                                city VARCHAR(255),
                                state VARCHAR(50),
                                zipCode INTEGER,
                                county VARCHAR(100),
                                longitude FLOAT,
                                latitude FLOAT
                            );
                            
                            CREATE TABLE zipoestate.sales_dim(
                                sales_id SERIAL PRIMARY KEY,                                
                                lastSalePrice FLOAT,
                                lastSaleDate DATE
                            );
                            
                            CREATE TABLE zipoestate.features_dim(
                                features_id SERIAL PRIMARY KEY,
                                features TEXT,
                                propertyType VARCHAR(100),
                                zoning VARCHAR(100)
                            );
                        '''
    cursor.execute(create_table_query)
    conn.commit() 
    cursor.close()
    conn.close()

    
create_tables()

# create a function to load the csv data into the database

def load_data_from_csv_to_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r', encoding = 'utf-8') as file:
        reader = csv.reader(file)
        next(reader) # Skip the header row
        for row in reader:
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} VALUES ({placeholders});'
            cursor.execute(query, row)
    conn.commit() 
    cursor.close()
    conn.close()  
            
# fact table
fact_csv_path = r'C:\Users\Acer\Zipco Real Estate\property_fact.csv'
load_data_from_csv_to_table(fact_csv_path, 'zipoestate.fact_table')

# location dimension table
location_csv_path = r'C:\Users\Acer\Zipco Real Estate\location_dimension.csv'
load_data_from_csv_to_table(location_csv_path, 'zipoestate.location_dim')

# features dimension table
features_csv_path = r'C:\Users\Acer\Zipco Real Estate\features_dimension.csv'
load_data_from_csv_to_table(features_csv_path, 'zipoestate.features_dim')

# Code to ignore the Not Available in the sales dimension table

# create a function to load the csv data into the database

def load_data_from_csv_to_sales_table(csv_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # define the columns name in sales_dim table
    sale_dim_columns = ['sales_id', 'lastSalePrice', 'lastSaleDate']
    
    with open(csv_path, 'r', encoding = 'utf-8') as file:
        reader = csv.reader(file)
        next(reader) # Skip the header row
        
        for row in reader:
            # Convert empty strings (or 'Not Available' in the date to None(Null in SQL)
            # row = [None if (cell == '' or cell == 'Not available') and col_name == 'lastSaledate' else cell for cell, col_name in zip(row, sale_dim_columns)]
            row = [None if col_name == 'lastSaleDate' and (cell == '' or cell.lower() == 'not available') else cell for cell, col_name in zip(row, sale_dim_columns)]
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} VALUES ({placeholders});'
            cursor.execute(query, row)
    conn.commit() 
    cursor.close()
    conn.close()  
    


# sales dimension table
sales_csv_path = r'C:\Users\Acer\Zipco Real Estate\sales_dimension.csv'
load_data_from_csv_to_sales_table(sales_csv_path, 'zipoestate.sales_dim')

print('All data has been loaded successfully into the respective schema and tables')


