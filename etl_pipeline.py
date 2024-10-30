from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import os
import psycopg2

#Initialize spark session

spark = SparkSession.builder \
    .appName("NugaBankETL") \
    .config("spark.jars", "postgresql-42.7.4.jar") \
    .getOrCreate()

#Extracting the data into a spark frame

nugabank_df = spark.read.csv(r'dataset\rawdata\nuga_bank_transactions.csv', header=True, inferSchema=True)

#check for missing values

for column in nugabank_df.columns:
    print(column, 'Nulls', nugabank_df.filter(nugabank_df[column].isNull()).count())

#Fill up the missing values

nugabank_df_clean = nugabank_df.fillna({
    'Customer_Name': 'Unknown',
    'Customer_Address': 'Unknown',
    'Customer_City': 'Unknown',
    'Customer_State': 'Unknown',
    'Customer_Country': 'Unknown',
    'Company': 'Unknown',
    'Job_Title': 'Unknown',
    'Email': 'Unknown',
    'Phone_Number': 'Unknown',
    'Credit_Card_Number': 0,
    'IBAN': 'Unknown',
    'Currency_Code': 'Unknown',
    'Random_Number': 0,
    'Category': 'Unknown',
    'Group': 'Unknown',
    'Is_Active': 'Unknown',
    'Description': 'Unknown',
    'Gender': 'Unknown',
    'Marital_Status': 'Unknown'
    
})


#check for missing values after cleaning

for column in nugabank_df.columns:
    print(column, 'Nulls', nugabank_df.filter(nugabank_df[column].isNull()).count())


#Drop rows where last updated is nul

nugabank_df_clean = nugabank_df_clean.na.drop(subset=['Last_Updated'])

#check for missing values after cleaning again

for column in nugabank_df.columns:
    print(column, 'Nulls', nugabank_df_clean.filter(nugabank_df_clean[column].isNull()).count())

#Transaction table

transaction = nugabank_df_clean.select('Transaction_Date', 'Amount', 'Transaction_Type')

##Adding the transaction_id column
transaction = transaction.withColumn('Transaction_id', monotonically_increasing_id())

#Re-ordering the columns
transaction = transaction.select('Transaction_id', 'Transaction_Date', 'Amount', 'Transaction_Type')

# Customer table

customer = nugabank_df_clean.select( 'Customer_Name','Customer_Address','Customer_City',\
                                    'Customer_State','Customer_Country','Email','Phone_Number').distinct()

#add the id column
customer = customer.withColumn('Customer_id', monotonically_increasing_id())

#reorder the table
customer = customer.select('Customer_id','Customer_Name','Customer_Address','Customer_City',\
                                    'Customer_State','Customer_Country','Email','Phone_Number' )

#Employee table

Employee = nugabank_df_clean.select('Company','Job_Title','Gender','Marital_Status').distinct()

#add id column
Employee = Employee.withColumn('Employee_id', monotonically_increasing_id())

#re-order the column
Employee = Employee.select('Employee_id','Company','Job_Title','Gender','Marital_Status')

#Building the fact table using join

fact_table = nugabank_df_clean.join(transaction, ['Transaction_Date', 'Amount', 'Transaction_Type'], 'inner')\
                              .join(customer, ['Customer_Name','Customer_Address','Customer_City',\
                                    'Customer_State','Customer_Country','Email','Phone_Number'], 'inner')\
                              .join(Employee, ['Company','Job_Title','Gender','Marital_Status'], 'inner')\
                              .select('Transaction_id', 'Customer_id', 'Employee_id', 'Credit_Card_Number','IBAN',\
                                     'Currency_Code','Random_Number','Category','Group','Is_Active','Last_Updated',\
                                      'Description' )

#Data Loading

def get_db_connection():
    connection = psycopg2.connect(
        host = 'localhost',
        database = 'nuga_bank',
        user = 'postgres',
        password = '#Tolexy5038'
    )
    return connection


#create a function to create tables
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = ''' 
                        DROP TABLE IF EXISTS customer;
                        DROP TABLE IF EXISTS transaction;
                        DROP TABLE IF EXISTS employee;
                        DROP TABLE IF EXISTS fact_table;

                        CREATE TABLE customer(
                            Customer_id BIGINT,
                            Customer_Name VARCHAR(100000),
                            Customer_Address VARCHAR(100000),
                            Customer_City VARCHAR(100000),
                            Customer_State VARCHAR(100000),
                            Customer_Country VARCHAR(100000),
                            Email VARCHAR(100000),
                            Phone_Number VARCHAR(100000)
                        );

                        CREATE TABLE transaction(
                            Transaction_id BIGINT,
                            Transaction_Date DATE,
                            Amount FLOAT,
                            Transaction_Type VARCHAR(100000)
                        );

                        CREATE TABLE employee(
                            Employee_id BIGINT,
                            Company VARCHAR(100000),
                            Job_Title VARCHAR(100000),
                            Gender VARCHAR(100000),
                            Marital_Status VARCHAR(100000)
                        );

                        CREATE TABLE fact_table(
                            Transaction_id BIGINT,
                            Customer_id BIGINT,
                            Employee_id BIGINT,
                            Credit_Card_Number VARCHAR(100000),
                            IBAN VARCHAR(100000),
                            Currency_Code VARCHAR(100000),
                            Random_Number FLOAT,
                            Category VARCHAR(100000),
                            Group_name VARCHAR(100000),
                            Is_Active VARCHAR(100000),
                            Last_Updated DATE,
                            Description VARCHAR(100000)
                        );
    
                         '''
    
    cursor.execute(create_table_query)

    conn.commit()
    cursor.close()
    conn.close()
    
fact_table = fact_table.withColumnRenamed("Group", "Group_Name")

url = "jdbc:postgresql://localhost:5432/nuga_bank"
properties = {
    "user": "postgres",
    "password": "#Tolexy5038",
    "driver" : "org.postgresql.Driver"
}

customer.write.jdbc(url=url, table="customer", mode="append", properties=properties)

transaction.write.jdbc(url=url, table="transaction", mode="append", properties=properties)
Employee.write.jdbc(url=url, table="Employee", mode="append", properties=properties)
fact_table.write.jdbc(url=url, table="fact_table", mode="append", properties=properties)


print("Data has been loaded successfully")