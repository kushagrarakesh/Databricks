How Time Travel is Supported:

Versioning:

Each change to the Delta table is recorded with a unique version ID or timestamp in the transaction log.
You can query data from a specific version or timestamp using SQL or Python, allowing you to view and recreate historical datasets.
Example:

sql
Copy code
-- Querying a previous version of a table
SELECT * FROM my_table VERSION AS OF 3;

-- Querying data at a specific timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-01 00:00:00';
Use Cases:

Data Auditing: Retrieve data as it existed at a specific point in time.
Debugging: Identify and fix issues by analyzing historical data changes.
Reproducibility: Reproduce results from past datasets for compliance or analysis.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Define the schema
schema = StructType([
    StructField("column1", StringType(), True),
    StructField("column2", IntegerType(), True)
])
# Read the CSV file with the specified schema
flight_data_2015 = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("path/to/csv")

# Show the data (optional)
flight_data_2015.show()

#Explanation:
Schema Definition:

The StructType in PySpark corresponds to the StructType in Scala.
StructField is used to define individual fields in the schema.
Reading the CSV:

.option("header", "true") specifies that the first row in the CSV file contains the header.
.schema(schema) applies the predefined schema to the data.
.csv("path/to/csv") reads the data from the given path.
Show Data:

.show() displays the data for verification, similar to how you might use .show() in Scala.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



#You are tasked to write a PySpark job in Azure Databricks to process incremental data updates from a Delta Lake table (bronze_table) and load it into a gold_table in another Delta Lake.
#The requirements are:
#Only process data where the updated_at column is greater than the last processed timestamp (stored in a variable last_processed_timestamp).
#Deduplicate the data based on a unique identifier column (id).
#Append the cleaned data to the gold_table.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# Initialize Spark session
spark = SparkSession.builder.appName("Delta Incremental Processing").getOrCreate()
# Last processed timestamp
last_processed_timestamp = "2024-12-01T00:00:00Z"
# Load data from the Bronze Delta Lake table
bronze_df = spark.read.format("delta").load("/mnt/delta/bronze_table")
# Filter the data for new updates
incremental_df = bronze_df.filter(col("updated_at") > last_processed_timestamp)
# Remove duplicates based on the 'id' column
deduplicated_df = incremental_df.dropDuplicates(["id"])
# Write the data to the Gold Delta Lake table
deduplicated_df.write.format("delta").mode("append").save("/mnt/delta/gold_table")

#~~~~~~~~~~~~~~~~~family  details.json~~~~~~~~~~~~~~~~~~~~~~~~~~~`#
[
  {
    "parent_id": 1,
    "parent_name": "ParentA",
    "children": [
      {"child_id": 101, "child_name": "ChildA1"},
      {"child_id": 102, "child_name": "ChildA2"}
    ]
  },
  {
    "parent_id": 2,
    "parent_name": "ParentB",
    "children": [
      {"child_id": 201, "child_name": "ChildB1"},
      {"child_id": 202, "child_name": "ChildB2"}
    ]
  }
]

Ans:-
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Flatten JSON for Azure SQL") \
    .getOrCreate()

# Load the source data (replace '/path/to/source.json' with your actual path)
source_data = spark.read.json("/FileStore/rakdataset/parent.json",multiLine="true")

# Flatten the JSON structure
flattened_df = source_data \
    .withColumn("child", explode(col("children"))) \
    .select(
        col("parent_id"),
        col("parent_name"),
        col("child.child_id"),
        col("child.child_name")
    )

# Display the transformed DataFrame
flattened_df.show()
# Write the transformed DataFrame to Azure SQL Database
# Define Azure SQL Database connection properties
jdbc_url = "jdbc:sqlserver://sqldbserver01.database.windows.net:1433;database=hierarchy"
db_properties = {
    "user": "admina",
    "password": "XXXXXXXXXX",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Write the DataFrame to Azure SQL Database table
flattened_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "parentschild") \
    .options(**db_properties) \
    .mode("overwrite") \
    .save()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Example: Using .saveAsTable() in Delta Lake~~~~~~~~~~~~~~~~~~~
from pyspark.sql import SparkSession
# Initialize Spark session with Delta Lake configuration
spark = SparkSession.builder \
    .appName("Delta Lake saveAsTable Example") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Sample DataFrame
data = [
    (1, "Alice", 3000),
    (2, "Bob", 4000),
    (3, "Charlie", 5000)
]
columns = ["ID", "Name", "Salary"]
df = spark.createDataFrame(data, columns)

# Write the DataFrame as a managed Delta table
df.write.format("delta").mode("overwrite").saveAsTable("employee_salaries")

# Query the table using SQL
spark.sql("SELECT * FROM employee_salaries").show()

# Drop the table after use (optional)
spark.sql("DROP TABLE employee_salaries")

~~~~~~~~~~~~~~#You are tasked to load a CSV file into a Delta Lake table in Apache Spark. Below is a snippet of code. Identify the missing part (denoted as ???) to ensure the data is properly loaded and written in Delta Lake format.~~~~~~~~~~~~

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Delta Lake Example") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Load CSV data into a DataFrame
df = spark.read.format("csv").option("header", "true").load("/path/to/data.csv")

# Write the data into a Delta Lake table
df.write.format("delta").mode("overwrite").save("/path/to/delta_table")

~~~~~~~~~~~~~~~~~~~~~~~
Here's a complete example that demonstrates schema evolution in Delta Lake using a CSV file:

Scenario:
You have an initial CSV file (data_v1.csv) with a simple schema.
A new version of the data (data_v2.csv) has an additional column.
You will demonstrate schema evolution by allowing Delta Lake to merge the new schema when appending the data.
Step-by-Step Example:
1. Prepare Initial CSV File (data_v1.csv):
csv
------

id,name
1,John
2,Jane
3,Smith

2. Prepare Updated CSV File with a New Column (data_v2.csv):
id,name,age
4,Alice,30
5,Bob,25
3. Code Implementation in Azure Databricks:
from pyspark.sql import SparkSession
# Start a Spark session
spark = SparkSession.builder \
    .appName("DeltaSchemaEvolutionExample") \
    .getOrCreate()

# Path to the Delta table
delta_table_path = "/mnt/delta/schema_evolution_example"

# Step 1: Load and write the initial CSV to Delta table
data_v1 = spark.read.csv("/path/to/data_v1.csv", header=True, inferSchema=True)
data_v1.write.format("delta").save(delta_table_path)

print("Initial data loaded into Delta table:")
data_v1.show()

# Step 2: Load the updated CSV (with new schema) and merge it with the Delta table
data_v2 = spark.read.csv("/path/to/data_v2.csv", header=True, inferSchema=True)

# Enabling schema evolution during the write
data_v2.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(delta_table_path)

print("Updated data merged into Delta table:")
data_v2.show()

# Step 3: Read the Delta table to confirm schema evolution
delta_table = spark.read.format("delta").load(delta_table_path)
print("Delta table after schema evolution:")
delta_table.show()
Explanation of Key Code Elements:
mergeSchema: "true": This option allows Delta Lake to merge the new schema into the existing table without breaking.
Initial Write:
The initial data (data_v1.csv) is loaded into the Delta table with the base schema (id, name).
Schema Evolution:
The updated data (data_v2.csv) contains an additional column age.
Delta Lake automatically evolves the schema when mergeSchema is enabled.
Delta Table Validation:
The final read confirms that the Delta table has the new schema (id, name, age) and contains all rows from both files.
Output:
Initial Delta Table (after Step 1):

diff
Copy code
+---+----+
| id|name|
+---+----+
|  1|John|
|  2|Jane|
|  3|Smith|
+---+----+
Delta Table After Schema Evolution (after Step 2):

sql
Copy code
+---+----+----+
| id|name| age|
+---+----+----+
|  1|John|null|
|  2|Jane|null|
|  3|Smith|null|
|  4|Alice|  30|
|  5|Bob  |  25|
+---+----+----+
Key Notes:
Ensure that Delta Lake is enabled in your Databricks environment.
Replace /mnt/delta/schema_evolution_example with your storage location.
Update the paths /path/to/data_v1.csv and /path/to/data_v2.csv with actual file paths in your environment.
