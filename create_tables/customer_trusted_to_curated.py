import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer data
Accelerometerdata_node1717863281328 = glueContext.create_dynamic_frame.from_catalog(database="stedi-proj-db", table_name="accelerometer_trusted", transformation_ctx="Accelerometerdata_node1717863281328")

# Script generated for node Trusted customer data
Trustedcustomerdata_node1717863278979 = glueContext.create_dynamic_frame.from_catalog(database="stedi-proj-db", table_name="customer_trusted", transformation_ctx="Trustedcustomerdata_node1717863278979")

# Script generated for node SQL Query
SqlQuery3191 = '''
with customer_curated as (
select myCustomerData.* from myCustomerData
inner join myAccData
on myCustomerData.email = myAccData.user
)

select distinct * from customer_curated
'''
SQLQuery_node1717863341802 = sparkSqlQuery(glueContext, query = SqlQuery3191, mapping = {"myAccData":Accelerometerdata_node1717863281328, "myCustomerData":Trustedcustomerdata_node1717863278979}, transformation_ctx = "SQLQuery_node1717863341802")

# Script generated for node Curated customer table
Curatedcustomertable_node1717863761193 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1717863341802, database="stedi-proj-db", table_name="customer_curated", transformation_ctx="Curatedcustomertable_node1717863761193")

job.commit()