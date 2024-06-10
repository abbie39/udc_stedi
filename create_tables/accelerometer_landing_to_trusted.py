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

# Script generated for node Customer trusted data
Customertrusteddata_node1717869789426 = glueContext.create_dynamic_frame.from_catalog(database="stedi-proj-db", table_name="customer_trusted", transformation_ctx="Customertrusteddata_node1717869789426")

# Script generated for node Accelerometer data
Accelerometerdata_node1717869748036 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-proj-bucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometerdata_node1717869748036")

# Script generated for node Inner join for research participants
SqlQuery3614 = '''
select myAccData.* from myAccData 
inner join myTrustedSource
on myAccData.user = myTrustedSource.email
'''
Innerjoinforresearchparticipants_node1717869807468 = sparkSqlQuery(glueContext, query = SqlQuery3614, mapping = {"myTrustedSource":Customertrusteddata_node1717869789426, "myAccData":Accelerometerdata_node1717869748036}, transformation_ctx = "Innerjoinforresearchparticipants_node1717869807468")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1717869856679 = glueContext.write_dynamic_frame.from_catalog(frame=Innerjoinforresearchparticipants_node1717869807468, database="stedi-proj-db", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1717869856679")

job.commit()