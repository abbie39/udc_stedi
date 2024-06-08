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

# Script generated for node Customer data
Customerdata_node1717869298016 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-proj-bucket/customer/landing/"], "recurse": True}, transformation_ctx="Customerdata_node1717869298016")

# Script generated for node Filter for research participants
SqlQuery3534 = '''
select * from myDataSource where sharewithresearchasofdate > 0

'''
Filterforresearchparticipants_node1717869368655 = sparkSqlQuery(glueContext, query = SqlQuery3534, mapping = {"myDataSource":Customerdata_node1717869298016}, transformation_ctx = "Filterforresearchparticipants_node1717869368655")

# Script generated for node Output to customer_trusted table
Outputtocustomer_trustedtable_node1717869397534 = glueContext.write_dynamic_frame.from_catalog(frame=Filterforresearchparticipants_node1717869368655, database="stedi-proj-db", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="Outputtocustomer_trustedtable_node1717869397534")

job.commit()