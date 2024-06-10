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

# Script generated for node Accelerometer trusted data
Accelerometertrusteddata_node1717864303955 = glueContext.create_dynamic_frame.from_catalog(database="stedi-proj-db", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusteddata_node1717864303955")

# Script generated for node Step trainer trusted data
Steptrainertrusteddata_node1717864301500 = glueContext.create_dynamic_frame.from_catalog(database="stedi-proj-db", table_name="step_trainer_trusted", transformation_ctx="Steptrainertrusteddata_node1717864301500")

# Script generated for node Inner join
SqlQuery3695 = '''
with aggregated_table as
(select 
    myStepTrainerData.*,
    myAccData.user,
    myAccData.x,
    myAccData.y,
    myAccData.z
from myAccData
inner join myStepTrainerData
on myAccData.timestamp=myStepTrainerData.sensorreadingtime
)

select distinct * from aggregated_table
'''
Innerjoin_node1717864348845 = sparkSqlQuery(glueContext, query = SqlQuery3695, mapping = {"myAccData":Accelerometertrusteddata_node1717864303955, "myStepTrainerData":Steptrainertrusteddata_node1717864301500}, transformation_ctx = "Innerjoin_node1717864348845")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1717866657116 = glueContext.write_dynamic_frame.from_catalog(frame=Innerjoin_node1717864348845, database="stedi-proj-db", table_name="machine_learning_curated", transformation_ctx="AWSGlueDataCatalog_node1717866657116")

job.commit()