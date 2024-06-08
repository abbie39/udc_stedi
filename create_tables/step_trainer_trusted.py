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

# Script generated for node Curated customer data
Curatedcustomerdata_node1717861025823 = glueContext.create_dynamic_frame.from_catalog(database="stedi-proj-db", table_name="customer_curated", transformation_ctx="Curatedcustomerdata_node1717861025823")

# Script generated for node Step trainer data
Steptrainerdata_node1717868822561 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-proj-bucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="Steptrainerdata_node1717868822561")

# Script generated for node Inner join for curated customers
SqlQuery3729 = '''
with trusted_records as (
    select myStepTrainerData.* from myStepTrainerData
    inner join myCustomerData
    on myStepTrainerData.serialnumber=myCustomerData.serialnumber
)

select distinct * from trusted_records


'''
Innerjoinforcuratedcustomers_node1717861064456 = sparkSqlQuery(glueContext, query = SqlQuery3729, mapping = {"myCustomerData":Curatedcustomerdata_node1717861025823, "myStepTrainerData":Steptrainerdata_node1717868822561}, transformation_ctx = "Innerjoinforcuratedcustomers_node1717861064456")

# Script generated for node Step trainer trusted
Steptrainertrusted_node1717862037659 = glueContext.write_dynamic_frame.from_catalog(frame=Innerjoinforcuratedcustomers_node1717861064456, database="stedi-proj-db", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="Steptrainertrusted_node1717862037659")

job.commit()