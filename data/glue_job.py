import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node dim airports
dimairports_node1714661864166 = glueContext.create_dynamic_frame.from_catalog(database="airlines", table_name="dev_airlines_dim_airports", transformation_ctx="dimairports_node1714661864166", redshift_tmp_dir="s3://s3-temp-misc/dim_airports/")

# Script generated for node s3 raw daily flights 
s3rawdailyflights_node1714661821695 = glueContext.create_dynamic_frame.from_catalog(database="airlines", table_name="raw_daily_flights", transformation_ctx="s3rawdailyflights_node1714661821695")

# Script generated for node depdelay at least 1 hr
depdelayatleast1hr_node1714662438852 = Filter.apply(frame=s3rawdailyflights_node1714661821695, f=lambda row: (row["depdelay"] >= 0), transformation_ctx="depdelayatleast1hr_node1714662438852")

# Script generated for node get dep airport
depdelayatleast1hr_node1714662438852DF = depdelayatleast1hr_node1714662438852.toDF()
dimairports_node1714661864166DF = dimairports_node1714661864166.toDF()
getdepairport_node1714662520529 = DynamicFrame.fromDF(depdelayatleast1hr_node1714662438852DF.join(dimairports_node1714661864166DF, (depdelayatleast1hr_node1714662438852DF['originairportid'] == dimairports_node1714661864166DF['airport_id']), "left"), glueContext, "getdepairport_node1714662520529")

# Script generated for node modify dep airport columns
modifydepairportcolumns_node1714662692602 = ApplyMapping.apply(frame=getdepairport_node1714662520529, mappings=[("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint"), ("destairportid", "long", "destairportid", "long"), ("carrier", "string", "carrier", "string"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="modifydepairportcolumns_node1714662692602")

# Script generated for node get arr airport
modifydepairportcolumns_node1714662692602DF = modifydepairportcolumns_node1714662692602.toDF()
dimairports_node1714661864166DF = dimairports_node1714661864166.toDF()
getarrairport_node1714663540681 = DynamicFrame.fromDF(modifydepairportcolumns_node1714662692602DF.join(dimairports_node1714661864166DF, (modifydepairportcolumns_node1714662692602DF['destairportid'] == dimairports_node1714661864166DF['airport_id']), "left"), glueContext, "getarrairport_node1714663540681")

# Script generated for node modify arr airport columns
modifyarrairportcolumns_node1714663620756 = ApplyMapping.apply(frame=getarrairport_node1714663540681, mappings=[("dep_delay", "bigint", "dep_delay", "long"), ("arr_delay", "bigint", "arr_delay", "long"), ("destairportid", "long", "destairportid", "long"), ("carrier", "string", "carrier", "string"), ("dep_city", "string", "dep_city", "string"), ("dep_airport", "string", "dep_airport", "string"), ("dep_state", "string", "dep_state", "string"), ("airport_id", "long", "airport_id", "long"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("state", "string", "arr_state", "string")], transformation_ctx="modifyarrairportcolumns_node1714663620756")

# Script generated for node redshift fact table
redshiftfacttable_node1714663736032 = glueContext.write_dynamic_frame.from_catalog(frame=modifyarrairportcolumns_node1714663620756, database="airlines", table_name="dev_airlines_fact_daily_flights", redshift_tmp_dir="s3://s3-temp-misc/fact_flights/",additional_options={"aws_iam_role": "arn:aws:iam::654654491149:role/redshift-role"}, transformation_ctx="redshiftfacttable_node1714663736032")

job.commit()