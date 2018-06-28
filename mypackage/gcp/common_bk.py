import subprocess
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
sqlcontext = SparkSession.builder.enableHiveSupport().getOrCreate()
sqlcontext.sql("set spark.sql.parquet.writeLegacyFormat true")
projectid = "gvs-cs-cisco"
keyfile = "/hdfs/app/GCS_ANA/google/google-cloud-sdk/GVS-CS-CISCO-4040861fd9bc.json"

__author__ = 'nsandela@cisco.com'

class MyException(Exception):
    pass

class GCP(object):

    def __init__(self, hiveTable=None, bqTableName=None, bucketPath=None, process_name=None):
        if not (hiveTable == None and bqTableName == None and bucketPath == None and process_name == None):
               self.hiveTable = hiveTable
               self.bqTableName = bqTableName
               self.bucketPath = bucketPath
               self.process_name = process_name
	       print "Hey"
        #self.process_list = process_list
        else:
               raise MyException('All provided arguments are None, Please provide valid arguments')

    #bq2hiveTable
    def store_data(self, dataframe):
        df = sqlcontext.createDataFrame(dataframe)
        df.registerTempTable('gcp_temp')
        sqlcontext.sql("drop table if exists" + " " + self.hiveTable)
        sqlcontext.sql("create table" + " " + self.hiveTable + " " + "as select * from gcp_temp")
        print "Successfully Loaded data to" + " " + self.hiveTable

    def bq2hiveTable(self):
        df = pd.read_gbq('SELECT * FROM' + " " + self.bqTableName, project_id=projectid, private_key=keyfile)
        try:
            self.store_data(df)
        except Exception as e:
            print str(e)

    def bq2bucket(self, format="NEWLINE_DELIMITED_JSON"):
        bqStr = 'bq extract --destination_format NEWLINE_DELIMITED_JSON --compression GZIP  \
        {bq_tablename} {gcp_bucketpath}/file-*.json.gz' \
            .format(bq_tablename=bqTableName, gcp_bucketpath=bucketPath)
        print bqStr
        subprocess.check_call(bqStr, shell=True)

    def datatype_cast(self, bqtablesdf, schemadf):
        for column in bqtablesdf.columns:
            v = schemadf.index[schemadf['name'] == column].tolist()
            newtype = schemadf.iloc[v[0]]['type']
            if newtype == 'STRING':
                bqtablesdf[column] = bqtablesdf[column].astype(object)
            elif newtype == 'BYTES':
                bqtablesdf[column] = bqtablesdf[column].astype(object)
        return bqtablesdf

    def get_bqschema(self):
        json_path = '/hdfs/app/GCS_ANA/gvscsde/projects/gcp/gcp_test/{bqTableName}.json'\
            .format(bqTableName=self.bqTableName)
        bqStr = 'bq show --schema --format=prettyjson gvs-cs-cisco:{bqTableName} >> {json_path}'.format(
            bqTableName=self.bqTableName, json_path=json_path)
        print bqStr
        subprocess.check_call(bqStr, shell=True)

    #gcp_hadoop2gcpBucket

    def hadoop2gcpBucket(self, statement='', execute=False, compress=False, partitions=0):
        #print execute
        if execute:
            sqlStr = statement
            print "Running query {statement}".format(statement=sqlStr)
        else:
            print "Retrieving data from hiveTable: " + " " + self.hiveTable
            sqlStr = "select * from {}".format(self.hiveTable)
        try:
            df = sqlcontext.sql(sqlStr)
            df = self.datatype_cast_decimal(df)
            print "Created dataframe with records: "+str(df.count())
            if compress:
                df = df.coalesce(partitions)
            try:
                print "Writing hiveTable data to " + " " + self.bucketPath + " " + "in gcp"
                df.write.mode('overwrite').parquet(self.bucketPath)
            except Exception as e:
                print str(e)
                raise
        except Exception as e:
            raise

    def gcpBucket2bq(self, tableFormat="PARQUET"):
        print
        "Loading data from " + self.bucketPath + "to" + self.bqTableName + " " + "in gcp"
        bqStr = 'bq load  --replace=true --source_format={table_format}  {bq_tablename} {gcp_bucketpath}' \
            .format(table_format=tableFormat, bq_tablename=self.bqTableName, gcp_bucketpath=self.bucketPath + "/*.parquet")
        print(bqStr)
        subprocess.check_call(bqStr, shell=True)

    def cleanBucket(self):
        rmStr = 'gsutil -m rm -r {}'.format(self.bucketPath + "/*.parquet")
        subprocess.check_call(rmStr, shell=True)



    def apply_function(self, df, fields, function):
        for field in fields:
            df = df.withColumn(field, function(field))
        return df

    def decimal_to_bigint(self, column):
        return F.col(column).cast(LongType()).alias(column)

    def datatype_cast_decimal(self, dataframe):
        df_list = dict(dataframe.dtypes)
        #print df_list
        lst = []
        for column in dataframe.columns:
            if 'decimal(' in df_list.get(column):
                lst.append(column)
        print "Changing the datatype for following columns: "+ str(lst)
        df = self.apply_function(dataframe, lst, self.decimal_to_bigint)
        return df

    def process(self, **rules):
        if self.process_name == 'bq2hiveTable':
            try:
                self.bq2bucket()
                print "Successfully Loaded data from bqTable to gcpbucket"
                try:
                    df = sqlcontext.read.json(self.bucketPath + "/file-*.json.gz")
                    pdsdf = df.toPandas()
                    #delete_json_file = 'rm -f' + " " + json_path
                    subprocess.check_call(self.delete_json_file, shell=True)
                    self.get_bqschema()
                    df = pd.read_json(json_path)
                    print "Successfully read the json data to dataframe"
                    self.datatype_cast(pdsdf, df)
                    print "Successfully Casted the datatypes"
                    self.store_data(pdsdf)
                    print "Successfully loaded the dataframe to hiveTable"
                except Exception as e:
                    print str(e)
            except Exception as e:
                print str(e)
        elif self.process_name == 'hadoop2gcpBucket':
            try:
                #print ("Started the process")
                if 'sqlText' in rules.keys():
                    #global statement
                    statement = rules['sqlText']
                    self.hadoop2gcpBucket(statement=statement, execute=True)
                    print "Successfully wrote data to " + " " + self.bucketPath
                else:
                    print "Running without any statement"
                    self.hadoop2gcpBucket()
                    print "Successfully wrote data to " + " " + self.bucketPath
                try:
                    self.gcpBucket2bq()
                    print "Successfully created bqtable from " + " " + self.bucketPath
                    try:
                        self.cleanBucket()
                        print "Successfully Deleted data from" + self.bucketPath + " " + "in gcp"
                    except Exception as e:
                        print str(e)
                        print "Errored Out while running the cleanBucket Function"
                except Exception as e:
                    print str(e)
                    print "Errored Out while running the gcpBucket2bq Function"
            except Exception as e:
                print str(e.__str__())
                print "Errored Out while running the hadoop2gcpBucket Function"
                raise
        else:
            print "Operation not permitted please refer to properties file for valid operations"

