# Databricks notebook source

###########################################################################################
# Program Name : KPI 
# Author       : Development Team
# Purpose      : 
############################################################################################



################ Import Modules and Library ################
from pyspark.sql import SparkSession
import datetime
import uuid
###########################################################


class DataFeed():    
    """ Preare and write data into Data lake/Delta tables basis """
    
    # pylint: disable=too-many-instance-attributes,too-many-arguments
    def __init__(self, param_group,param_source,param_kpi, param_region,
                 library_config_path):
        ################ Initializing Variables ################
        self.spark = SparkSession.builder.getOrCreate()
        self.param_group = param_group
        self.param_kpi = param_kpi
        self.param_region = param_region
        self.library_config_path = library_config_path        
        self.flag_logtbl=1
        self.param_source = param_source
        self.exec_process_time = datetime.datetime.now()
        self.exec_start_time = datetime.datetime.now()
        
      ############## Initializing Dataframes ##############
        
        self.kpilist_kpi = None
        self.kpilist_org = None
        self.kpilist = None
        self.kpilist_preprocessing = None
        self.kpilist_srcfiles = None
        self.kpilist_kpi = None

        ############## Truncating Stg Log Table ##############      
        
    def proc_capture_log(self, kpi_step, func_nm, status, err_desc):
      
      """ Function to Capture Logging: error,success and validation """
      
      self.spark.sql("""Insert into analytics.h_kpi_log select '{0}','{1}' Group,'{2}' SourceName,'{3}' Region,'{4}' KPI_Step,
      '{5}' Process_Step,'{6}' Processing_Date,'{7}' Start_Time, current_Timestamp() End_Time, '{8}' Status,
      "'{9}'" ErrorMessage""".format(
        uuid.uuid4(),self.param_group.replace("'",""),self.param_source.replace("'",""),self.param_region.replace("'",""),
        kpi_step,func_nm,self.exec_process_time,self.exec_start_time,status,err_desc))                                                                                 
  
    def create_persitent_tables(self, stg_preprocessing_qry, fname,col_partitionby,path_datalake,schema_loadtype):
        
        """ Function to Create Delta Tables and Data Lake files """
       
        calculate_kpi = self.spark.sql(stg_preprocessing_qry)
        calculate_kpi.write.format("delta").mode(schema_loadtype).option(
            "overwriteSchema", "true").partitionBy(col_partitionby).save(path_datalake + fname)
        
        self.proc_capture_log(fname, 'create_persitent_tables',
                              'success', 'File is Created in Data Lake')
        
        self.exec_start_time = datetime.datetime.now()
        self.spark.sql("DROP TABLE IF EXISTS analytics." + fname)
        self.spark.sql("CREATE TABLE analytics." + fname +
                  " USING DELTA LOCATION '" + path_datalake + fname + "'")
        
        ############## Record successfull message ############## col_partitionby,path_datalake,schema_loadtype
          
        self.proc_capture_log(fname, 'create_persitent_tables',
                              'success', 'Delta Table is Created')

    def read_kpi_config(self):

        """ Read Library/Mapping CSV file from BLOB """
        
        self.exec_start_time = datetime.datetime.now()
        try:
            self.kpilist_org = self.spark.read.csv(self.library_config_path,
                                              header="true", escape="\"", multiLine="true",
                                              inferSchema="true")
            
            if self.kpilist_org.rdd.isEmpty():
               self.proc_capture_log(
                      'kpilist_org', 'read_kpi_config', 'validation', 'Empty_DF')

            self.kpilist_org.createOrReplaceTempView("kpilist_org")
            
            
            ############## Record successfull message ##############

            self.proc_capture_log(
                'Read_CSV', 'read_kpi_config', 'success', 'NA')
        except Exception as err:
            #raise Exception(e)
            err_desc = str(err).replace("'", "").replace('"', '').replace("\n", " ")

            ############## Inserting into log in table #####################

            self.proc_capture_log(
                'Read_CSV', 'read_kpi_config', 'error', err_desc)

    def get_kpilist(self):
      
        """ Filter mapping on the basis of parameters. And create TempViews for selected rows """
        
        self.exec_start_time = datetime.datetime.now()
        try:
            grp_in_cls = self.param_group.replace(",", "','")
            kpi_in_cls = self.param_kpi.replace(",", "','")
            rgn_in_cls = self.param_region.replace(",", "','")
            
            sqlstrkpilist = "select * from kpilist_org where 1=1"
            if self.param_group != 'ALL' and self.param_group !='':
                sqlstrkpilist = sqlstrkpilist + \
                    " and Group In ('" + grp_in_cls + "')"
            if self.param_kpi != 'ALL' and self.param_kpi !='':
                sqlstrkpilist = sqlstrkpilist + \
                    " and KPI In('" + kpi_in_cls + "')"
            if self.param_region != 'ALL' and self.param_region !='':
               sqlstrkpilist = sqlstrkpilist + \
                   " and Region In('" + rgn_in_cls + "')"
            
            self.kpilist = self.spark.sql(sqlstrkpilist)
            if self.kpilist.rdd.isEmpty():
                self.proc_capture_log(
                            'kpilist', 'get_kpilist', 'validation', 'Empty_DF')
            self.kpilist.createOrReplaceGlobalTempView("kpilist")
              
            ############## Record successfull message ##############

            self.proc_capture_log(
                'apply_parameter', 'get_kpilist', 'success', 'NA')
        except Exception as err_dsc:
            #raise Exception(e)
            err_desc = str(err_dsc).replace("'", "").replace('"', '').replace("\n", " ")

            ############## Inserting into Stg Log table ##############

            self.proc_capture_log('apply_parameter', 'get_kpilist', 'error', "'"+err_desc + "'")
    
    def create_source_temp_tables(self):
        """ Load source data files and create tempview """
       
        self.kpilist_srcfiles = self.spark.sql(
            """select distinct SourceFileName,MountPath, SourceFileName_PATH
               from global_temp.kpilist where SourceFileName!='NA' or
             SourceFileName is not null""")
        if self.kpilist_srcfiles.rdd.isEmpty():
          self.proc_capture_log(
                      'kpilist_srcfiles', 'create_SourceData_temp_tables', 'validation', 'Empty_DF')
        for src_file_nm, mount_path, src_file_nm_path in zip(
            self.kpilist_srcfiles.select("SourceFileName").collect(),
            self.kpilist_srcfiles.select("MountPath").collect(),
            self.kpilist_srcfiles.select("SourceFileName_PATH").collect()):
          
            
            ############## Validate Null/Blank Values ##################
          
            self.exec_start_time = datetime.datetime.now()
            if src_file_nm[0] and mount_path[0] and src_file_nm_path[0]:
                tmp_str_name = str(src_file_nm[0])
                tmp_str_name = tmp_str_name.replace("\n", "")
                src_file_list = tmp_str_name.split(',')
                for each_file in zip(src_file_list):
                    self.exec_start_time = datetime.datetime.now()
                    try:                       
                        str_file = str(each_file[0]).replace('"', '')
                        src_file_path = mount_path[0] + \
                            src_file_nm_path[0]+str_file
                        
                        temp_df = self.spark.read.load(
                            src_file_path, format="delta", inferSchema="true", header="true")
                        temp_df.createOrReplaceGlobalTempView(str_file)

                        ########## Record Successfull Message ##############
                        self.proc_capture_log(
                            str_file, 'create_source_temp_tables', 'success', 'NA')
                    except Exception as err:
                        
                        err_desc = str(err).replace("'", "").replace('"', '').replace("\n", " ")

                        ########### Inserting into Stg Log Table #################

                        self.proc_capture_log(str_file,'create_SourceData_tables','error', err_desc)
            else:

                ############ Record Validation Fail ##################

                self.proc_capture_log(
                    'sourcetable', 'create_SourceData_temp_tables', 'validation', 'Fail')

    def create_stg_temp_tables(self):
        
        """ Create staging Temp Tables """

        self.kpilist_srcfiles = self.spark.sql(
            "select distinct TargetKPITableName,ScriptPath,STGProcessorder from global_temp.kpilist where Temp_Flag=1 order by STGProcessorder")
        
        for tgt_tbl_nm, script_path in zip(
            self.kpilist_srcfiles.select("TargetKPITableName").collect(),
            self.kpilist_srcfiles.select("ScriptPath").collect()):
            self.exec_start_time = datetime.datetime.now()
            try:
                ############## Validate Null/Blank Values ##############
                if tgt_tbl_nm[0] and script_path[0]:
                    fname = tgt_tbl_nm[0]
                    sql_script_path = script_path[0]
                    
                    sql_txt = self.spark.sparkContext.wholeTextFiles('dbfs:'+sql_script_path)
                    stg_preprocessing_qry = sql_txt.collect()[0][1]
                    
                    tmp_stg_pre_df = self.spark.sql(stg_preprocessing_qry)
                    tmp_stg_pre_df.createOrReplaceGlobalTempView(fname)

                    ############## Record Successfull Message ##############

                    self.proc_capture_log(
                        fname, 'create_stg_temp_tables', 'success', 'NA')
                else:
                    
                    fname = tgt_tbl_nm[0]
                    self.proc_capture_log(
                        fname, 'create_stg_temp_tables', 'validation', 'Fail')
            except Exception as err:
                #raise Exception(e)
                err_desc = str(err).replace("'", "").replace(
                    '"', '').replace("\n", " ")

                ################ Inserting into Stg Log table ################

                self.proc_capture_log(
                    fname, 'create_stg_temp_tables', 'error', err_desc)

    def calculate_kpi(self):
        
        """ Calcaulate KPIs:Persistent delta tables in databrick and delta files in datalake """
        
        self.kpilist_kpi = self.spark.sql(
            """select distinct TargetKPITableName,ScriptPath,Country,Market,
            Region,Partitionby,DataLakePath,LoadType from global_temp.kpilist where Temp_Flag=0 """)
        
        if self.kpilist_kpi.rdd.isEmpty():
          self.proc_capture_log(
                      'kpilist_kpi', 'calculate_kpi', 'validation', 'Empty_DF')
        self.kpilist_kpi.show()
        for tgt_tbl_nm, script_path, country, region, market,partitionby,datalakepath,loadtype in zip(
            self.kpilist_kpi.select("TargetKPITableName").collect(),
            self.kpilist_kpi.select("ScriptPath").collect(),
            self.kpilist_kpi.select("Country").collect(),
            self.kpilist_kpi.select("Region").collect(),
            self.kpilist_kpi.select("Market").collect(),
            self.kpilist_kpi.select("Partitionby").collect(),
            self.kpilist_kpi.select("DataLakePath").collect(),
            self.kpilist_kpi.select("LoadType").collect()):
          
          self.exec_start_time = datetime.datetime.now()
          
          try:

            ############## Validate Null/Blank Values ##############

            if tgt_tbl_nm[0] and script_path[0] and country[0] and region[0] and market[0] \
                and partitionby[0] and datalakepath[0] and loadtype[0]:
                
                fname = tgt_tbl_nm[0]
                  
                sql_script_path = script_path[0]
                col_partitionby=partitionby[0]
                path_datalake=datalakepath[0]
                schema_loadtype=loadtype[0]
                
                paramctry="'" + country[0].replace(",", "','") + "'"
                paramrgn="'" + region[0].replace(",", "','") + "'"
                parammkt="'" + market[0].replace(",", "','") + "'"
                
                sql_file = self.spark.sparkContext.wholeTextFiles('dbfs:'+sql_script_path)                
                
                sql_script_qry = sql_file.collect()[0][1]                
                
                sql_script_qry = sql_script_qry.format(
                ctry=paramctry, rgn=paramrgn, mkt=parammkt)
                print(sql_script_qry)
                self.create_persitent_tables(sql_script_qry, fname,col_partitionby,path_datalake,schema_loadtype)

                ############## Record Successfull Message ##############

                self.proc_capture_log(
                fname, 'calculate_kpi', 'success', 'NA')
            else:

                ############## Record Validation Fail ##############

                fname = tgt_tbl_nm[0]
                self.proc_capture_log(
                fname, 'calculate_kpi', 'validation', 'Fail')
          except Exception as err:
            #raise Exception(e)
            err_desc = str(err).replace("'", "").replace(
            '"', '').replace("\n", " ")
            self.proc_capture_log(
            fname, 'calculate_kpi', 'error', err_desc)

    def calculate_kpi_main(self):
        """ Function to act as single entry point for execution context """        
        self.read_kpi_config()
        self.get_kpilist()
        self.create_source_temp_tables()
        self.create_stg_temp_tables()
        self.calculate_kpi()
 

# COMMAND ----------

############### Main() ###############
data_feed = DataFeed('Activity','CRMF','TOTAL_CALLS_AG_TERR','', '/mnt/blob-devtest-kpi-foundation/Config/KPI_Library.csv')
data_feed.calculate_kpi_main()

