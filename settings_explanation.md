## memory/settings.py  

#### NEED_CERTIFICATE
if your hadoop cluster need certificate, set it to True

#### KEYTAB_PATH
if NEED_CERTIFICATE=True, set KEYTAB_PATH to where you have put your keytab file  
if NEED_CERTIFICATE=False, set to ''  

#### PRINCIPAL
if NEED_CERTIFICATE=True, set PRINCIPAL to your kerberos user name  
if NEED_CERTIFICATE=False, set to ''  

#### SERVER_PORT
IML-Predictor is a web application, which needs a port number

#### ImpalaConstants
- Host: Impala instance address
- Port: Impala instance port
- User: Your Impala Username
- Version: Imapala version

## memory/log_settings.py
you can modify format of formatters, level of handlers and handlers of loggers or leave them as they are.  
what matters most are handlers' filenames, you must create log directory before start running  
IML-Predictor application.

## memory/model/settings.py

#### MODEL_DIR
Use mkdir command to create a directory to hold your models and put its path to MODEL_DIR  

#### FEATURE_FILE
file to hold all the trainning data, you can leave it as it is  

#### RESULT_FILE
file to record cross-validation accuracy result, you can leave it as it is 

#### HDFS
- FILE_PATTERN: HDFS's file name pattern, leave it as it is
- THREAD_NUM: when downloading hdfs files, how many threads you need
- LOCAL_PATH: local path for saving hdfs files, leave it as it is
- REMOTE_PATH: remote hdfs file path, which is a parameter for [spark application](./fex)
- NODES: HDFS proxy nodes address

#### SparkSubmit
- LOCAL: if you can run sparksubmit in current server, set LOCAL=True
- PREFIX: leave it as it is
- APP: [spark application](.fex) jar name
 

#### SparkSubmit.SPARK_SUBMIT_PARAMS
- class: leave it as it is
- master: spark-submit's parameter
- driver-memory: spark-submit's parameter
- executor-memory: spark-submit's parameter
- num-executors: spark-submit's parameter
- executor-cores: spark-submit's parameter


#### FEATURE_NUM
how many features your models need, which should not beyond numbers of all features set in memory/model/constants.py  
and less than 6 according to internal mechanism  

#### COLUMNS_CLEAN_FUNC
a dict with feature's name as key, the function you want to apply on that feature as value,
you can define the function as you like or use python built-in functions

#### MEMORY_SPLIT
our machine-learning use multi-classification, you need a list of memory boundaries to label the samples

#### CLASS_NUM
how many categories you want to label the sample

#### ACCURACY_SPLIT
memory boudaries for getting accuracy result from cross-validating

#### CROSS_VALIDATE_RATIO
trainning and validating ratio when cross-validating, range from (0,1)

#### MEMORY_PREDICT_RATIO
To improve accuray, we return the predicted memory getting from the predicted category memory boundary   
and memory boundary of the category next to it, range from [0,1]

#### MODEL_GROUP
MODEL_GROUP is a list of dicts which refer to a series of models' definition. In each dict you must  
set key "name" with value different from other dicts, there is an optional key "pool_group" which  
refers to pools the model should be suited to, if not set, it will suit all pools in the cluster by default.  

##### For example
if you only need one model, you can set MODEL_GROUP=[{'name': "first"}]  
if you want to separate model by pools, then add 'pool_group' key for each model. Note that if you are  
not sure of all the pools in the cluster, you'd better set a default model with "pool_group" not sepecified. 