# 1. Introduction
IML-Predictor is a tool for predicting Impala query memory limit based on Supervisored learning. We collect certain   
amount of Impala queries and their details as samples,  extract features and label them as data for trainning models,  
which are used to predict memory for new comming impala queries.  

# 2. Installation
## 2.1. Dependencies
#### System dependencies: 
 - Impala(2.9.0-cdh5.12.1 or 2.5.0-cdh5.7.2)
 - Python(>=3.5)
 - Scala(>=2.10.1)

#### Project dependencies:
 - impala-toolbox/data-divertor

## 2.2. Install
```
git clone git@gitlab.gridsum.com:data-engineering/impala-toolbox/iml-predictor.git
cd iml-predictor
sudo apt-get -y install --no-install-recommends \`cat depend_ubuntu\`
python3 -m pip install -r depend_pip3
```

## 2.3. [Edit settings of IML-Predictor](./settings_explanation.md)

## 2.4. Start/Stop
IML-Predictor is depend on impala-toolbox/data-divertor, which collects queries' details from Cloudera Manager API.  
you need to deploy data-divertor in your hadoop cluster and keep it running. 

**features** is the scala application for collecting Impala queries and their details, package and upload to spark node
```
cd features
mvn clean package
cd target
mv feature-engineering-1.0-SNAPSHOT-jar-with-dependencies.jar feature-engineering.jar
scp feature-engineering.jar username@remoteip:/home/username
```
Generate a shell script for running spark task and upload to spark node
```
python generate_spark_submit.py
chmod +x spark.sh
scp spark.sh username@remoteip:/home/username
```
Login remote server to create the directory for holding feature data
```
ssh username@remoteip
hadoop fs -mkdir iml-predictor/feature
```
Before building models, run spark.sh on spark node with parameters start_day and end_day in format '%Y%m%d', for example:
```
./spark.sh 20180312 20180318
```

Start IML-Predictor application
```
python -m memory.server
```

# 3. Tutorials & Documentation

## 3.1. Building Models
If your local server can not run spark application, set memory/model/settings.py SparkSubmit.LOCAL=False   
and make sure you have execute spark.sh shell script on spark node before building Models.

```
Post http://ip:port/v1/impala/memory/model_build  
```

Parameters(json format)  

| Name      | Type |     description    |
|-----------|------|--------------------|
| start_day | int or string  |start day for queries we collecting, in format "%Y%m%d", like "20180101" |
| end_day | int or string |end day for queries we collecting, in format "%Y%m%d", like "20180110" |
| generate_feature(optional) | bool | if you want to reuse feature collected before, set it to False |
| cross_validate(optional) | bool |if you need cross validating accuracy result saved, set it to True |  

###### Response

| Name      | Type |     description    |
|-----------|------|--------------------|
| message | string  | Information about whether models are building or not |
| error_code | int | If model succeed started building, error_code equals 0, else -1 |

###### For example:  
```
Status: 200 OK
{"message": "Models are being built now, try after some time", "error_code": -1}
```

## 3.2. Check Model Building Status
```
Get http://ip:port/v1/impala/memory/model_status 
```

###### Response
| Name      | Type |     description    |
|-----------|------|--------------------|
| status_code | int  | 0 is for Finished, 1 is for Running, 2 is for Failed |
| status_str | string| it could be "Finished", "Running" and "Failed" |

###### For example:
```
Status: 200 OK
{"status_code": 1, "status_str": "Running"}
```

## 3.3. Predict Impala Query Memory limit

```
Post http://ip:port/v1/impala/memory/predict  
```

Parameters(json format)  

| Name      | Type |     description    |
|-----------|------|--------------------|
| sql | string  | the sql you want to predict memory limit |
| db | string | which db the sql will execute in |
| pool(optional) | string | impala pool name for sql executing |

###### Succeed Response
| Name      | Type |     description    |
|-----------|------|--------------------|
| mem | int  | memory limit for sql, unit("MB") |
| error_code | int | error_code is zero in this case |

###### For example:  
```
{"mem": 400, "error_code": 0}
```
###### Error Response:  
| Name      | Type |     description    |
|-----------|------|--------------------|
| message | string  | message explain predict failure reason |
| error_code | int | code corresponding to that situation |

##### Error Code Explanation:
| Value     |     description    |
|-----------|--------------------|
| 1 | PARAMETER_ERROR |
| 2 | IMPALA_CONNECT_ERROR |
| 3 | IMPALA_QUERY_ERROR |
| 4 | NetWorkError |
| 5 | MODEL_FILE_NOT_FOUND_ERROR |
| 6 | UNKNOWN_ERROR |


# 4. Communication
  impala-toolbox-help@gridsum.com

# 5. License
IML-Predictor is [licensed under the Apache License 2.0.](./LICENSE)
