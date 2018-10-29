#!/usr/bin/env bash

table="baiqishi_ivs_risk_user"

columns="BD@Mz@channelName,BD@Mz@uuid,BD@Mz@userCertNo,BD@Mz@createTime,BD@Mz@updateTime,BD@Mz@honorData@strategySet@Mz@0@Mz@hitRules@Mz@0@Mz@ruleName,BD@Mz@honorData@Mz@finalDecision"

# sql的字段需要使用 ``的符号包裹，在shell中需要加上\转义
sql="select \`BD@Mz@channelName\` as channelName,\`BD@Mz@uuid\` as uuid,\`BD@Mz@userCertNo\` as userCertNo,\`BD@Mz@createTime\` as createTime,\`BD@Mz@updateTime\` as updateTime,\`BD@Mz@honorData@strategySet@Mz@0@Mz@hitRules@Mz@0@Mz@ruleName\` as bqs_ruleName,\`BD@Mz@honorData@Mz@finalDecision\` as bqs_finalDecision from baiqishi_ivs_risk_user"

# 参数检测
if [ -n "${1}" -a -n "${2}" -a -n "${3}" ];then
    table=$1
    columns=$2
    sql=$3
  else
    echo "需要传入3个参数，table columns sql"
fi


echo ${table}
echo ${columns}
echo ${sql}

/usr/local/Cellar/spark/2.3.1/libexec/bin/spark-submit \
--class com.maizi.Application \
--driver-memory 1g \
--executor-memory 1g \
--total-executor-cores 2 \
--driver-java-options -DPropPath=/Users/zhangzhiqiang/Documents/projects/mongodb-on-spark/src/main/resources/stb/application.properties \
/Users/zhangzhiqiang/Documents/projects/mongodb-on-spark/scrpits/mongodb-on-spark-jar-with-dependencies.jar ${table} ${columns} "${sql}"