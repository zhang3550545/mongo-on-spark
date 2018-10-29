### mongodb-on-spark项目介绍

#### 1. Application类

功能介绍：启动类，读取mongo中的数据，执行spark sql，写入到mysql中

参数传递：

1.配置文件：在resource对应的目录下

Application中关于mongo和mysql的参数，均通过配置文件传入，传入的方式：

```
通过jvm 参数 -DPropPath="/xxx/application.properties 的形式将配置文件传入

配置文件内容，具体可参考 src/main/resource/dev/application
```

2.System.args参数

System.args参数需要传入3个，分别是：

- table：表名，为mongo的collection和mysql的table。
    
    如：baiqishi_ivs_risk_user

- columns：是从mongo中去数据的schema的约束字段，以逗号分割

    例如：
    
    ```
    BD@Mz@channelName,BD@Mz@uuid,BD@Mz@userCertNo,BD@Mz@createTime,BD@Mz@updateTime,BD@Mz@honorData@strategySet@Mz@0@Mz@hitRules@Mz@0@Mz@ruleName,BD@Mz@honorData@Mz@finalDecision
    ```
- sql：sparksql执行的sql语句

    例如：这里默认所有的临时表都应和参数传入的table表一致
    
    ```
    "select `BD@Mz@channelName` as channelName,`BD@Mz@uuid` as uuid,`BD@Mz@userCertNo` as userCertNo,`BD@Mz@createTime` as createTime,`BD@Mz@updateTime` as updateTime,`BD@Mz@honorData@strategySet@Mz@0@Mz@hitRules@Mz@0@Mz@ruleName` as bqs_ruleName,`BD@Mz@honorData@Mz@finalDecision` as bqs_finalDecision from baiqishi_ivs_risk_user"
    ```
    
### 2. scripts目录

spark sql的spark-submit提交的脚本

dev目录：开发环境（master：local）

stb目录：测试环境（master：local spark cluster）

生产环境这里没有

**在dev和stb目录下都有一个applicatipn.sh脚本，脚本有默认的参数，默认执行 白骑士。可以通过传入参数替换。**

dev环境和stb环境的区别在与配置参数不同

