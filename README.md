### Flink集群
- 客户端Client：代码由客户端获取并做转发，之后提交给JobManager
- 协调调度中心JobManager: Flink集群中的管事人，对作业进行中央调度管理；而它获取到要执行的作业后，会进一步处理转发，然后分发任务给众多的TaskManager
- TaskManager：真正的干活人，数据的处理都由taskManager来做1

- 业务数据(MYSQL)，
- 用户行为数据(由埋点来)，日志文件记录
- 爬虫数据，爬别人家数据
### 数据仓库：是为企业指定决策，提供数据支持的；数据仓库并不少数据的最终目的地，而是为数据最终的目的第做好准备，这些准备包括对数据的：备份，清洗，聚合，统计等
- 将用户行为数据通过Flume,业务数据通过DataX将数据到HDFS（数据仓库上）
### 数仓分层
- ODS 接收传来的数据，主要进行备份（数据层）
- DWD 进行数据清洗，敏感数据脱敏（基础层）
- DWS 预聚合（联结层） 
- ADS 统计最终指标（集市层）
数据输入->数据分析->数据输出(报表系统/用户画像/推荐系统)
#### 技术选型
- 数据采集传输：Flume(通用),Kafka(通用),DataX(实时),Maxwell(通用)
- 数据存储：MySql(通用),HDFS(离线),HBase(实时),Redis(实时)
- 数据计算：Hive(离线),Spark(离线),Flink(实时)
- 数据查询:Presto(离线),ClickHouse(实时)
- 数据可视化：Superset(离线),Sugar(实时)
- 任务调度(离线):DolphinScheduler
- 集群监控:Zabbix(离线),Prometheus(实时)
- 元数据管理 Atlas
- 权限管理：Ranger，Sentry
### 免密登录配置
- 进入.ssh目录下
- 执行 ssh-keygen -t rsa
- 执行 ssh-copy-id hadoop102(ssh-copy-id hadoop103,ssh-copy-id hadoop104)
- 这样再执行xsync命令就不需要再次输入密码了

[//]: # (#### zookeeper leader 选取规则)

[//]: # (先启动的是leader)

source 
    taildir 实时读取文件数据，支持断点续传
    avro 配合sink avro使用
    nc
    exec 不支持断点续传
    spooling 支持断点续传，监控文件夹
    kafka source
channel
    file
    memory
    kafka channel
slink
    hdfs sink
    kafka sink

### Flume配置流程
- 定义组件
  - al.sources = r1
  - al.channel = c1
- 配置sources
  - al.sources.r1.type = TAILDIR
  - al.sources.r1.filegroups = f1
  - al.sources.r1.filegroups.f1 = /opt/module/applog/log/app.*
  - al.sources.r1.positionFile = /opt/module/flime/taildir_position.json
- 配置channels
  - al.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
  - al.sources.c1.kafka.bootstrap.servers = hadoop102:9092,hadoop103:9092
  - al.sources.r1.kafka.topic = topic_log
  - al.sources.r1.parseAsFlumeEvent = false 
- 配置sink
- 组装
  - al.sources.r1.channels =  c1