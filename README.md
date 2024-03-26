# H2Duck
combine h2database and DuckDB, than make DuckDB can be used in more and more java projects

OLAP 数据库代理，兼容mysql语法，让你不用修改微服务和数据库的情况下，单机上具备一个spark集群的计算能力。

目标：
帮助企业能在单机低成本的前提下完整部署项目（mysql+数据库代理）
如果没有历史mysql包袱，可以完全用duckdb作为主数据库

动机：
duckdb日渐成熟，展示出强劲的性能，亿级数据量秒出，甚至 商用的htap数据库整个集群的查询性能，唯一的不足是 它的语法不兼容mysql，也是嵌入式数据库，不支持远程jdbc连接，这样 常规的中小型微服务项目 要使用上duckdb，迁移代价巨大。
本项目 受 duckdb_fdw 项目启发（pgsql下用duck_fdw加速分析），为了弥补这个技术空白，把duckdb的性能增量，免费地带入使用mysql技术的生态圈中

使用到的其他开源技术：
1）duckdb 内核计算引擎，不同数据源的连接
2）H2database，复用前端jdbc，和后端tcpserver
3）Calicte,jsqlparser, 用于解析mysql中各类型语法，mysql4）和duckdb函数兼容处理（例如日期相关函数）

使用场景：
1）作为无状态的数据查询代理，对mysql做代理，最好跟读库部署在同一个服务器中（ 大表大量数据时 网络io不可忽略），
可以加速多表join 慢查询，有filter下推，但没有lookup join，如果维度表巨大但是被关联到的数据量很少情况，本代理效果有打折扣
2）直接作为完整独立的数据库使用，性能最大化， 本项目通过第一期beta测试前，不建议这样用。

运行启动数据库命令：
java -Dfile.encoding=UTF-8 -jar {h2duck}.jar startserver {/path/to/config}

运行单元测试参数配置:
jdk 8
运行类:testhdserver.TestAll
运行参数：{/path/to/test_config}

配置说明,配置格式为yaml, 例子参考：config_example.yaml, test_config_example.yaml：

配置参数含义
#基础配置
name: 数据库名字，如果不是代理模式，会在数据库文件夹中创建对应名字的文件作为主数据库文件
baseDir: 数据库文件所在的目录
port: 服务器tcp连接监听端口
maxConnection: 服务器同时保持的连接数，由于主要定位是 最小资源消耗olap加速，所以尽量qps小，并发连接少
maxSqlTranCacheSize: 单查询回话中，mysq转换为duckdbsql的结果缓存数量

#日志相关
logDir: 日志存放的文件夹
logFileRowLimit: 单日志文件的最大行数
logFileCount: 日志文件滚动的文件数

#mysql代理相关
mysqlIp: mysql数据库的ip地址
mysqlPort: mysql数据库的ip端口
mysqlUser: mysql的用户名，目前只支持用用户密码方式连接，不支持ssl
mysqlPassword: mysql用户的访问密码
mysqlSchemaCacheTime: duckdb中，缓存mysqlchema的持续时间，缓存过期后查询会产生额外时间重新拉取mysqlschema
forbidWrite: 代理设置为只读，这时能防止mysqlchema频繁更新拖慢查询响应速度

