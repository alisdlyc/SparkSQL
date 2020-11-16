# SparkSQL
## 项目目录
+ SparkRDD
    + RDD的创建
    + RDD的转换和行动算子
+ SparkZKPK
    + Spark词频统计
    + Spark倒排索引
    + Spark多值排序
+ MovieAnalyze
    + 计算一周中每一天电影的观看量，并且按照观看量排序
    + 统计电影榜评分 Top10 电影
        + Dao：数据交互层
        + Data：数据源
        + Domain：实体类
        + ETLJob：ETL 操作
        + Utils：工具类
    + 数据采集
    + 需求分析
    + 数据清洗
    + 数据分析
    + 结果存储
+ LogsAnalyze
    + 网站用户访问时间段分析
        + emote_addr：记录客户端的 IP 地址
        + remote_user：记录客户端用户名称
        + time_local：记录访问时间与时区
        + request：记录的请求的 URL 和 HTTP 协议
        + status：记录请求状态
        + body_bytes_sent：记录发送给客户端文件主体内容的大小
        + 中间分隔符 "-"
        + http_referer：记录从哪个页面链接访问过来的
        + http_user_agent：客户端浏览器相关信息
        + 中间分隔符 "-"
        + http_arr_referer：记录用户到达的页面链接
    + 数据采集
        + Nginx日志
    + 需求分析
    + 数据清洗
        + 保留有用的字段，剔除其他字段数据
        + 转换日期格式为我们需要的格式
    + 数据分析
    + 结果存储
    