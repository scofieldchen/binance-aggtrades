# 优化脚本

## 数据获取模块

通过API下载数据效率太低，转为从历史数据仓库下载，并使用多线程加速下载。

https://data.binance.vision/

历史数据仓库按照资产/时间分区，按日存储数据。

我需要创建一个函数，从binance历史数据仓库下载聚合交易数据。

请求url: 
- https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-02-27.zip
- https://data.binance.vision/data/spot/daily/aggTrades/ETHUSDT/ETHUSDT-aggTrades-2025-02-16.zip

函数设计：
- 包含两个参数：交易对名称，日期
- 数据集较大，从几MB到几十MB不等，使用最佳方式来下载大文件
- 异常处理：无法找到对应文件，网络请求异常
- 输出pandas数据框，数据结构和`get_daily_agg_trades`保持一致

解压缩的数据集是csv文件，不包含表头，字段从左到右分别是：
- aggregate trade id
- price
- quantity
- first trade id
- last trade id
- timestamp(精确到毫秒)
- is buyer maker
- is the trade best price match

## 优化数据存储模块

数据分区逻辑发生变化，资产类别（现货/合约）-> 交易对 -> 年份 -> 月份 -> 按日存储。

布隆过滤器

关于统计信息，每存储一个数据框就创建一份对应的json文件来存储开始时间等元数据，这样有什么意义？为什么不能把数据存储到一个json文件中？

先放弃这部分逻辑，目前只需要下载数据，以后再加入sqlite数据库来存储元数据。
