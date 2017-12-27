# Ant_Tianchi_CCF_Positioning
2017CCF大数据与计算智能大赛-蚂蚁金服-商铺定位赛题(第5名)

### 赛题链接
[商场中精确定位用户所在店铺](https://tianchi.aliyun.com/competition/introduction.htm?spm=5176.100068.5678.1.aa26a5emijaWb&raceId=231620)


### 文件说明
* getFeatures.sql<br>为二分类部分预处理与得到特征中间文件处理，之后再在PAI平台上搭建组件让每个特征文件和构建的样本join起来，多分类部分由队友负责
* wifiFingerprint.ipynb<br>为初赛的python版构建指纹库及计算指纹得分，复赛java实现在udtf中
* udtf为getFeatures<br>用到的几个udtf方法
* xgb_train.sql<br>为PAI平台的xgboost命令(平台没有xgboost的拖拽组件，只能用PAI命令)
* submission.sql<br>为提交结果部分，包括最佳成绩的加权平均


### 训练说明：
* 7.1-8.17做8.18-8.31统计区间，7.15-8.31做9.1-9.14测试集的统计区间
* 复赛有约38%的数据没有wifi信息，采取的策略是测试集有wifi的部分用训练集全集训练后预测，测试集无wifi的部分只由训练集无wifi的部分训练后预测
* 由于资源限制，复赛我们主要使用可以限制核心数的PS-SMART模型,最终融合主要由<br>
1)加入了PS-SMART多分类概率特征的bagging结果；<br>
2)加入了RF多分类概率特征的bagging结果；<br>
3)加入了两种多分类概率特征的bagging结果<br>
进行加权平均得到。


### 候选说明：
* 初赛利用指纹得分及各种规则筛选出候选样本，正负样本比1：13，覆盖率0.973，最后补全训练集正样本
* 复赛构造候选集直接用记录wifi交互过的所有shop；用户到过的所有shop；和离交易中心最近的10个shop取并集
倍数约23倍，覆盖率0.965，我们队伍候选集及覆盖率上相比别的队伍差了不少，覆盖率是个很大的提分点


### 特征说明：
* 计算rate时分母有两种，该bssid总数(rate_inbssid)和该shop总数(rate_inshop)，由于bssid存在稀疏现象，需要对rate_inbssid进行平滑，为了方便，直接将bssid在商场出现次数小于6的rate_inbssid置零
* 筛选数据为只保留所有记录的前3强wifi；
* 缺失bssid的rssi用-113代替
* 每条记录取10条wifi按强度topk排序展开构造特征，不足10的填缺失值
* 所有统计特征构造完后又构造了top3尺度累加和top10尺度累加特征
* 所有特征严格用对应统计区间构造
* 特征维数太高，训练时丢弃了大部分count特征，保留了rate特征


### 特征：
* 多分类概率特征（多分类概率一定要跟stacking一样划分窗口防止穿越，仅利用一个多分类概率特征在复赛提升了1.7个百分点，我们最终使用了PS-SMART和随机森林两种多分类概率特征,做特征和选候选不能用同一组多分类,虽然多分类线上成绩并不高,但其概率做特征在二分类中特征重要性非常高,多分类候选都是取概率最高的N个shop,会加入很多高概率的负样本导致预测错误）
* 指纹算法得分及组内占比（由于不同shop或mall的wifi数量差别大，某些特征转化为以row_id为组的组内占比有不小的提升效果，比rank好）
* 与店铺交易位置/店铺实际位置 中值的经纬度距离(由于有的店铺出现次数很少，也需要按统计区间来构造)
* topkwifi在店铺的rssi中值,最强值及之差，在筛选记录中构造同样特征
* topkwifi在shop里出现了多少次，shop总bssid数，bssid在mall的总出现数,及rate_inshop,rate_inbssid
* topkwifi在筛选数据中(筛选条件是只保留记录top3wifi)，bssid在shop里出现了多少次,及rate_inshop,rate_inbssid
* topkwifi在shop里rssi_rank出现的count,及rate_inshop,rate_inbssid（比如记录最强bssid在shop历史记录里也是最强的记录数）
* topkwifi在shop里rssi偏差小于8出现了多少次,及rate_inshop,rate_inbssid
* topkwifi在该商场几个店铺出现过,及在商场的占比
* topkwifi在该shop的rssi方差
* 记录所有wifi在shop的连接count,及rate_inshop,rate_inbssid
* 记录所有wifi和shop历史wifi取交集，计算记录wifi与在shop的中值强度之差的绝对值的均值,在筛选记录中构造同样特征
* 记录所有wifi和shop历史wifi取交集，计算记录wifi_rank与其在shop的历史记录rank均值之差的绝对值的均值
* 记录所有wifi曾经出现的平均price与记录price之差的绝对值的均值
* 记录所有wifi有几个在shop记录里
* shop出现过几个wifi
* 记录所有wifi的rssi比shop历史记录该bssid最强值强的个数
* price、category的wifi出现数
* topkwifi在shop的强度中值rank
* 记录所有wifi在店铺的rssi贡献率，贡献率用-1/rssi计算
* user和shop交叉统计
* shop和hour交叉统计
* category和hour交叉统计
* 店铺的再次光顾率
* 店铺在时间点类型(是否饭点)的记录数、两种比例
* 店铺在日期点类型(是否周末)的记录数、两种比例
* 用户平均消费水平与店铺消费水平差


### 其他思路：
* 每条记录wifi按强度排序，并将rssi离散化，bssid和rssi合起来看成一个单词，利用doc2vec生成比如20纬向量作为特征
* 仅利用经纬度进行KNN得到topN店铺，含义是看曾经离该位置最近的K个人在哪些店铺
* 由于多分类利用softmax,所以只考虑 预测概率>1/类别数 的店铺作为候选
* topkwifi与shop的共现记录数
