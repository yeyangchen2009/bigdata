## 运维中心任务甘特图制作

> 获取任务开始时间，结束时间，运行时长，用于制作甘特图，确定耗时多的任务，逐个击破。

![](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/sp20251119_174302_095.png)

> 用excel生成甘特图，从面积长度可以直观看出每个任务的持续时间。

![](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/sp20251119_174619_806.png)
![](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/sp20251119_174652_078.png)
![](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/sp20251119_174815_603.png)


## 基于Spark UI进行YARN集群优化实践

1、 Spark sql任务默认summit包含以下参数:

1. driver-core, driver-memory

2. exectours-num、 exectours-core, exectours-memory
    ①上述 driver相关的参数,基本上使用默认即可,不需要进行调整。
    driver 参数，主要是指执行代理基于 Spark runner 提交所使用参数。
    exectours可以认为是 YARN 的执行进程、task为进程中的执行线程。
    2、最终执行任务，由每个任务所对应的 ApplicationMaster进行与 Yarn 资源管理器进行协调。
    1)执行代理 Spark runner-> CM 管理节点->AppMaster->协调 CM 空闲节点->分配 Proxy->下载 jar 包以及
    HDFS 文件;
    2） 关于 memory 不是一味加大、也不是一味保持 1g. 若需要则调优为 2~8G （默认则不需要 ）
    ① 内存过大，会加重、拉长一次 GC 的时间；
    ②内存过小，会触发频繁 GC;
    3)关于exectours-core计算公式:需结合计算节点(<=12)每节点<12.8核、以及内存<4g以及并行任务数
    （此项需综合评估，扣除系统本身运算支撑以及整体任务的并发数、均摊下来可用的资源）
    调优示例一：（优化第一要义，优先考虑 Spark sql 本身，而非集群）
    1)在默认配置情况下，首次跑出9分钟。
    2） Exectour 频繁失败、并且做出增加堆外内存的告示。

  ![image-20240731100044871](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731100044871.png)

调整运行参数加大运行内存、增加Exectour （这个是根据 最终Spark UI中生成的 job 个数来定。）

![image-20240731100052733](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731100052733.png)

![image-20240731100120452](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731100120452.png)

![image-20240731100305397](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731100305397.png)

![image-20240731100340601](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731100340601.png)

![image-20240731100445204](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731100445204.png)

## sql_dws_fk stock all stage_value_detail 优化

表cache
第一次写入的是正式表,改为生成缓存表,表大小大致为800M 左右。
同步修改executor memory

![image-20240731100712913](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731100712913.png)

Partition 与 executor 数量联合优化
Partition参数优化思路

```sql
-- 自动分区,开启时会要盖spark.sgl.shuffle.partitions参数,分区数由spark程序指定。
set spark.sgl.adaptive.enabled=true;
-- 配合自动分区参数
set spark.sgl.adaptive.shuffle.targetPostShuffleInputSize=67108864b;
-- 配合自动分区参数
set spark.sgl.adaptive.join.enabled=true;
-- 自动广播，一般在 SQL 中已经有 Hint 指定广播，这边可以留着放置部分作业忘记指定。
set spark.sgl.autoBroadcastJoinThreshold=20971520;
-- 自动分区数根据具体数据数量和数据总存储占用而定。
-- 若过大，则会造成 stage中task数量过多，每个task执行时间过短。
-- task 轮换时间甚至大于 task 的执行时间
-- 并且 shuffle时 join为两个表分区的乘积，例如次数设置为 600，则任务数为 360000。
-- 若过小，则会导致单个任务中存储的数据量过大，executor 会内存不足。
-- 一般来说，需要设置合理范围，每个分区对应的数据条数为2万~10万
-- 每分区存储量在 20-50MB 左右
-- 需要注意的是，若hive数据来源文件大小不均衡，则不应设置为hive 数据来源文件数的倍数，并且不能小于文件数量。
set spark.sgl.shuffle.partitions=22;

```

```sql
Executor 优化思路
与 executor 配置相关的：
1.集群任务数据级别分为PROCESS-LOCAL, NODE-LOCAL, NO-PREF,RACK-LOCAL、ANY
这几个值在图中代表 task 的计算节点和 task 的输入数据的节点位置关系
- PROCESS_LOCAL:数据在同一个JVM中,即同一个executor上。这是最佳数据 locality。
- NODE_LOCAL:数据在同一个节点上。比如数据在同一个节点的另一个executor上;或在HDFS上,恰好有block在同一个节点上。速度比PROCESS_LOCAL稍慢,因为数据需要在不同进程之间传递或从文件中读取
- NO_PREF:数据从哪里访问都一样快,不需要位置优先
- RACK_LOCAL:数据在同一机架的不同节点上。需要通过网络传输数据及文件1O,比 NODE_LOCAL 慢
- ANY:数据在非同一机架的网络上,速度最慢
                 
```

![image-20240731101506377](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731101506377.png)

```
2. yarn 中 executor 扩充是由 num-executors*executor-cores 数量即可用核数，对比任务数决定的。一般可用核数需要在同时执行任务数的1.2倍以上才不会创建executor和 executor 内存数关系不大。
3. 当driver第一次申请executor时,yarn会尽量将其放置在同一个机器中。此时当需要shuffle时,数据级别为NODE_LOCAL ;若新增了executor ,一般情况下不会在同一个机器中,因此数据级别会变为RACK_LOCAL。并且需要初始化数据到该executor中,造成时间变长。
4. executor自动回收策略为:一分钟内没有执行任务。
5. 任务数取决于任务执行类型和partition数量,一般来说map和broadcastjoin任务的数量即为partition数量。若是大表关联大表则为partition数量的平方( left outer join 笛卡尔积）。
6. 当遇到数据不均衡的情况,若单一任务执行时间与其他任务之差会超过一分钟;会导致executor 被回收。
7. executor内存分为存储内存和执行内存,当前集群配置中,存储内存占总内存的55%。即配置10G内存, 5.5G用以存储数据(cache, brordcast), 4.5G用于计算。判断存储内存是否够用可以在之心时参考「executors」界面的「storage monory」一栏。判断执行内存是否够用,可以参考「executor」界面中「task time」一栏GC时间长短,若多个executor过长(红色)则表示需要适当增加内存。   
                                                                            
```

![image-20240731101816139](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731101816139.png)

综上, executor数量需要做到以下几点:
1,合理设置executor和core数量,使其不会受到任务数量的影响而进行伸缩。
2,合理设置内存数量,使其能够承受对应的存储且任务GC时间不会过长。



写入时执行 UNION 操作
该优化仅在当前逻辑下可用,即一张表分多次生成,并且有前后关联关系(tmp2根据tmp1的结果生成),后续可不执行该优化。

![image-20240731101858375](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731101858375.png)

SQL-union后写入

![image-20240731101951460](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731101951460.png)

SQL-单独写入
1. 关于写入hdfs时文件数问题union写入与每个单独写入在spark中的处理一样,即直接将分区数据写入到hive中。由以下图片可知,union写入时是20个文件,不执行union单独写入时也是20个文件;从文件名可知union之后由于执行了合并的SQL,因此文件重命名了。由于执行了一个union操作,执行时间稍长,这步大致只能省几秒。大于两个表的union操作待观察。

![image-20240731102056233](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102056233.png)

Union 写入时的执行计划，第 17步为 union 并写入

![image-20240731102128022](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102128022.png)

![image-20240731102146472](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102146472.png)

不执行union执行计划,第17, 18步为写入数据

![image-20240731102233772](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102233772.png)

不执行 union 执行时的文件数
2.写入顺序问题
此处需要先写入后执行的表(大表),然后写入先执行的表(小表),否则会触发重复计算。
使用left anti join替代 not exists

![image-20240731102313572](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102313572.png)

left anti join与 not exists 语义相同,但是 not exists 执行效率不如left anti join ,并且left anti join可以广播右表。需要左右表差距过大并且右表在存储内存限制范围内(broadcast ),例如此处左表tmp数量为80万、400MB右表tmp1数据量为8万、40MB

![image-20240731102406395](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102406395.png)

not exists与left anti join对比,左侧为left anti join
优化结果

![image-20240731102447707](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102447707.png)

## 数据倾斜优化，以案例 dwd_cw_cost_book_detail_df

1. 脚本运行时间大于3min,检查运行过程,发现存在明显数据倾斜现象

![image-20240731102603873](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102603873.png)

2.可以通过告诉主动告诉编译器连接哪张行广播,将连接中各个节点上重复用到的维表广播到各节点,不用重复读取,这样进行优化

![image-20240731102655217](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102655217.png)

3,优化后时间明显减少。整体时间从9.8min->2.7min
倒数第二步,最大的spill size,7.0G调整后变为736MB,节点的最大运行时间从4.5分钟,变成 1 分钟

![image-20240731102808199](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102808199.png)

倒数第三步,也有类似情况。减少了运行时间

![image-20240731102834607](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102834607.png)

优化前,这个说明task负载很高,在做拼命的shuffle动作

![image-20240731102915365](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731102915365.png)

优化后

![image-20240731103056689](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731103056689.png)

## 优化过程记录（聚合优化）

1,发现聚合时候存在数据倾斜,最大结点时间3.3min,平均7.8s
它是哈希聚合过程产生的数据倾斜,由于数据分组时候,每组数据数量不均衡,数据较多的
分组聚合运算时花费时间较多。

![image-20240731103826948](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731103826948.png)

2.通过随机增加前缀方式优化,随机增加前缀后,由于前缀随机的,每个分组数量就接近,
再通过第二次聚合,去掉前缀。这种适用于sum等,拆分成两次运算不会影响结果的场景。

![image-20240731103842711](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731103842711.png)

新增随机前缀作为 group by一部分

![image-20240731103915401](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731103915401.png)

3.更新完成后效果
更新前

![image-20240731103929535](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731103929535.png)

更新后，两次较为均衡的聚合

![image-20240731103946238](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731103946238.png)

整体运行时间从3.7min 缩短到2.6 min

![image-20240731104006437](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104006437.png)

## 优化过程记录--查询条件优化

背景：
生成环境中 sql_dwd_fk_sfee_detail，运行时间较长，脚本由两部分 uinion 查询得到， union的后半部分,运行时间超过5分钟。经过优化后该部分1分钟内可以运行完成。左侧是优化前，右侧是优化后。

![image-20240731104235570](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104235570.png)

一、查询条件优化,谓语前置
1.DAG 分析
观察该脚本DAG,整体存在多个join构成,主要步骤都是查询(HiveTableScan)和连接(SortMergeloin)。多次查询之后, join并入。

![image-20240731104259259](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104259259.png)

并且在脚本中发现低效率的遍历查询嵌套循环连接nestedLoop方式,

![image-20240731104323350](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104323350.png)

2.原脚本分析
根据DAG发现的问题,查看原先脚本,筛选位置处于join完成后的where处,导致每个join数据量较大,进而导致占用时间较长。

![image-20240731104356250](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104356250.png)

3. 优化
调整了 where 中筛选条件
左侧,耗时是优化前,查询(HiveTableScan) 5.1min,右侧优化后相同位置耗时4s 原先筛选条件处于连接步骤后,导致全表扫描并进行了连接操作,耗时较长。

![image-20240731104521526](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104521526.png)

二、查询条件写法优化
原脚本中采用"not in (子查询) ”方式,可以改为"not (子条件) ”可以减少一次查询(hive table scan)步骤,可以减少相应子查询查表时间。

![image-20240731104542325](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104542325.png)

左侧是更新前写法
右侧是更新后写法

![image-20240731104557340](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104557340.png)

## 优化过程记录--指标计算优化

背景:
指标计算脚本通常是各个板块逻辑最复杂,运行时间最长的脚本之一。并且各个板块指标,每年都有一些调整,由于该部分逻辑复杂,改动起来也费时费力。
本次在物业预算数仓的指标计算脚本优化,相同数据量下,该脚本运行速度, 1.5min减少到1min以内。且后续指标的调整变得方便。

![image-20240731104824466](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104824466.png)

一、指标计算代码依赖窄表
链路上由依赖adm宽表挪动到,依赖dws窄表。运行时间点提前,整体链路速度提升。

![image-20240731104842123](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104842123.png)

指标计算,会涉及建立的临时表,各个临时表字段量都大量减少,由47个减少到15个。左侧图片是旧脚本临时表部分字段,右侧是新脚本临时表字段。新脚本打宽位置挪动到脚本所有计算完成后。
![image-20240731104859622](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104859622.png)

二、指标计算更新为指标配置表形式
计算逻辑由固定逻辑写死代码,转换为一类指标逻辑+配置表(例如,加法逻辑、除法逻辑、部分性质辅助判断逻辑等)。
左侧图片是旧版代码逻辑,每个指标都有对应取数逻辑。新增指标都需要更新代码并校验。
右侧图片是新版代码逻辑,右下是配置表截图,通过相同代码读取配置表。新增指标可以通过更新配置表实现。

![image-20240731104948620](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731104948620.png)

## 数字营销优化记录 20240708

dwd_dmfk_visitor_record
优化方案：
1,对于临时表使用catche table方式,把数据加载到内存中
2,关联小表时,使用广播join方式
3,关闭adaptive参数,自主设置patition参数,参考房开优化patition=40 ,并增加exectuor 的内存和核心数
4, 查看执行计划,发现存在数据倾斜,调整SQL 脚本逻辑,避免字段存在大量 null出现数据倾斜（null 所引起的 数据倾斜。就是将 null给消除）---关键的一步

![image-20240731105437048](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731105437048.png)

优化前：
![image-20240731105526247](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731105526247.png)

优化后：
![image-20240731105537998](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240731105537998.png)

dws_dmfk_onsite_visitor
该作业仅为单表处理无多表关联,作业历史执行时间浮动较大
优化方案：

1. 关闭adaptive参数,自主设置patition参数,参考房开优化patition=40 ,并增加exectuor 的内存和核心数
2. 避免数据倾斜

dwd_ac_visitor_di
优化方案：
1. 提前过滤无效数据
2. 使用left antijoin
3. 对于临时表使用catche table方式,把数据加载到内存中
因部分过滤逻辑前置处理在该处进行,执行效率无明显变化

dim_dmfk_customer_project_compare
优化方案：

1. 逻辑调整:提前过滤无效数据,部分过滤逻辑前置处理(放在增量合并时处理)
2. 临时表使用 catche table方式,把数据加载到内存中
3. 关联小表时,使用广播join方式

dwd_dmfk_clues_summary
优化方案：
1. 临时表使用catche table方式,把数据加载到内存中
2. 关联小表时,使用广播join方式
3. 增加exectuor的内存和核心数

dws_dmfk_clues_summary
该作业为单表逻辑处理,无其它表关联,且数据量较大
优化方案：
1. 增加exectuor的内存和核心数



## 物业优化

![image-20240801094250715](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240801094250715.png)

```sql
conf
-- spark.executor.extraJavaOptions=-XX:+UseG1GC   -- 启用G1垃圾收集器，一种适合大堆内存的低延迟垃圾收集策略
spark.dynamicAllocation.enabled=false

-- 跑任务时，根据sparkui上的任务需求，动态调整executor个数和partitions数，确保最有资源。-- 主要
set spark.executor.instances=3;
set spark.sql.shuffle.partitions =10;

```

```sql
-- set spark.sql.adaptive.enabled=true;
-- set spark.sql.adaptive.shuffle.targetPostshuffleInputSize=67108864b;
set spark.sql.adaptive.join.enabled=true;  -- 广播join
-- set spark.sql.autoBroadcastJoinThreshold=20971520;

cache lazy table XXX    -- cache lazy
select
  /*+ BROADCASTJOIN(b) */  -- b为小表  广播join
  xxx,
  yyy
from a
join b on a.xxx = b.yyy ……

/*
"Cache lazy table" 这个术语通常用于数据库和计算机科学领域，指的是一种优化技术，用于提高数据库查询的效率。下面是对这一概念的简要解释：

 Cache：缓存是一种存储机制，用于存储频繁访问的数据，以减少对原始数据源的访问次数，从而加快数据检索速度。
 Lazy Loading：懒加载是一种按需加载数据的技术，即仅在实际需要时才加载数据。这有助于减少不必要的数据加载，提高系统性能。
 Table：在数据库中，表是数据的基本存储单元，用于组织数据集合。

结合这些概念，"cache lazy table" 可能指的是一种策略，即在数据库查询过程中，首先尝试从缓存中获取数据（如果可用），如果没有，则按需从数据库表中加载数据。这种策略可以显著提高数据检索的效率，尤其是在处理大量数据或高并发请求时。
*/

select
  xxx
from a   
left anti join b on a.id = b.id

-- left anti join 能连上的就不取，用于获取左表中存在但不在右表中的行。返回左表中那些在右表中没有匹配的行。
```

![image-20240801102909431](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/spark调优/image-20240801102909431.png)