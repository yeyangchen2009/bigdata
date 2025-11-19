# Flink中极其重要的Time与Window详细解析

### 前言

Flink 是流式的、实时的 计算引擎

上面一句话就有两个概念，一个是流式，一个是实时。

**流式**：就是数据源源不断的流进来，也就是数据没有边界，但是我们计算的时候必须在一个有边界的范围内进行，所以这里面就有一个问题，边界怎么确定？无非就两种方式，**根据时间段或者数据量进行确定**，根据时间段就是每隔多长时间就划分一个边界，根据数据量就是每来多少条数据划分一个边界，Flink 中就是这么划分边界的，本文会详细讲解。

**实时**：就是数据发送过来之后立马就进行相关的计算，然后将结果输出。这里的计算有两种：

- **一种是只有边界内的数据进行计算**，这种好理解，比如统计每个用户最近五分钟内浏览的新闻数量，就可以取最近五分钟内的所有数据，然后根据每个用户分组，统计新闻的总数。
    
- **另一种是边界内数据与外部数据进行关联计算**，比如：统计最近五分钟内浏览新闻的用户都是来自哪些地区，这种就需要将五分钟内浏览新闻的用户信息与 hive 中的地区维表进行关联，然后在进行相关计算。
    

本篇文章所讲的 Flink 的内容就是围绕以上概念进行详细剖析的！

## Time与Window

### Time

在Flink中，如果以时间段划分边界的话，那么时间就是一个极其重要的字段。

Flink中的时间有三种类型，如下图所示：

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img42.png)

- **Event Time**：是事件创建的时间。它通常由事件中的时间戳描述，例如采集的日志数据中，每一条日志都会记录自己的生成时间，Flink通过时间戳分配器访问事件时间戳。
    
- **Ingestion Time**：是数据进入Flink的时间。
    
- **Processing Time**：是每一个执行基于时间操作的算子的本地系统时间，与机器相关，默认的时间属性就是Processing Time。
    

例如，一条日志进入Flink的时间为2021-01-22 10:00:00.123，到达Window的系统时间为2021-01-22 10:00:01.234，日志的内容如下： 

2021-01-06 18:37:15.624 INFO Fail over to rm2

对于业务来说，要统计1min内的故障日志个数，哪个时间是最有意义的？—— eventTime，因为我们要根据日志的生成时间进行统计。

### Window

Window，即窗口，我们前面一直提到的边界就是这里的Window(窗口)。

官方解释：**流式计算是一种被设计用于处理无限数据集的数据处理引擎，而无限数据集是指一种不断增长的本质上无限的数据集，而window是一种切割无限数据为有限块进行处理的手段**。

所以**Window是无限数据流处理的核心，Window将一个无限的stream拆分成有限大小的”buckets”桶，我们可以在这些桶上做计算操作**。

#### Window类型

本文刚开始提到，划分窗口就两种方式：

1.  根据时间进行截取(time-driven-window)，比如每1分钟统计一次或每10分钟统计一次。
    
2.  根据数据进行截取(data-driven-window)，比如每5个数据统计一次或每50个数据统计一次。
    

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img44.png)
窗口类型

对于TimeWindow(根据时间划分窗口)， 可以根据窗口实现原理的不同分成三类：**滚动窗口（Tumbling Window）、滑动窗口（Sliding Window）和会话窗口（Session Window）**。

1.  **滚动窗口（Tumbling Windows）**
    
将数据依据固定的窗口长度对数据进行切片。

特点：**时间对齐，窗口长度固定，没有重叠**。

滚动窗口分配器将每个元素分配到一个指定窗口大小的窗口中，滚动窗口有一个固定的大小，并且不会出现重叠。

例如：如果你指定了一个5分钟大小的滚动窗口，窗口的创建如下图所示：

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img43.png)
滚动窗口

适用场景：适合做BI统计等（做每个时间段的聚合计算）。

2.  **滑动窗口（Sliding Windows）**

滑动窗口是固定窗口的更广义的一种形式，滑动窗口由固定的窗口长度和滑动间隔组成。

特点：**时间对齐，窗口长度固定，有重叠**。

滑动窗口分配器将元素分配到固定长度的窗口中，与滚动窗口类似，窗口的大小由窗口大小参数来配置，另一个窗口滑动参数控制滑动窗口开始的频率。因此，滑动窗口如果滑动参数小于窗口大小的话，窗口是可以重叠的，在这种情况下元素会被分配到多个窗口中。

例如，你有10分钟的窗口和5分钟的滑动，那么每个窗口中5分钟的窗口里包含着上个10分钟产生的数据，如下图所示：

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img48.png)
滑动窗口

适用场景：对最近一个时间段内的统计（求某接口最近5min的失败率来决定是否要报警）。

3.  **会话窗口（Session Windows）**

由一系列事件组合一个指定时间长度的timeout间隙组成，类似于web应用的session，也就是一段时间没有接收到新数据就会生成新的窗口。

特点：**时间无对齐**。

session窗口分配器通过session活动来对元素进行分组，session窗口跟滚动窗口和滑动窗口相比，不会有重叠和固定的开始时间和结束时间的情况，相反，**当它在一个固定的时间周期内不再收到元素，即非活动间隔产生，那个这个窗口就会关闭**。一个session窗口通过一个session间隔来配置，这个session间隔定义了非活跃周期的长度，当这个非活跃周期产生，那么当前的session将关闭并且后续的元素将被分配到新的session窗口中去。

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img45.png)
会话窗口

### Window API

#### TimeWindow

TimeWindow是将指定时间范围内的所有数据组成一个window，一次对一个window里面的所有数据进行计算（就是本文开头说的对一个边界内的数据进行计算）。

我们以 **红绿灯路口通过的汽车数量** 为例子：

红绿灯路口会有汽车通过，一共会有多少汽车通过，无法计算。因为车流源源不断，计算没有边界。

所以我们统计每15秒钟通过红路灯的汽车数量，如第一个15秒为2辆，第二个15秒为3辆，第三个15秒为1辆 …

- **tumbling-time-window (无重叠数据)**
    

我们使用 Linux 中的 nc 命令模拟数据的发送方

```
 11.开启发送端口，端口号为9999
 2nc -lk 9999
 3
 42.发送内容（key 代表不同的路口，value 代表每次通过的车辆）
 5一次发送一行，发送的时间间隔代表汽车经过的时间间隔
 69,3
 79,2
 89,7
 94,9
102,6
111,5
122,3
135,7
145,4
```

Flink 进行采集数据并计算：

```
 1object Window {
 2  def main(args: Array[String]): Unit = {
 3    //TODO time-window
 4    //1.创建运行环境
 5    val env = StreamExecutionEnvironment.getExecutionEnvironment
 6
 7    //2.定义数据流来源
 8    val text = env.socketTextStream("localhost", 9999)
 9
10    //3.转换数据格式，text->CarWc
11    case class CarWc(sensorId: Int, carCnt: Int)
12    val ds1: DataStream[CarWc] = text.map {
13      line => {
14        val tokens = line.split(",")
15        CarWc(tokens(0).trim.toInt, tokens(1).trim.toInt)
16      }
17    }
18
19    //4.执行统计操作，每个sensorId一个tumbling窗口，窗口的大小为5秒
20    //也就是说，每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量。
21    val ds2: DataStream[CarWc] = ds1
22      .keyBy("sensorId")
23      .timeWindow(Time.seconds(5))
24      .sum("carCnt")
25
26    //5.显示统计结果
27    ds2.print()
28
29    //6.触发流计算
30    env.execute(this.getClass.getName)
31
32  }
33}
```

我们发送的数据并没有指定时间字段，所以Flink使用的是默认的 Processing Time，也就是Flink系统处理数据时的时间。

- **sliding-time-window (有重叠数据)**
    

```
 1//1.创建运行环境
 2val env = StreamExecutionEnvironment.getExecutionEnvironment
 3
 4//2.定义数据流来源
 5val text = env.socketTextStream("localhost", 9999)
 6
 7//3.转换数据格式，text->CarWc
 8case class CarWc(sensorId: Int, carCnt: Int)
 9
10val ds1: DataStream[CarWc] = text.map {
11  line => {
12    val tokens = line.split(",")
13    CarWc(tokens(0).trim.toInt, tokens(1).trim.toInt)
14  }
15}
16//4.执行统计操作，每个sensorId一个sliding窗口，窗口时间10秒,滑动时间5秒
17//也就是说，每5秒钟统计一次，在这过去的10秒钟内，各个路口通过红绿灯汽车的数量。
18val ds2: DataStream[CarWc] = ds1
19  .keyBy("sensorId")
20  .timeWindow(Time.seconds(10), Time.seconds(5))
21  .sum("carCnt")
22
23//5.显示统计结果
24ds2.print()
25
26//6.触发流计算
27env.execute(this.getClass.getName)
```

#### CountWindow

CountWindow根据窗口中相同key元素的数量来触发执行，执行时只计算元素数量达到窗口大小的key对应的结果。

**注意：CountWindow的window_size指的是相同Key的元素的个数，不是输入的所有元素的总数**。

- **tumbling-count-window (无重叠数据)**
    

```
 1//1.创建运行环境
 2val env = StreamExecutionEnvironment.getExecutionEnvironment
 3
 4//2.定义数据流来源
 5val text = env.socketTextStream("localhost", 9999)
 6
 7//3.转换数据格式，text->CarWc
 8case class CarWc(sensorId: Int, carCnt: Int)
 9
10val ds1: DataStream[CarWc] = text.map {
11  (f) => {
12    val tokens = f.split(",")
13    CarWc(tokens(0).trim.toInt, tokens(1).trim.toInt)
14  }
15}
16//4.执行统计操作，每个sensorId一个tumbling窗口，窗口的大小为5
17//按照key进行收集，对应的key出现的次数达到5次作为一个结果
18val ds2: DataStream[CarWc] = ds1
19  .keyBy("sensorId")
20  .countWindow(5)
21  .sum("carCnt")
22
23//5.显示统计结果
24ds2.print()
25
26//6.触发流计算
27env.execute(this.getClass.getName)
```

___

- **sliding-count-window (有重叠数据)**
    

同样也是窗口长度和滑动窗口的操作：窗口长度是5，滑动长度是3

```
 1//1.创建运行环境
 2val env = StreamExecutionEnvironment.getExecutionEnvironment
 3
 4//2.定义数据流来源
 5val text = env.socketTextStream("localhost", 9999)
 6
 7//3.转换数据格式，text->CarWc
 8case class CarWc(sensorId: Int, carCnt: Int)
 9
10val ds1: DataStream[CarWc] = text.map {
11  (f) => {
12    val tokens = f.split(",")
13    CarWc(tokens(0).trim.toInt, tokens(1).trim.toInt)
14  }
15}
16//4.执行统计操作，每个sensorId一个sliding窗口，窗口大小3条数据,窗口滑动为3条数据
17//也就是说，每个路口分别统计，收到关于它的3条消息时统计在最近5条消息中，各自路口通过的汽车数量
18val ds2: DataStream[CarWc] = ds1
19  .keyBy("sensorId")
20  .countWindow(5, 3)
21  .sum("carCnt")
22
23//5.显示统计结果
24ds2.print()
25
26//6.触发流计算
27env.execute(this.getClass.getName)
```

- **Window 总结**
    

1.  flink支持两种划分窗口的方式（time和count）
    

- 如果根据时间划分窗口，那么它就是一个time-window
    
- 如果根据数据划分窗口，那么它就是一个count-window
    

3.  flink支持窗口的两个重要属性（size和interval）
    

- 如果size=interval,那么就会形成tumbling-window(无重叠数据)
    
- 如果size>interval,那么就会形成sliding-window(有重叠数据)
    
- 如果size<interval,那么这种窗口将会丢失数据。比如每5秒钟，统计过去3秒的通过路口汽车的数据，将会漏掉2秒钟的数据。
    

5.  通过组合可以得出四种基本窗口
    

- time-tumbling-window 无重叠数据的时间窗口，设置方式举例：timeWindow(Time.seconds(5))
    
- time-sliding-window  有重叠数据的时间窗口，设置方式举例：timeWindow(Time.seconds(5), Time.seconds(3))
    
- count-tumbling-window无重叠数据的数量窗口，设置方式举例：countWindow(5)
    
- count-sliding-window 有重叠数据的数量窗口，设置方式举例：countWindow(5,3)
    

#### Window Reduce

WindowedStream → DataStream：给window赋一个reduce功能的函数，并返回一个聚合的结果。

```
 1import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
 2import org.apache.flink.api.scala._
 3import org.apache.flink.streaming.api.windowing.time.Time
 4
 5object StreamWindowReduce {
 6  def main(args: Array[String]): Unit = {
 7    // 获取执行环境
 8    val env = StreamExecutionEnvironment.getExecutionEnvironment
 9
10    // 创建SocketSource
11    val stream = env.socketTextStream("node01", 9999)
12
13    // 对stream进行处理并按key聚合
14    val streamKeyBy = stream.map(item => (item, 1)).keyBy(0)
15
16    // 引入时间窗口
17    val streamWindow = streamKeyBy.timeWindow(Time.seconds(5))
18
19    // 执行聚合操作
20    val streamReduce = streamWindow.reduce(
21      (item1, item2) => (item1._1, item1._2 + item2._2)
22    )
23
24    // 将聚合数据写入文件
25    streamReduce.print()
26
27    // 执行程序
28    env.execute("TumblingWindow")
29  }
30}
```

#### Window Apply

apply方法可以进行一些自定义处理，通过匿名内部类的方法来实现。当有一些复杂计算时使用。

用法

1.  实现一个 WindowFunction 类
    
2.  指定该类的泛型为 \[输入数据类型, 输出数据类型, keyBy中使用分组字段的类型, 窗口类型\]
    

示例：使用apply方法来实现单词统计

步骤：

1.  获取流处理运行环境
    
2.  构建socket流数据源，并指定IP地址和端口号
    
3.  对接收到的数据转换成单词元组
    
4.  使用 keyBy 进行分流（分组）
    
5.  使用 timeWinodw 指定窗口的长度（每3秒计算一次）
    
6.  实现一个WindowFunction匿名内部类
    

- apply方法中实现聚合计算
    
- 使用Collector.collect收集数据
    

核心代码如下：

```
 1    //1. 获取流处理运行环境
 2    val env = StreamExecutionEnvironment.getExecutionEnvironment
 3
 4    //2. 构建socket流数据源，并指定IP地址和端口号
 5    val textDataStream = env.socketTextStream("node01", 9999).flatMap(_.split(" "))
 6
 7    //3. 对接收到的数据转换成单词元组
 8    val wordDataStream = textDataStream.map(_->1)
 9
10    //4. 使用 keyBy 进行分流（分组）
11    val groupedDataStream: KeyedStream[(String, Int), String] = wordDataStream.keyBy(_._1)
12
13    //5. 使用 timeWinodw 指定窗口的长度（每3秒计算一次）
14    val windowDataStream: WindowedStream[(String, Int), String, TimeWindow] = groupedDataStream.timeWindow(Time.seconds(3))
15
16    //6. 实现一个WindowFunction匿名内部类
17    val reduceDatStream: DataStream[(String, Int)] = windowDataStream.apply(new RichWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
18      //在apply方法中实现数据的聚合
19      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
20        println("hello world")
21        val tuple = input.reduce((t1, t2) => {
22          (t1._1, t1._2 + t2._2)
23        })
24        //将要返回的数据收集起来，发送回去
25        out.collect(tuple)
26      }
27    })
28    reduceDatStream.print()
29    env.execute()
```

#### Window Fold

WindowedStream → DataStream：给窗口赋一个fold功能的函数，并返回一个fold后的结果。

```
 1import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
 2import org.apache.flink.api.scala._
 3import org.apache.flink.streaming.api.windowing.time.Time
 4
 5object StreamWindowFold {
 6  def main(args: Array[String]): Unit = {
 7    // 获取执行环境
 8    val env = StreamExecutionEnvironment.getExecutionEnvironment
 9
10    // 创建SocketSource
11    val stream = env.socketTextStream("node01", 9999,'\n',3)
12
13    // 对stream进行处理并按key聚合
14    val streamKeyBy = stream.map(item => (item, 1)).keyBy(0)
15
16    // 引入滚动窗口
17    val streamWindow = streamKeyBy.timeWindow(Time.seconds(5))
18
19    // 执行fold操作
20    val streamFold = streamWindow.fold(100){
21      (begin, item) =>
22        begin + item._2
23    }
24
25    // 将聚合数据写入文件
26    streamFold.print()
27
28    // 执行程序
29    env.execute("TumblingWindow")
30  }
31}
```

#### Aggregation on Window

WindowedStream → DataStream：对一个window内的所有元素做聚合操作。min和 minBy的区别是min返回的是最小值，而minBy返回的是包含最小值字段的元素(同样的原理适用于 max 和 maxBy)。

```
 1import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
 2import org.apache.flink.streaming.api.windowing.time.Time
 3import org.apache.flink.api.scala._
 4
 5object StreamWindowAggregation {
 6  def main(args: Array[String]): Unit = {
 7    // 获取执行环境
 8    val env = StreamExecutionEnvironment.getExecutionEnvironment
 9
10    // 创建SocketSource
11    val stream = env.socketTextStream("node01", 9999)
12
13    // 对stream进行处理并按key聚合
14    val streamKeyBy = stream.map(item => (item.split(" ")(0), item.split(" ")(1))).keyBy(0)
15
16    // 引入滚动窗口
17    val streamWindow = streamKeyBy.timeWindow(Time.seconds(5))
18
19    // 执行聚合操作
20    val streamMax = streamWindow.max(1)
21
22    // 将聚合数据写入文件
23    streamMax.print()
24
25    // 执行程序
26    env.execute("TumblingWindow")
27  }
28}
```

## EventTime与Window

### EventTime的引入

1.  与现实世界中的时间是不一致的，在flink中被划分为事件时间，提取时间，处理时间三种。
    
2.  如果以EventTime为基准来定义时间窗口那将形成EventTimeWindow,要求消息本身就应该携带EventTime
    
3.  如果以IngesingtTime为基准来定义时间窗口那将形成IngestingTimeWindow,以source的systemTime为准。
    
4.  如果以ProcessingTime基准来定义时间窗口那将形成ProcessingTimeWindow，以operator的systemTime为准。
    

在Flink的流式处理中，绝大部分的业务都会使用eventTime，一般只在eventTime无法使用时，才会被迫使用ProcessingTime或者IngestionTime。

如果要使用EventTime，那么需要引入EventTime的时间属性，引入方式如下所示：

```
1val env = StreamExecutionEnvironment.getExecutionEnvironment
2
3// 从调用时刻开始给env创建的每一个stream追加时间特征
4env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
```

### Watermark

#### 引入

我们知道，流处理从事件产生，到流经 source，再到 operator，中间是有一个过程和时间的，虽然大部分情况下，流到 operator 的数据都是按照事件产生的时间顺序来的，但是也不排除由于网络、背压等原因，导致乱序的产生，所谓乱序，就是指 Flink 接收到的事件的先后顺序不是严格按照事件的 Event Time 顺序排列的，所以 Flink 最初设计的时候，就考虑到了网络延迟，网络乱序等问题，所以提出了一个抽象概念：水印（WaterMark）；

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img41.png)

如上图所示，就出现一个问题，一旦出现乱序，如果只根据 EventTime 决定 Window 的运行，我们不能明确数据是否全部到位，但又不能无限期的等下去，此时必须要有个机制来保证一个特定的时间后，必须触发 Window 去进行计算了，这个特别的机制，就是 Watermark。

**Watermark 是用于处理乱序事件的，而正确的处理乱序事件，通常用 Watermark 机制结合 Window 来实现**。

**数据流中的 Watermark 用于表示 timestamp 小于 Watermark 的数据，都已经到达了，因此，Window 的执行也是由 Watermark 触发的。**

**Watermark 可以理解成一个延迟触发机制，我们可以设置 Watermark 的延时时长 t，每次系统会校验已经到达的数据中最大的 maxEventTime，然后认定 EventTime 小于 maxEventTime - t 的所有数据都已经到达，如果有窗口的停止时间等于 maxEventTime – t，那么这个窗口被触发执行**。

有序流的Watermarker如下图所示：（Watermark设置为0）

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img46.png)
有序数据的Watermark

乱序流的Watermarker如下图所示：（Watermark设置为2）

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img47.png)
无序数据的Watermark

**当 Flink 接收到每一条数据时，都会产生一条 Watermark，这条 Watermark 就等于当前所有到达数据中的 maxEventTime - 延迟时长，也就是说，Watermark 是由数据携带的，一旦数据携带的 Watermark 比当前未触发的窗口的停止时间要晚，那么就会触发相应窗口的执行。由于 Watermark 是由数据携带的，因此，如果运行过程中无法获取新的数据，那么没有被触发的窗口将永远都不被触发**。

上图中，我们设置的允许最大延迟到达时间为2s，所以时间戳为7s的事件对应的Watermark是5s，时间戳为12s的事件的Watermark是10s，如果我们的窗口1是1s~5s，窗口2是6s~10s，那么时间戳为7s的事件到达时的Watermarker恰好触发窗口1，时间戳为12s的事件到达时的Watermark恰好触发窗口2。

### Flink对于迟到数据的处理

waterMark和Window机制解决了流式数据的乱序问题，对于因为延迟而顺序有误的数据，可以根据eventTime进行业务处理，于延迟的数据Flink也有自己的解决办法，主要的办法是给定一个允许延迟的时间，在该时间范围内仍可以接受处理延迟数据。

设置允许延迟的时间是通过 **allowedLateness(lateness: Time)** 设置

保存延迟数据则是通过 **sideOutputLateData(outputTag: OutputTag\[T\])** 保存

获取延迟数据是通过 **DataStream.getSideOutput(tag: OutputTag\[X\])** 获取

具体的用法如下：

#### allowedLateness(lateness: Time)

```
1def allowedLateness(lateness: Time): WindowedStream[T, K, W] = {
2  javaStream.allowedLateness(lateness)
3  this
4}
```

该方法传入一个Time值，设置允许数据迟到的时间，这个时间和 WaterMark 中的时间概念不同。再来回顾一下：

WaterMark=数据的事件时间-允许乱序时间值

随着新数据的到来，waterMark的值会更新为最新数据事件时间-允许乱序时间值，但是如果这时候来了一条历史数据，waterMark值则不会更新。总的来说，waterMark是为了能接收到尽可能多的乱序数据。

那这里的Time值，主要是为了等待迟到的数据，在一定时间范围内，如果属于该窗口的数据到来，仍会进行计算，后面会对计算方式仔细说明

注意：该方法只针对于基于event-time的窗口，如果是基于processing-time，并且指定了非零的time值则会抛出异常。

#### sideOutputLateData(outputTag: OutputTag\[T\])

```
1def sideOutputLateData(outputTag: OutputTag[T]): WindowedStream[T, K, W] = {
2  javaStream.sideOutputLateData(outputTag)
3  this
4}
```

该方法是将迟来的数据保存至给定的outputTag参数，而OutputTag则是用来标记延迟数据的一个对象。

#### DataStream.getSideOutput(tag: OutputTag\[X\])

通过window等操作返回的DataStream调用该方法，传入标记延迟数据的对象来获取延迟的数据。

#### 对延迟数据的理解

延迟数据是指：

在当前窗口【假设窗口范围为10-15】已经计算之后，又来了一个属于该窗口的数据【假设事件时间为13】，这时候仍会触发 Window 操作，这种数据就称为延迟数据。

那么问题来了，延迟时间怎么计算呢？

假设窗口范围为10-15，延迟时间为2s，则只要 WaterMark<15+2，并且属于该窗口，就能触发 Window 操作。而如果来了一条数据使得 WaterMark>=15+2，10-15这个窗口就不能再触发 Window 操作，即使新来的数据的 Event Time 属于这个窗口时间内 。

### Flink 关联 Hive 分区表

Flink 1.12 支持了 Hive 最新的分区作为时态表的功能，可以通过 SQL 的方式直接关联 Hive 分区表的最新分区，并且会自动监听最新的 Hive 分区，当监控到新的分区后，会自动地做维表数据的全量替换。通过这种方式，用户无需编写 DataStream 程序即可完成 **Kafka 流实时关联最新的 Hive 分区实现数据打宽**。

具体用法：

在 Sql Client 中注册 HiveCatalog：

```
1vim conf/sql-client-defaults.yaml 
2catalogs: 
3  - name: hive_catalog 
4    type: hive 
5    hive-conf-dir: /disk0/soft/hive-conf/ #该目录需要包hive-site.xml文件 
```

**创建 Kafka 表**

```
 1CREATE TABLE hive_catalog.flink_db.kfk_fact_bill_master_12 (  
 2    master Row<reportDate String, groupID int, shopID int, shopName String, action int, orderStatus int, orderKey String, actionTime bigint, areaName String, paidAmount double, foodAmount double, startTime String, person double, orderSubType int, checkoutTime String>,  
 3proctime as PROCTIME()  -- PROCTIME用来和Hive时态表关联  
 4) WITH (  
 5 'connector' = 'kafka',  
 6 'topic' = 'topic_name',  
 7 'format' = 'json',  
 8 'properties.bootstrap.servers' = 'host:9092',  
 9 'properties.group.id' = 'flinkTestGroup',  
10 'scan.startup.mode' = 'timestamp',  
11 'scan.startup.timestamp-millis' = '1607844694000'  
12); 
```

**Flink 事实表与 Hive 最新分区数据关联**

dim_extend_shop_info 是 Hive 中已存在的表，所以我们用 table hint 动态地开启维表参数。

```
 1CREATE VIEW IF NOT EXISTS hive_catalog.flink_db.view_fact_bill_master as  
 2SELECT * FROM  
 3 (select t1.*, t2.group_id, t2.shop_id, t2.group_name, t2.shop_name, t2.brand_id,   
 4     ROW_NUMBER() OVER (PARTITION BY groupID, shopID, orderKey ORDER BY actionTime desc) rn  
 5    from hive_catalog.flink_db.kfk_fact_bill_master_12 t1  
 6       JOIN hive_catalog.flink_db.dim_extend_shop_info   
 7  /*+ OPTIONS('streaming-source.enable'='true',  
 8     'streaming-source.partition.include' = 'latest',  
 9     'streaming-source.monitor-interval' = '1 h',
10     'streaming-source.partition-order' = 'partition-name') */
11    FOR SYSTEM_TIME AS OF t1.proctime AS t2 --时态表  
12    ON t1.groupID = t2.group_id and t1.shopID = t2.shop_id  
13    where groupID in (202042)) t  where t.rn = 1 
```

参数解释：

- **streaming-source.enable** 开启流式读取 Hive 数据。
    
- **streaming-source.partition.include** 有以下两种值：
    

1.  latest 属性: 只读取最新分区数据。
    
2.  all: 读取全量分区数据 ，默认值为 all，表示读所有分区，latest 只能用在 temporal join 中，用于读取最新分区作为维表，不能直接读取最新分区数据。
    

- **streaming-source.monitor-interval** 监听新分区生成的时间、不宜过短 、最短是1 个小时，因为目前的实现是每个 task 都会查询 metastore，高频的查可能会对metastore 产生过大的压力。需要注意的是，1.12.1 放开了这个限制，但仍建议按照实际业务不要配个太短的 interval。
    
- **streaming-source.partition-order** 分区策略，主要有以下 3 种，其中最为推荐的是 **partition-name**：
    

1.  partition-name 使用默认分区名称顺序加载最新分区
    
2.  create-time 使用分区文件创建时间顺序
    
3.  partition-time 使用分区时间顺序
    
