# 以直播平台监控用户弹幕为例详解 Flink CEP

我们在看直播的时候，不管对于主播还是用户来说，非常重要的一项就是弹幕文化。为了增加直播趣味性和互动性, 各大网络直播平台纷纷采用弹窗弹幕作为用户**实时交流**的方式，内容丰富且形式多样的弹幕数据中隐含着复杂的用户属性与用户行为, 研究并理解在线直播平台用户具有弹幕内容审核与监控、舆论热点预测、个性化摘要标注等多方面的应用价值。

本文不分析弹幕数据的应用价值，只通过弹幕内容审核与监控案例来了解下Flink CEP的概念及功能。

在用户发弹幕时，**直播平台主要实时监控识别两类弹幕内容**：一类是发布不友善弹幕的用户 ；一类是刷屏的用户。

我们先记住上述需要**实时监控识别**的两类用户，接下来介绍Flink CEP的API，然后使用CEP解决上述问题。

## Flink CEP

### Flink CEP 是什么

Flink CEP是一个基于Flink的复杂事件处理库，可以从多个数据流中发现复杂事件，识别有意义的事件（例如机会或者威胁），并尽快的做出响应，而不是需要等待几天或则几个月相当长的时间，才发现问题。

### Flink CEP API

CEP API的核心是Pattern(模式) API，它允许你快速定义复杂的事件模式。每个模式包含多个阶段（stage）或者我们也可称为状态（state）。从一个状态切换到另一个状态，用户可以指定条件，这些条件可以作用在邻近的事件或独立事件上。

介绍API之前先来理解几个概念：

#### 1. 模式与模式序列

- 简单模式称为模式，将最终在数据流中进行搜索匹配的复杂模式序列称为模式序列，每个复杂模式序列是由多个简单模式组成。
    
- 匹配到的一系列输入事件，这些事件通过一系列有效的模式转换，能够访问复杂模式图的所有模式。
    
- 每个模式必须具有唯一的名称，我们可以使用模式名称来标识该模式匹配到的事件。
    

#### 2. 单个模式

一个模式既可以是单例的，也可以是循环的。**单例模式接受单个事件，循环模式可以接受多个事件**。

#### 3. 模式示例：

有如下模式：`a b+ c？d`

其中`a,b,c,d`这些字母代表的是模式，`+`代表循环，`b+`就是循环模式；`?`代表可选，`c?`就是可选模式；

所以上述模式的意思就是：`a`后面可以跟一个或多个`b`，后面再可选的跟`c`，最后跟`d`。

其中`a、c? 、d`是单例模式，`b+`是循环模式。

一般情况下，模式都是单例模式，可以使用量词（Quantifiers）将其转换为循环模式。

每个模式可以带有一个或多个条件，这些条件是基于事件接收进行定义的。或者说，每个模式通过一个或多个条件来匹配和接收事件。

了解完上述概念后，接下来介绍下案例中需要用到的几个CEP API：

#### 案例中用到的CEP API：

- Begin：定义一个起始模式状态
    
    用法：`start = Pattern.<Event>begin("start");`
    
- Next：附加一个新的模式状态。匹配事件必须直接接续上一个匹配事件
    
    用法：`next = start.next("next");`
    
- Where：定义当前模式状态的过滤条件。仅当事件通过过滤器时，它才能与状态匹配
    
    用法：`patternState.where(_.message == "yyds");`
    
- Within: 定义事件序列与模式匹配的最大时间间隔。如果未完成的事件序列超过此时间，则将其丢弃
    
    用法：`patternState.within(Time.seconds(10));`
    
- Times：一个给定类型的事件出现了指定次数
    
    用法：`patternState.times(5);`
    

API 先介绍以上这几个，接下来我们解决下文章开头提到的案例：

### 监测用户弹幕行为案例

#### 案例一：监测恶意用户

规则：**用户如果在10s内，同时输入 TMD 超过5次，就认为用户为恶意攻击，识别出该用户**。

使用 Flink CEP 检测恶意用户：

```
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object BarrageBehavior01 {
  case class  LoginEvent(userId:String, message:String, timestamp:Long){
    override def toString: String = userId
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 使用IngestionTime作为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 用于观察测试数据处理顺序
    env.setParallelism(1)

    // 模拟数据源
    val loginEventStream: DataStream[LoginEvent] = env.fromCollection(
      List(
        LoginEvent("1", "TMD", 1618498576),
        LoginEvent("1", "TMD", 1618498577),
        LoginEvent("1", "TMD", 1618498579),
        LoginEvent("1", "TMD", 1618498582),
        LoginEvent("2", "TMD", 1618498583), 
        LoginEvent("1", "TMD", 1618498585)
      )
    ).assignAscendingTimestamps(_.timestamp * 1000)

    //定义模式
    val loginEventPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.message == "TMD")
      .times(5)
      .within(Time.seconds(10))

    //匹配模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginEventPattern)

    import scala.collection.Map
    val result = patternStream.select((pattern:Map[String, Iterable[LoginEvent]])=> {
      val first = pattern.getOrElse("begin", null).iterator.next()
      (first.userId, first.timestamp)
    })
    //恶意用户，实际处理可将按用户进行禁言等处理，为简化此处仅打印出该用户
    result.print("恶意用户>>>")
    env.execute("BarrageBehavior01")
  }
}
```

#### 案例二：监测刷屏用户

规则：**用户如果在10s内，同时连续输入同样一句话超过5次，就认为是恶意刷屏**。

使用 Flink CEP检测刷屏用户

```
object BarrageBehavior02 {
  case class Message(userId: String, ip: String, msg: String)

  def main(args: Array[String]): Unit = {
    //初始化运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    // 模拟数据源
    val loginEventStream: DataStream[Message] = env.fromCollection(
      List(
        Message("1", "192.168.0.1", "beijing"),
        Message("1", "192.168.0.2", "beijing"),
        Message("1", "192.168.0.3", "beijing"),
        Message("1", "192.168.0.4", "beijing"),
        Message("2", "192.168.10.10", "shanghai"),
        Message("3", "192.168.10.10", "beijing"),
        Message("3", "192.168.10.11", "beijing"),
        Message("4", "192.168.10.10", "beijing"),
        Message("5", "192.168.10.11", "shanghai"),
        Message("4", "192.168.10.12", "beijing"),
        Message("5", "192.168.10.13", "shanghai"),
        Message("5", "192.168.10.14", "shanghai"),
        Message("5", "192.168.10.15", "beijing"),
        Message("6", "192.168.10.16", "beijing"),
        Message("6", "192.168.10.17", "beijing"),
        Message("6", "192.168.10.18", "beijing"),
        Message("5", "192.168.10.18", "shanghai"),
        Message("6", "192.168.10.19", "beijing"),
        Message("6", "192.168.10.19", "beijing"),
        Message("5", "192.168.10.18", "shanghai")
      )
    )

    //定义模式
    val loginbeijingPattern = Pattern.begin[Message]("start")
      .where(_.msg != null) //一条登录失败
      .times(5).optional  //将满足五次的数据配对打印
      .within(Time.seconds(10))

    //进行分组匹配
    val loginbeijingDataPattern = CEP.pattern(loginEventStream.keyBy(_.userId), loginbeijingPattern)

    //查找符合规则的数据
    val loginbeijingResult: DataStream[Option[Iterable[Message]]] = loginbeijingDataPattern.select(patternSelectFun = (pattern: collection.Map[String, Iterable[Message]]) => {
      var loginEventList: Option[Iterable[Message]] = null
      loginEventList = pattern.get("start") match {
        case Some(value) => {
          if (value.toList.map(x => (x.userId, x.msg)).distinct.size == 1) {
            Some(value)
          } else {
            None
          }
        }
      }
      loginEventList
    })

    //打印测试
    loginbeijingResult.filter(x=>x!=None).map(x=>{
      x match {
        case Some(value)=> value
      }
    }).print()

    env.execute("BarrageBehavior02)
  }
}
```

## Flink CEP API

除了案例中介绍的几个API外，我们在介绍下其他的常用API：

#### 1. 条件 API

为了让传入事件被模式所接受，给模式指定传入事件必须满足的条件，这些条件由事件本身的属性或者前面匹配过的事件的属性统计量等来设定。比如，事件的某个值大于5，或者大于先前接受事件的某个值的平均值。

可以使用`pattern.where()、pattern.or()、pattern.until()`方法来指定条件。条件既可以是迭代条件`IterativeConditions`，也可以是简单条件`SimpleConditions`。

FlinkCEP支持事件之间的三种临近条件：

- **next()**：严格的满足条件
    
    示例：模式为`begin("first").where(_.name='a').next("second").where(.name='b')`当且仅当数据为a,b时，模式才会被命中。如果数据为a,c,b，由于a的后面跟了c，所以a会被直接丢弃，模式不会命中。
    
- **followedBy()**：松散的满足条件
    
    示例：模式为`begin("first").where(_.name='a').followedBy("second").where(.name='b')`当且仅当数据为a,b或者为a,c,b，模式均被命中，中间的c会被忽略掉。
    
- **followedByAny()**：非确定的松散满足条件
    
    示例：模式为`begin("first").where(_.name='a').followedByAny("second").where(.name='b')`当且仅当数据为`a,c,b,b`时，对于followedBy模式而言命中的为{a,b}，对于followedByAny而言会有两次命中{a,b},{a,b}。
    

#### 2. 量词 API

还记得我们在上面讲解模式概念时说过的一句话：一般情况下，模式都是单例模式，可以使用量词（Quantifiers）将其转换为循环模式。这里的量词就是指的量词API。

以下这几个量词API，可以将模式指定为循环模式：

- `pattern.oneOrMore()`：一个给定的事件有一次或多次出现，例如上面提到的b+。
    
- `pattern.times(#ofTimes)`：一个给定类型的事件出现了指定次数，例如4次。
    
- `pattern.times(#fromTimes, #toTimes)`：一个给定类型的事件出现的次数在指定次数范围内，例如2~4次。
    
- 可以使用`pattern.greedy()`方法将模式变成**循环模式**，但是不能让一组模式都变成循环模式。greedy：就是尽可能的重复。
    
- 使用`pattern.optional()`方法将循环模式变成**可选的**，即可以是循环模式也可以是单个模式。
    

#### 3. 匹配后的跳过策略

所谓的匹配跳过策略，是对多个成功匹配的模式进行筛选。也就是说如果多个匹配成功，可能我不需要这么多，按照匹配策略，过滤下就可以。

Flink中有五种跳过策略：

- **NO_SKIP**: 不过滤，所有可能的匹配都会被发出。
    
- **SKIP_TO_NEXT**: 丢弃与开始匹配到的事件相同的事件，发出开始匹配到的事件，即直接跳到下一个模式匹配到的事件，以此类推。
    
- **SKIP_PAST_LAST_EVENT**: 丢弃匹配开始后但结束之前匹配到的事件。
    
- **SKIP_TO_FIRST\[PatternName\]**: 丢弃匹配开始后但在PatternName模式匹配到的第一个事件之前匹配到的事件。
    
- **SKIP_TO_LAST\[PatternName\]**: 丢弃匹配开始后但在PatternName模式匹配到的最后一个事件之前匹配到的事件。
    

怎么理解上述策略，我们以**NO_SKIP**和**SKIP_PAST_LAST_EVENT**为例讲解下：

在模式为：`begin("start").where(_.name='a').oneOrMore().followedBy("second").where(_.name='b')`中，我们输入数据：`a,a,a,a,b` ，如果是NO_SKIP策略，即不过滤策略，模式匹配到的是:{a,b},{a,a,b},{a,a,a,b},{a,a,a,a,b}；如果是SKIP_PAST_LAST_EVENT策略，即丢弃匹配开始后但结束之前匹配到的事件，模式匹配到的是:{a,a,a,a,b}。

## Flink CEP 的使用场景

除上述案例场景外，Flink CEP 还广泛用于网络欺诈，故障检测，风险规避，智能营销等领域。

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img50.png)

#### 1. 实时反作弊和风控

对于电商来说，羊毛党是必不可少的，国内拼多多曾爆出 100 元的无门槛券随便领，当晚被人褥几百亿，对于这种情况肯定是没有做好及时的风控。另外还有就是商家上架商品时通过频繁修改商品的名称和滥用标题来提高搜索关键字的排名、批量注册一批机器账号快速刷单来提高商品的销售量等作弊行为，各种各样的作弊手法也是需要不断的去制定规则去匹配这种行为。

#### 2. 实时营销

分析用户在手机 APP 的实时行为，统计用户的活动周期，通过为用户画像来给用户进行推荐。比如用户在登录 APP 后 1 分钟内只浏览了商品没有下单；用户在浏览一个商品后，3 分钟内又去查看其他同类的商品，进行比价行为；用户商品下单后 1 分钟内是否支付了该订单。如果这些数据都可以很好的利用起来，那么就可以给用户推荐浏览过的类似商品，这样可以大大提高购买率。

#### 3. 实时网络攻击检测

当下互联网安全形势仍然严峻，网络攻击屡见不鲜且花样众多，这里我们以 DDOS（分布式拒绝服务攻击）产生的流入流量来作为遭受攻击的判断依据。对网络遭受的潜在攻击进行实时检测并给出预警，云服务厂商的多个数据中心会定时向监控中心上报其瞬时流量，如果流量在预设的正常范围内则认为是正常现象，不做任何操作；如果某数据中心在 10 秒内连续 5 次上报的流量超过正常范围的阈值，则触发一条警告的事件；如果某数据中心 30 秒内连续出现 30 次上报的流量超过正常范围的阈值，则触发严重的告警。

## Flink CEP 的原理简单介绍

Apache Flink在实现CEP时借鉴了`Efficient Pattern Matching over Event Streams`论文中NFA的模型，在这篇论文中，还提到了一些优化，我们在这里先跳过，只说下NFA的概念。

在这篇论文中，提到了NFA，也就是Non-determined Finite Automaton，叫做**不确定的有限状态机**，指的是状态有限，但是每个状态可能被转换成多个状态（不确定）。

非确定有限自动状态机：

![图片](https://raw.githubusercontent.com/yeyangchen2009/img_bed/master/bigdata/image/img49.png)

先介绍两个概念：

- **状态**：状态分为三类，起始状态、中间状态和最终状态。
    
- **转换**：take/ignore/proceed都是转换的名称。
    

在NFA匹配规则里，本质上是一个状态转换的过程。三种转换的含义如下所示：

- **Take:** 主要是条件的判断，当过来一条数据进行判断，一旦满足条件，获取当前元素，放入到结果集中，然后将当前状态转移到下一个的状态。
    
- **Proceed**：当前的状态可以不依赖任何的事件转移到下一个状态，比如说透传的意思。
    
- **Ignore**：当一条数据到来的时候，可以忽略这个消息事件，当前的状态保持不变，相当于自己到自己的一个状态。
    

**NFA的特点**：在NFA中，给定当前状态，可能有多个下一个状态。可以随机选择下一个状态，也可以并行（同时）选择下一个状态。输入符号可以为空。

## 规则引擎

> 规则引擎：将业务决策从应用程序代码中分离出来，并使用预定义的语义模块编写业务决策。接受数据输入，解释业务规则，并根据业务规则做出业务决策。
> 
> 使用规则引擎可以通过降低实现复杂业务逻辑的组件的复杂性，降低应用程序的维护和可扩展性成本。

#### 1. Drools

Drools 是一款使用 Java 编写的开源规则引擎，通常用来解决业务代码与业务规则的分离，它内置的 Drools Fusion 模块也提供 CEP 的功能。

优势：

- 功能较为完善，具有如系统监控、操作平台等功能。
    
- 规则支持动态更新。
    

劣势：

- 以内存实现时间窗功能，无法支持较长跨度的时间窗。
    
- 无法有效支持定时触达（如用户在浏览发生一段时间后触达条件判断）。
    

#### 2. Aviator

Aviator 是一个高性能、轻量级的 Java 语言实现的表达式求值引擎，主要用于各种表达式的动态求值。

优势：

- 支持大部分运算操作符。
    
- 支持函数调用和自定义函数。
    
- 支持正则表达式匹配。
    
- 支持传入变量并且性能优秀。
    

劣势：

- 没有 if else、do while 等语句，没有赋值语句，没有位运算符。
    

#### 3. EasyRules

EasyRules 集成了 MVEL 和 SpEL 表达式的一款轻量级规则引擎。

优势：

- 轻量级框架，学习成本低。
    
- 基于 POJO。
    
- 为定义业务引擎提供有用的抽象和简便的应用。
    
- 支持从简单的规则组建成复杂规则。
    

#### 4. Esper

Esper 设计目标为 CEP 的轻量级解决方案，可以方便的嵌入服务中，提供 CEP 功能。

优势：

- 轻量级可嵌入开发，常用的 CEP 功能简单好用。
    
- EPL 语法与 SQL 类似，学习成本较低。
    

劣势：

- 单机全内存方案，需要整合其他分布式和存储。
    
- 以内存实现时间窗功能，无法支持较长跨度的时间窗。
    
- 无法有效支持定时触达（如用户在浏览发生一段时间后触达条件判断）。
    

#### 5. Flink CEP

Flink 是一个流式系统，具有高吞吐低延迟的特点，Flink CEP 是一套极具通用性、易于使用的实时流式事件处理方案。

优势：

- 继承了 Flink 高吞吐的特点。
    
- 事件支持存储到外部，可以支持较长跨度的时间窗。
    
- 可以支持定时触达（用 followedBy ＋ PartternTimeoutFunction 实现）。
    
