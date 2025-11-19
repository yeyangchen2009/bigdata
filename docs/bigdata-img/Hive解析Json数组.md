# Hive解析Json数组

在Hive中会有很多数据是用Json格式来存储的，如开发人员对APP上的页面进行埋点时，会将多个字段存放在一个json数组中，因此数据平台调用数据时，要对埋点数据进行解析。接下来就聊聊Hive中是如何解析json数据的。

## Hive自带的json解析函数

### 1\. get_json_object

-   语法：`get_json_object(json_string, '$.key')`
    
-   说明：解析json的字符串json_string,返回path指定的内容。如果输入的json字符串无效，那么返回NULL。**这个函数每次只能返回一个数据项。**
    
-   示例：
    

```
select 
get_json_object('{"name":"zhangsan","age":18}','$.name'); 
```

-   结果：
    

|name|
|---|
|zhangsan|

如果既要解析name字段，也解析age字段，则可以这样写：

```
select 
get_json_object('{"name":"zhangsan","age":18}','$.name'),
get_json_object('{"name":"zhangsan","age":18}','$.age');
```

但是如果要解析的字段有很多，再这样写就太麻烦了，所以就有了 `json_tuple` 这个函数。

### 2\. json_tuple

-   语法：`json_tuple(json_string, k1, k2 ...)`
    
-   说明：解析json的字符串json_string,可指定多个json数据中的key，返回对应的value。如果输入的json字符串无效，那么返回NULL。
    
-   示例：
    

```
select 
b.name
,b.age
from tableName a lateral view
json_tuple('{"name":"zhangsan","age":18}','name','age') b as name,age;
```

-   结果:
    

|name|age|
|---|---|
|zhangsan|18|

**注意**：上面的`json_tuple`函数中没有$.

如果在使用`json_tuple`函数时加上$.就会解析失败：

```
select 
b.name
,b.age
from tableName a lateral view
json_tuple('{"name":"zhangsan","age":18}','$.name','$.age') b as name,age;
```

结果:

|name|age|
|---|---|
|NULL|NULL|

字段全是NULL，所以`json_tuple`函数不需要加$.了，否则会解析不到。

总结：json_tuple相当于get_json_object的优势就是一次可以解析多个json字段。但是如果我们有个json数组，这两个函数都无法处理。

## Hive解析json数组

#### 一、嵌套子查询解析json数组

如果有一个hive表，表中 json_str 字段的内容如下：

|json_str|
|---|
|\[{"website":"baidu.com","name":"百度"},{"website":"google.com","name":"谷歌"}\]|

我们想把这个字段解析出来，形成如下的结构：

|website|name|
|---|---|
|baidu.com|百度|
|google.com|谷歌|

要解析这个json数组，仅用上面介绍的两个函数就解析不出来了，还需用到如下介绍的几个函数：

### explode函数

-   语法：`explode(Array OR Map)`
    
-   说明：explode()函数接收一个array或者map类型的数据作为输入，然后将array或map里面的元素按照每行的形式输出，即将hive一列中复杂的array或者map结构拆分成多行显示，也被称为列转行函数。
    
-   示例：
    

```
-- 解析array
hive> select explode(array('A','B','C'));
OK
A
B
C
-- 解析map
hive> select explode(map('A',10,'B',20,'C',30));
OK
A       10
B       20
C       30
```

### regexp_replace函数

-   语法: regexp_replace(string A, string B, string C)
    
-   说明：将字符串A中的符合java正则表达式B的部分替换为C。注意，在有些情况下要使用转义字符，类似oracle中的regexp_replace函数。
    
-   示例：
    

```
hive> select regexp_replace('foobar', 'oo|ar', ''); 
OK
fb
```

上述示例将字符串中的 oo 或 ar 替换为''。

___

有了上述几个函数，接下来我们来解析json_str字段的内容：

1.  先将json数组中的元素解析出来，转化为每行显示：
    

```
hive> SELECT explode(split(regexp_replace(regexp_replace('[{"website":"baidu.com","name":"百度"},{"website":"google.com","name":"谷歌"}]', '\\[|\\]',''),'\\}\\,\\{','\\}\\;\\{'),'\\;'));
OK
{"website":"baidu.com","name":"百度"}
{"website":"google.com","name":"谷歌"}
```

对上述sql进行简要说明：

```
SELECT explode(split(
    regexp_replace(
        regexp_replace(
            '[
                {"website":"baidu.com","name":"百度"},
                {"website":"google.com","name":"谷歌"}
            ]', 
            '\\[|\\]' , ''), 将json数组两边的中括号去掉
            
              '\\}\\,\\{' , '\\}\\;\\{'), 将json数组元素之间的逗号换成分号
                
                 '\\;') 以分号作为分隔符(split函数以分号作为分隔)
          );  
```

> 为什么要将json数组元素之间的逗号换成分号？
> 
> 因为**元素内**的分隔也是逗号，如果不将**元素之间**的逗号换掉的话，后面用split函数分隔时也会把元素内的数据给分隔，这不是我们想要的结果。

2.  上步已经把一个json数组转化为多个json字符串了，接下来结合son_tuple函数来解析json里面的字段：
    

```
select 
json_tuple(explode(split(
regexp_replace(regexp_replace('[{"website":"baidu.com","name":"百度"},{"website":"google.com","name":"谷歌"}]', '\\[|\\]',''),'\\}\\,\\{','\\}\\;\\{'),'\\;')) 
, 'website', 'name') ;
```

执行上述语句，结果报错了：

`FAILED: SemanticException [Error 10081]: UDTF's are not supported outside the SELECT clause, nor nested in expressions`

意思是**UDTF函数不能写在别的函数内，也就是这里的explode函数不能写在json_tuple里面**。

既然explode函数不能写在别的json_tuple里面，那我们可以用**子查询**方式，如下所示：

```
select json_tuple(json, 'website', 'name') 
from (
select explode(split(regexp_replace(regexp_replace('[{"website":"baidu.com","name":"百度"},{"website":"google.com","name":"谷歌"}]', '\\[|\\]',''),'\\}\\,\\{','\\}\\;\\{'),'\\;')) 
as json) t;
```

执行上述语句，没有报错，执行结果如下：

```
www.baidu.com   百度
google.com      谷歌
```

#### 二 使用 lateral view 解析json数组

hive表中 goods_id 和 json_str 字段的内容如下：

|goods_id|json_str|
|---|---|
|1,2,3|\[{"source":"7fresh","monthSales":4900,"userCount":1900,"score":"9.9"},{"source":"jd","monthSales":2090,"userCount":78981,"score":"9.8"},{"source":"jdmart","monthSales":6987,"userCount":1600,"score":"9.0"}\]|

**目的**：把 goods_id 字段和 json_str 字段中的monthSales解析出来。

下面我们就开始解析：

拆分goods_id字段及将json数组转化成多个json字符串：

```
select 
explode(split(goods_id,',')) as good_id,
explode(split(regexp_replace(regexp_replace(json_str , '\\[|\\]',''),'\\}\\,\\{','\\}\\;\\{'),'\\;')) 
as sale_info 
from tableName;
```

执行上述语句，结果报错：

`FAILED: SemanticException 3:0 Only a single expression in the SELECT clause is supported with UDTF's. Error encountered near token 'sale_info'`

意思是**用UDTF的时候，SELECT 只支持一个字段**。而上述语句select中有两个字段，所以报错了。

那怎么办呢，要解决这个问题，还得再介绍一个hive语法：

### lateral view

lateral view用于和split、explode等UDTF一起使用的，能将一行数据拆分成多行数据，在此基础上可以对拆分的数据进行聚合，lateral view首先为原始表的每行调用UDTF，UDTF会把一行拆分成一行或者多行，lateral view在把结果组合，产生一个支持别名表的虚拟表。

-   示例：
    

假设我们有一张用户兴趣爱好表 hobbies_table，它有两列数据，第一列是name，第二列是用户兴趣爱好的id_list，是一个数组，存储兴趣爱好的id值：

|name|id_list|
|---|---|
|zhangsan|\[1,2,3\]|
|lisi|\[3,4,5\]|

**我们要统计所有兴趣id在所有用户中出现的次数**：

1.  对兴趣id进行解析：
    

```
SELECT name, hobby_id 
FROM hobbies_table 
LATERAL VIEW explode(id_list) tmp_table AS hobby_id;
```

上述sql执行结果：

|name|hobby_id|
|---|---|
|zhangsan|1|
|zhangsan|2|
|zhangsan|3|
|lisi|3|
|lisi|4|
|lisi|5|

2\. 按照hobby_id进行分组聚合即可：

```
SELECT hobby_id ,count(name) client_num
FROM hobbies_table 
LATERAL VIEW explode(id_list) tmp_table AS hobby_id
GROUP BY hobby_id;
```

结果：

|hobby_id|client_num|
|---|---|
|1|1|
|2|1|
|3|2|
|4|1|
|5|1|

___

介绍完 `lateral view` 之后，我们再来解决上面遇到的**用UDTF的时候，SELECT 只支持一个字段**的问题：

```
select good_id,get_json_object(sale_json,'$.monthSales') as monthSales
from tableName 
LATERAL VIEW explode(split(goods_id,','))goods as good_id 
LATERAL VIEW explode(split(regexp_replace(regexp_replace(json_str , '\\[|\\]',''),'\\}\\,\\{','\\}\\;\\{'),'\\;')) sales as sale_json;
```

> **注意**：上述语句是三个表**笛卡尔积**的结果，所以此方式适用于数据量不是很大的情况。

上述语句执行结果如下：

|goods_id|monthSales|
|---|---|
|1|4900|
|1|2090|
|1|6987|
|2|4900|
|2|2090|
|2|6987|
|3|4900|
|3|2090|
|3|6987|

如果表中还有其他字段，我们可以根据其他字段筛选出符合结果的数据。

**总结：lateral view通常和UDTF一起出现，为了解决UDTF不允许在select存在多个字段的问题**。

