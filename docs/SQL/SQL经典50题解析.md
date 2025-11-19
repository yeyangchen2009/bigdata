
> [sql 经典50题--可能是你见过的最全解析-知乎](https://zhuanlan.zhihu.com/p/72223558)

## 引言

SQL经典50题网上有很多人分享过，但是，只有自己实际操作过以后才能真正的理解其中的坑，也才能够加深对知识的理解。

解析包含：1解题思路，2相关知识，3 实际语句实现；

首先建表，主要有四个表，学生表（Student），课程表（Course），教师表（Teacher），以及成绩表（SC）

在分别介绍一下每个表的字段

- 学生表（Student ）有四个字段 sid--学生id，sname--学生姓名，sage--学生年龄，ssex--学生性别

- 课程表（Course）有三个字段，cid--课程id，cname--课程名，tid--教师id

- 教师表（Teacher）有两个字段，tid--教师id，tname--教师姓名

- 成绩表（SC）有三个字段，sid--学生id，cid--课程id，score--成绩

我们用脑图的方式绘制出来，如下所示

## 表结构

学生表结构

![](https://pic1.zhimg.com/v2-28a986c5f7c3824fb2f065b6238f7476_1440w.jpg)

课程表结构

![](https://picx.zhimg.com/v2-9a321dd657cd8c1a52f16ec4c7c21e93_1440w.jpg)

教师表结构

![](https://picx.zhimg.com/v2-d74a7aba28331eeaddb11d65e592d4af_1440w.jpg)

成绩表结构

![](https://pic3.zhimg.com/v2-810f8fbd7a9cbebc5dae56cf57f4628c_1440w.jpg)

各个表之间的关系如下，Student和SC表通过学生id（sid）来连接，Course和Teacher表通过教师id（tid）连接，SC和Course表通过课程id（cid）来连接。

```sql
create table Student(sid varchar(10),sname varchar(10),sage datetime,ssex nvarchar(10));
insert into Student values('01' , '赵雷' , '1990-01-01' , '男');
insert into Student values('02' , '钱电' , '1990-12-21' , '男');
insert into Student values('03' , '孙风' , '1990-05-20' , '男');
insert into Student values('04' , '李云' , '1990-08-06' , '男');
insert into Student values('05' , '周梅' , '1991-12-01' , '女');
insert into Student values('06' , '吴兰' , '1992-03-01' , '女');
insert into Student values('07' , '郑竹' , '1989-07-01' , '女');
insert into Student values('08' , '王菊' , '1990-01-20' , '女');
create table Course(cid varchar(10),cname varchar(10),tid varchar(10));
insert into Course values('01' , '语文' , '02');
insert into Course values('02' , '数学' , '01');
insert into Course values('03' , '英语' , '03');
create table Teacher(tid varchar(10),tname varchar(10));
insert into Teacher values('01' , '张三');
insert into Teacher values('02' , '李四');
insert into Teacher values('03' , '王五');
create table SC(sid varchar(10),cid varchar(10),score decimal(18,1));
insert into SC values('01' , '01' , 80);
insert into SC values('01' , '02' , 90);
insert into SC values('01' , '03' , 99);
insert into SC values('02' , '01' , 70);
insert into SC values('02' , '02' , 60);
insert into SC values('02' , '03' , 80);
insert into SC values('03' , '01' , 80);
insert into SC values('03' , '02' , 80);
insert into SC values('03' , '03' , 80);
insert into SC values('04' , '01' , 50);
insert into SC values('04' , '02' , 30);
insert into SC values('04' , '03' , 20);
insert into SC values('05' , '01' , 76);
insert into SC values('05' , '02' , 87);
insert into SC values('06' , '01' , 31);
insert into SC values('06' , '03' , 34);
insert into SC values('07' , '02' , 89);
insert into SC values('07' , '03' , 98);
```

## 1.查询"01"课程比"02"课程成绩高的学生的信息及课程分数

解题思路：要查询的是两个课程的成绩，而且还要显示学生的信息。所以需要用到两张表，SC，Student这两张表。

问题拆分：（1） 怎么查找两个课程的成绩呢？ （2） 如何把课程表和学员信息表连接起来呢？

那么用到哪些知识呢？

（1） 子查询 （2） join

语句实现：先找到两门课的成绩

```sql
-- 课程1的成绩
SELECT sid ,score AS class1 FROM sc WHERE sc.cid = '01';
-- 课程2的成绩
SELECT sid,score AS class2 FROM sc WHERE sc.cid = '02';
```

class1 查询结果

class2 查询结果

两个子查询结束了以后，下一步就是使用join把这两个查询的结果连接起来

```sql
SELECT * FROM Student RIGHT JOIN 
(SELECT t1.sid,class1,class2 
FROM 
-- 这个是要查询两个表的成绩，即课程1大于课程2
(SELECT sid ,score AS class1 FROM sc WHERE sc.cid = '01') AS t1,
(SELECT sid,score AS class2 FROM sc WHERE sc.cid = '02') AS t2
WHERE t1.sid = t2.sid AND t1.class1 > t2.class2) r -- 这里是固定的，就是子查询的一个别名的设置
-- 这一步就是把两个表按照sid联结起来
ON Student.sid = r.sid;
```

![](https://pic1.zhimg.com/v2-f722d4ee9472c731c5b9eb28e2539d74_r.jpg)

最终查询结果

好的，，第一题就解决了

### 1.1 查询存在" 01 "课程但可能不存在" 02 "课程的情况(不存在时显示为 null )

思路：首先要查询所有的学生的选课情况，然后找到选择课程1和选择课程2的学生

解决方法： 使用join

选择两个分别选择课程1和课程2的学员表，然后把这两个表join起来

```sql
SELECT *
 FROM(SELECT *
 FROM sc
 WHERE sc.cid = '01' ) t1 LEFT JOIN 
 (SELECT *
 FROM sc WHERE sc.cid = '02') t2
 ON t1.sid = t2.sid ;
```

![](https://pic4.zhimg.com/v2-21acc1ef6604743ddcabb82e89d6296f_r.jpg)

输出结果

### 1.2 查询同时存在01和02课程的情况

思路：要求同时选择了01和02课程的情况，则需要使用where链接起来，这一题和上一题相比就是多了一个去除选了01但是没有选择02课程的这一部分

知识点：子查询，where语句

```sql
 SELECT *
 FROM (SELECT *
 FROM sc WHERE sc.cid = '01') AS t1,
 (SELECT * FROM sc WHERE sc.cid = '02') AS t2
 WHERE t1.sid = t2.sid;
```

![](https://pic3.zhimg.com/v2-918943d17b3cddc50475c8c858baa922_r.jpg)

执行结果

### 1.3 查询选择了02课程但没有01课程的情况

思路，要求首先得到选择了02课程的学员，然后剔除掉选择01课程的学员

知识点：子查询，NOT IN

```sql
 SELECT *
 FROM sc
 WHERE sc.sid NOT IN (SELECT sid FROM sc WHERE sc.cid = '01')
 AND sc.cid = '02';
```

写好了这个语句以后我觉得自己写的不够清楚，因此我使用了别名

看一下结果:

```sql
 SELECT *
 FROM sc
 WHERE sc.sid NOT IN 
 (SELECT sid FROM sc WHERE sc.cid = '01')
 AND sc.cid = '02';
```

![](https://pic4.zhimg.com/v2-7b49d1f35714782b607ad671011865df_r.jpg)

执行结果

**小结：**上面几个题基本可以说是同一类型，要注意子查询的使用，以及join和where的区别

## 2.查询平均成绩大于等于 60 分的同学的学生编号和学生姓名和平均成绩

思路：看到这个题的时候第一想法是要使用group by，和having，又因为成绩表（sc）没有学生姓名，所以还要使用join，当然avg肯定是需要的

考察知识：group by，join

```sql
SELECT s.sid,sname,AVG(sc.score)
FROM student AS s INNER JOIN sc
ON s.sid = sc.sid 
GROUP BY sc.sid
HAVING AVG(sc.score) >= 60;
```

![](https://pic3.zhimg.com/v2-f4575db930b19aa46f898bf0c06862ea_r.jpg)

执行结果

还有一种解法，使用子查询

```sql
SELECT r.*, s.sname FROM
(SELECT sid,AVG(score) FROM sc
GROUP BY sid 
HAVING AVG(score) > 60) r
LEFT JOIN student AS s ON s.sid = r.sid;
```

## 3.查询在 SC 表存在成绩的学生信息

思路：这题比较简单，就是两张表通过id连接，然后求单独值

知识点：Distinct

```sql
SELECT DISTINCT student.*
FROM sc,student
WHERE student.sid = sc.sid ;
```

![](https://pic2.zhimg.com/v2-f2cdc736ba7be86e54b8773702c8cb99_r.jpg)

执行结果

## 4.查询所有同学的学生编号、学生姓名、选课总数、所有课程的成绩总和

解题思路：首先用到两张表格，然后要求选课总数，和成绩总和，那么就需要用到groupby 和sum以及count

知识点：gruop by， sum，join，count

```sql
SELECT s.sid,s.sname,COUNT(sc.cid),SUM(score)
FROM student AS s INNER JOIN sc 
ON s.sid = sc.sid
GROUP BY sc.sid;
```

![](https://pic4.zhimg.com/v2-beb8841602c267c5148644313d0eb81f_r.jpg)

执行结果

## 5.查询「李」姓老师的数量

解题思路：这题考的是通配符的查询，比如使用like和%

知识点：count，like，%

```sql
SELECT COUNT(*)
FROM teacher
WHERE tname LIKE ('李%');
```

![](https://pic3.zhimg.com/v2-bca41c1adc9c9d799c70d0420b9cd046_r.jpg)

执行结果

## 6.查询学过「张三」老师授课的同学的信息

解题思路：要查找张三老师的授课科目，然后通过科目和score表连接，然后再和学生表连接

知识点：就是多重连接 inner join

```sql
SELECT s.*
FROM student AS s INNER JOIN sc ON s.sid = sc.sid
INNER JOIN  course  AS c ON sc.cid = c.cid
INNER JOIN teacher AS t ON t.tid = c.tid
WHERE t.tname = '张三';
```

![](https://pic2.zhimg.com/v2-76dd8ac0c7c8d097cb5d0babf7c27a85_r.jpg)

执行结果

## 7.查询没有学全所有课程的同学的信息

解题思路：首先利用子查询查询course表查询得到共有几门课，按照groupby的方式求课程数加上having小于查询出来的课程的所有学员信息就好

知识点：子查询，groupby，having，join

```sql
SELECT s.*,COUNT(cid)
FROM sc RIGHT JOIN student AS s 
ON sc.sid = s.sid
GROUP BY sc.sid
HAVING COUNT(cid) < (SELECT COUNT(DISTINCT cid)
FROM course);
```

![](https://pic4.zhimg.com/v2-044f64564a8b5b8160c3f88f0e2a21db_r.jpg)

执行结果

网友的有一种比较好的做法，是使用not in

```sql
SELECT s.*
FROM student AS s
WHERE s.sid NOT IN (SELECT sc.sid
FROM sc 
GROUP BY sc.sid
HAVING COUNT(cid) >= 3);-- 这一句也可以使用子查询
```

![](https://pic1.zhimg.com/v2-13358cd9d7f67831711e2a5779d3e008_r.jpg)

执行结果

## 8.查询至少有一门课与学号为" 01 "的同学所学相同的同学的信息

解题思路：首先要查询出01同学所学的课程，然后使用cid in 01同学的课程，并排除sid=01的同学

```sql
SELECT s.*
FROM sc INNER JOIN student AS s
ON sc.sid = s.sid
WHERE sc.cid IN (SELECT cid
FROM sc
WHERE sid = '01') AND sc.sid != '01'
GROUP BY sc.sid;
```

![](https://pic1.zhimg.com/v2-4d9a1006bda93d3ed7cf4d3fb91757c0_r.jpg)

执行结果

我这里使用了groupby语句来求取单个学员id，也可以使用distinct关键字来做区分

```sql
SELECT DISTINCT s.*
FROM sc INNER JOIN student AS s
ON sc.sid = s.sid
WHERE sc.cid IN (SELECT cid
FROM sc
WHERE sid = '01') AND sc.sid != '01';
```

![](https://pic4.zhimg.com/v2-4e4374486bec52d532afe57a5d93843f_r.jpg)

执行结果

这里就有一个知识点就是group by有去除重复值的功能，这个其实不难理解，因为group by就是按照单个组分类，可以理解为按照同一类进行切分

## 9.查询和" 01 "号的同学学习的课程完全相同的其他同学的信息

解题思路：首先感觉真题很难，想法是在student表的sid在sc的id中，而这个id又和cid相关联，然后再查询结束时，还要排除id=01的情况。反正很复杂。。。

知识点：n重子查询，in的使用

```sql
  SELECT 
    * 
  FROM
    student 
  WHERE sid IN 
    (SELECT 
      sid 
    FROM
      (SELECT 
        * 
      FROM
        sc AS a 
      WHERE cid IN 
        (SELECT 
          cid 
        FROM
          sc 
        WHERE sid = 01)) b 
    GROUP BY sid 
    HAVING COUNT(cid) = 
      (SELECT 
        COUNT(cid) 
      FROM
        sc c 
      WHERE sid = 01)) 
    AND sid != 01 ;  SELECT 
    * 
  FROM
    student 
  WHERE sid IN 
    (SELECT 
      sid 
    FROM
      (SELECT 
        * 
      FROM
        sc AS a 
      WHERE cid IN 
        (SELECT 
          cid 
        FROM
          sc 
        WHERE sid = 01)) b 
    GROUP BY sid 
    HAVING COUNT(cid) = 
      (SELECT 
        COUNT(cid) 
      FROM
        sc c 
      WHERE sid = 01)) 
    AND sid != 01 ;
```

![](https://pic4.zhimg.com/v2-d3db67a20a168dd0e867beaf059b8b73_r.jpg)

## 10.查询没学过"张三"老师讲授的任一门课程的学生姓名

解题思路：1先要查询出张三老师的教授课程，2然后利用not in 来找到学生的id

知识点：子查询，not in，多重join

```sql
SELECT sname FROM student 
WHERE sid NOT IN (
SELECT sid FROM sc 
LEFT JOIN course ON sc.cid=course.cid
LEFT JOIN teacher ON course.tid=teacher.tid 
WHERE tname='张三' )
```

![](https://pic1.zhimg.com/v2-70f6c723fb72666eee349efc8f0d2124_r.jpg)

## 11.查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩

解题思路：题中提到了两门及以上的不及格课程，因此考虑使用Groupby 和 having，题中还提到学生姓名以及平均成绩，考虑到这里还要求出平均成绩，但是平均成绩说明不是很清楚，

知识点：Group by ，having，以及子查询在from子句中的使用

```sql
SELECT sname,s.sid,AVG(sc1.score) AS avg_score
FROM (SELECT * FROM sc WHERE score < 60)sc1,
student AS s 
WHERE sc1.sid=s.sid
GROUP BY sc1.sid
HAVING COUNT(sc1.cid)>=2;
```

![](https://pic1.zhimg.com/v2-3456254095e6e3f78f0c8d0d835380a4_r.jpg)

## 12.检索" 01 "课程分数小于 60，按分数降序排列的学生信息

解题思路：要求01课程小于60的学员信息，然后按照降序排列学员信息

知识点：就是很简单的order by

```sql
SELECT s.*
FROM sc,student AS s
WHERE cid = '01' AND score <60
AND sc.sid = s.sid
ORDER BY score DESC;
```

![](https://pic4.zhimg.com/v2-0ce03b5889e6305935d11c2b67f393ef_r.jpg)

当然还有其他解法，比如使用join，这里就不多写了

## 13.按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩

解题思路：要按照平均成绩降序排列，则需要使用group by，以及要显示所有的课程信息，那么就需要使用join来实现了

知识点：group by ，子查询，join

```sql
SELECT 
  s.*,
  avg_score 
FROM
  sc AS s 
  LEFT JOIN 
    (SELECT 
      sc.sid,AVG(sc.score) AS avg_score 
    FROM
      sc 
    GROUP BY sc.sid 
    ORDER BY avg_score DESC) r 
    ON s.sid = r.sid ;
```

![](https://pic1.zhimg.com/v2-34e49f7c073c5f9f9edb9cfaf27dfbec_r.jpg)

可以发现这里的做法，发现并没有按照我想要的平均成绩的从高到低来排序，所以就去排查一下原因，发现这个错误是执行顺序的关系。

正确做法

```sql
SELECT 
  s.*,
  avg_score 
FROM
  sc AS s 
  LEFT JOIN 
    (SELECT 
      sc.sid ,AVG(sc.score) AS avg_score 
    FROM
      sc 
    GROUP BY sc.sid 
   ) r 
    ON s.sid = r.sid 
 ORDER BY avg_score DESC ;
```

![](https://pic4.zhimg.com/v2-cbc22c3de089bf51d0b488a97dd356f3_r.jpg)

这样就可以了，因此啊，sql执行顺序还是要牢记的

## 14.查询各科成绩最高分、最低分和平均分,以如下形式显示：

以如下形式显示：课程 ID，课程 name，最高分，最低分，平均分，及格率，中等率，

优良率，优秀率

及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90

要求输出课程号和选修人数，查询结果按人数降序排列，若人数相同，按课程号升序

排列

解题思路：这里要把最高分，最低分，平均分，等多个维度，那么就需要使用case when 语句了还有就是函数的使用

知识点：case when 别名的使用，以及函数的使用，和函数的嵌套使用，题目后边还考到了多列排列的使用，先按照desc，后按照asc排列

```sql
SELECT 
  cid AS 课程ID,
  COUNT(sid) AS 课程人数,
  MAX(score) AS 最高分,
  MIN(score) AS 最低分,
  AVG(score) AS 平均分,
  SUM(及格) / COUNT(sid) AS 及格率,
  SUM(中等) / COUNT(sid) AS 中等率,
  SUM(优良) / COUNT(sid) AS 优良率,
  SUM(优秀) / COUNT(sid) AS 优秀率 
FROM
  (SELECT 
    *,
    CASE
      WHEN score >= 60 
      THEN 1 
      ELSE 0 
    END AS 及格,
    CASE
      WHEN score >= 70 
      AND score < 80 
      THEN 1 
      ELSE 0 
    END AS 中等,
    CASE
      WHEN score >= 80 
      AND score < 90 
      THEN 1 
      ELSE 0 
    END AS 优良,
    CASE
      WHEN score >= 90 
      THEN 1 
      ELSE 0 
    END AS 优秀 
  FROM
    sc) a 
GROUP BY cid 
ORDER BY COUNT(sid) DESC,
  cid ;
```

特别长，但是知识很简单，就是case when语句的使用

![](https://pic3.zhimg.com/v2-f30630bfc867b0b795340f1cc050045a_r.jpg)

## 15.按各科成绩进行排序，并显示排名， Score 重复时保留名次空缺

解题思路：两张成绩表连接，然后按照成绩大小做一个排名

知识点：排序相关知识

```sql
SELECT 
  a.*,
  COUNT(a.score) AS 排名 
FROM
  sc AS a 
  LEFT JOIN sc AS b 
    ON a.cid = b.cid 
    AND a.score < b.score 
GROUP BY a.cid,
  a.sid 
ORDER BY a.cid,
  排名 ;
```

![](https://pic3.zhimg.com/v2-c794b56b48491f370cffdc292ee707ca_r.jpg)

### 15.1 按各科成绩进行行排序，并显示排名， Score 重复时合并名次

解题思路：相比于上一题这里多了一个合并名次的事情，所谓名次合并，意思是两个人的成绩一致按照一个名次来，比如两个成绩一样的排名第一，下一个排名第二的就当做第三名

```sql
SELECT 
  a.*,
  COUNT(b.score)+1 AS 排名 
FROM
  sc AS a 
  LEFT JOIN sc AS b 
    ON a.cid = b.cid 
    AND a.score < b.score 
GROUP BY a.cid,
  a.sid 
ORDER BY a.cid,
  排名 ;
```

![](https://pic2.zhimg.com/v2-06e4f77e38842437f06462ce18cd8ef9_r.jpg)

## 16.查询学生的总成绩，并进行排名，总分重复时保留名次空缺

解题思路：和15题差不多，不过我要引入一个用户变量

知识点：用户变量的使用

```sql
SELECT 
  a.*,
  @rank := @rank + 1 AS rank 
FROM
  (SELECT 
    sid,
    SUM(score) 
  FROM
    sc 
  GROUP BY sid 
  ORDER BY SUM(score) DESC) a,
  (SELECT 
    @rank := 0) b ;
```

![](https://pic4.zhimg.com/v2-be7523e066fa6f19c6c038b92a993843_r.jpg)

### 16.1 查询学生的总成绩，并进行排名，总分重复时不保留名次空缺

解题思路：这一题，不熟练，参考了前辈们的做法

知识点：用户变量，子查询

```sql
SELECT 
  a.*,
  CASE
    WHEN @fscore = a.sumscore 
    THEN @rank 
    WHEN @fscore := a.sumscore 
    THEN @rank := @rank + 1 
  END AS 排名 
FROM
  (SELECT 
    sc.sid,
    SUM(score) AS sumscore 
  FROM
    sc 
  GROUP BY sid 
  ORDER BY SUM(score) DESC) AS a,
  (SELECT 
    @rank := 0,
    @fscore := NULL) AS t ;
```

![](https://pic3.zhimg.com/v2-c6c9549a51496459a77173cecf1e8dfa_r.jpg)

## 17.统计各科成绩各分数段人数：课程编号，课程名称，[100-85]，[85-70]，[70-60]，[60-0] 及所占百分比

解题思路：这个题和之前的那一题很像，都是使用case when 语句，然后加一个join

知识点：join case when，以及

```sql
SELECT 
  sc.cid AS 课程编号,
  cname AS 课程名称,
  SUM(
    CASE
      WHEN score >= 0 
      AND score <= 60 
      THEN 1 
      ELSE 0 
    END
  ) AS '[60-0]',
  SUM(
    CASE
      WHEN score >= 0 
      AND score <= 60 
      THEN 1 
      ELSE 0 
    END
  ) / COUNT(sid) AS '[60-0]百分比',
  SUM(
    CASE
      WHEN score >= 60 
      AND score <= 70 
      THEN 1 
      ELSE 0 
    END
  ) AS '[70-60]',
  SUM(
    CASE
      WHEN score >= 60 
      AND score <= 70 
      THEN 1 
      ELSE 0 
    END
  ) / COUNT(sid) AS '[70-60]百分比',
  SUM(
    CASE
      WHEN score >= 70 
      AND score <= 85 
      THEN 1 
      ELSE 0 
    END
  ) AS '[85-70]',
  SUM(
    CASE
      WHEN score >= 70 
      AND score <= 85 
      THEN 1 
      ELSE 0 
    END
  ) / COUNT(sid) AS '[85-70]百分比',
  SUM(
    CASE
      WHEN score >= 85 
      AND score <= 100 
      THEN 1 
      ELSE 0 
    END
  ) AS '[100-85]',
  SUM(
    CASE
      WHEN score >= 85 
      AND score <= 100 
      THEN 1 
      ELSE 0 
    END
  ) / COUNT(sid) AS '[100-85]百分比' 
FROM
  sc 
  JOIN course 
    ON sc.cid = course.cid 
GROUP BY sc.cid,
  cname ;
```

![](https://pic3.zhimg.com/v2-026e8953cb21d1365c4b3f1e80b77e8a_r.jpg)

## 18.查询各科成绩前三名的记录

解题思路：按照课程id和学生id来分组，然后合并两个sc表，找到第一个表的成绩比第二个表成绩低的的值，然后排序

知识点： join groupby以及having的使用，当然还有order by的使用

```sql
SELECT a.*,COUNT(b.score) +1 AS ranking
FROM SC AS a LEFT JOIN SC AS b 
ON a.cid = b.cid AND a.score<b.score
GROUP BY a.cid,a.sid
HAVING ranking <= 3
ORDER BY a.cid,ranking;
```

## 19.查询每门课程被选修的学生数

解题思路：就是很简单的按照cid分组，然后计算每个组里面的学生人数，由于前面几题都是想了很久，遇到这题，我竟然不敢相信这么简单

知识点：groupby

```sql
SELECT cid,COUNT(sid) AS num
FROM  sc 
GROUP BY cid;
```

![](https://pic4.zhimg.com/v2-4e2e33658d82f1cb91a44c9c285a0cff_r.jpg)

## 20.查询出只选修两门课程的学生学号和姓名

解题思路：查询选择了两门课的学生，然后连接student表，还算是比较基础的题

知识点：groupby having join

```sql
SELECT sc.sid,sname
FROM sc INNER JOIN student
ON sc.sid = student.sid
GROUP BY sc.sid
HAVING COUNT(sc.cid) = 2;
```

![](https://pic1.zhimg.com/v2-93d4ff84caad2d0747ce0bb86e2d57dc_r.jpg)

## 21.查询男生、女生人数

解题思路：男女生人数这个毫无疑问考的是groupby 还有就是分组以后的count，或者可以使用case when

知识点：groupby，或者 case when

```sql
SELECT ssex,COUNT(ssex) as 
FROM student
GROUP BY ssex;
```

![](https://pic2.zhimg.com/v2-d8c68ba27e8ec02b88f1fc36e460cbe5_r.jpg)

```sql
SELECT SUM(CASE WHEN ssex='男' THEN 1 ELSE 0 END)AS 男生人数,
       SUM(CASE WHEN ssex='女' THEN 1 ELSE 0 END) AS 女生人数
FROM student;
```

![](https://pic1.zhimg.com/v2-255dfdaf5367231522fe6a013bcdb10c_r.jpg)

当然这里不完美的，可以使用count（sid）-男生人数，有很多的方式。

## 22.查询名字中含有「风」字的学生信息

解题思路：很简单的一题，就是考察通配符，而考察点是是否有风

知识点：通配符%

```sql
SELECT *
FROM student
WHERE sname LIKE '%风%';
```

![](https://pic2.zhimg.com/v2-b0621b8dc18e069cd22d417a3ba886dd_r.jpg)

## 23.查询同名同性学生名单，并统计同名人数

解题思路：同名同性，我们需要的是count（sname）>1，还要做一步筛选。这里有一个小坑，开始的时候我以为是同名同姓，中国文化博大精深啊

知识点：having，groupby

```sql
SELECT sname,COUNT(sname) AS 同名人数
FROM student
GROUP BY sname
HAVING 同名人数>1;
```

![](https://pic4.zhimg.com/v2-b0427c3ba5977351a351f27d2c5d1d5b_r.jpg)

由于这个数据表中没有同名的人数，所以显示结果如上。

## 24.查询 1990 年出生的学生名单

解题思路：很简单的题，但是我还是做错了

知识点：通配符

```sql
SELECT sname
FROM student
WHERE sage LIKE '1990%';
```

![](https://pic2.zhimg.com/v2-01ec95349a32c9261928f76e58280321_r.jpg)

本以为可以很快的执行并产生结果，但是没想到报错了，看一下报错信息，睡是datetime error，这个是什么原因呢，我们看一下，之前创表的语句

![](https://pic2.zhimg.com/v2-9f696a7027f21036e45f0c70b362cd39_r.jpg)

原来原本的建表的时候就是设置了datetime类型，那这里就要使用date函数了，这里对date函数做一些介绍

![](https://pic2.zhimg.com/v2-aa21c3c3923a78da51545017530bcd4d_r.jpg)

网上搜的图

我觉得上面的图能够很好的说明常用的时间函数处理，无非就是hour（小时），minute（分钟），second（秒），day（天），week（周），month（月），quarter（季度），year（年度）

那么这一题，显然就是要使用year了

```sql
SELECT *
FROM student
WHERE YEAR(sage) LIKE '1990%';
```

![](https://pic4.zhimg.com/v2-2919741d2838816e4065d67065689627_r.jpg)

好的，很完美的解决了问题

## 25.查询每门课程的平均成绩，结果按平均成绩降序排列，平均成绩相同时，按课程编

号升序排列

解题思路：按照课程分组，然后求分组以后的平均值。然后外加一个排序，排序顺序按照课程id升序排列

知识点：group by ，order by

```sql
SELECT cid,AVG(score)
FROM sc
GROUP BY cid
ORDER BY AVG(score) DESC,cid;
```

![](https://pic1.zhimg.com/v2-df7b5740b72a28ad95fc97aa435d997c_r.jpg)

由于这个表里面只有3门课，所以这里显示不出来，如果有很多数据，然后又恰好有相同的id

的时候应该就可以看出不同了

## 26.查询平均成绩大于等于 85 的所有学生的学号、姓名和平均成绩

解题思路：按照学生id分组，然后求平均分，然后用having语句判断平均分大于等于85，又因为要查询学生姓名因此可以使用join，或者直接使用笛卡尔积，但是笛卡尔积对于数据量比较大的时候不宜使用

知识点：having，groupby，join

```sql
SELECT sc.sid,sname,AVG(score) AS 平均成绩
FROM sc,student
WHERE sc.sid = student.sid
GROUP BY sc.sid
HAVING 平均成绩>= 85;
```

![](https://pic4.zhimg.com/v2-f76f1bf9c6adfe62ae2715697b22c6bf_r.jpg)

  

或者

```sql
SELECT sc.sid,sname,AVG(score) AS 平均成绩
FROM sc INNER JOIN student ON sc.sid = student.sid
GROUP BY sc.sid
HAVING 平均成绩>= 85;
```

结果都是第一样的，但是数据里大的时候我会使用第二种解法

## 27.查询课程名称为「数学」，且分数低于 60 的学生姓名和分数

解题思路：和上一题一样，可以使用join，也可以直接使用笛卡尔积，但是要使用三张表，所以最好还是使用join

知识点：多重join的使用

```sql
SELECT 
  sname,
  score 
FROM
  course AS c 
  INNER JOIN sc 
    ON c.cid = sc.cid 
    AND c.cname = '数学' 
    AND sc.score < 60 
  INNER JOIN student AS s 
    ON sc.sid = s.sid ;
```

![](https://pic1.zhimg.com/v2-36f00af0555589e35590c250b90ecafc_r.jpg)

## 28.查询所有学生的课程及分数情况（存在学生没成绩，没选课的情况）

解题思路：要把学生的姓名展示出来，所以使用join

知识点：join的使用

```sql
SELECT 
  sname,
  sc.cid,
  score 
FROM
  sc 
  INNER JOIN student AS s 
    ON sc.sid = s.sid ;
```

![](https://pic4.zhimg.com/v2-16414dbdd40d32c965f976d91882becf_r.jpg)

## 29.查询任何一门课程成绩在 70 分以上的姓名、课程名称和分数

解题思路:就是在上一题的基础上，加上一个where条件判断，或者直接在join on后边判断

知识点：join加上条件判断

```sql
SELECT 
  sname,
  sc.cid,
  score 
FROM
  sc 
  INNER JOIN student AS s 
    ON sc.sid = s.sid 
    AND score > 70 ;
```

![](https://pic4.zhimg.com/v2-d825e9b343e1a2c1ba5d6ae21355e5cf_r.jpg)

或者

```sql
SELECT 
  sname,
  sc.cid,
  score 
FROM
  sc 
  INNER JOIN student AS s 
    ON sc.sid = s.sid 
WHERE score > 70 ;
```

结果是一样的，至于两者的区别，可以参考下面这篇文章

总结起来就是inner join后边时两个做法是没有区别的，但是如果是left join这种就会有多余的出来了，这是因为使用join以后会生成一个虚拟表，而使用where是在这张虚拟表的基础上进行筛选，所以结果可能不一样。这个值得注意。

## 30.查询不及格的课程

解题思路：这个其实和上面一题是同一个考法，不详细展开，唯一的区别就是只查询课程，所以就是要distinct，或者groupby

知识点：join，

```sql
SELECT 
sc.cid
FROM
  sc 
  INNER JOIN student AS s 
    ON sc.sid = s.sid 
WHERE score < 60 
GROUP BY sc.cid
```

第二种使用distinct

```sql
SELECT 
DISTINCT sc.cid
FROM
  sc 
  INNER JOIN student AS s 
    ON sc.sid = s.sid 
WHERE score < 60 ;
```

![](https://pic3.zhimg.com/v2-2764f1c4dde6ba173ab0e97f6801235e_r.jpg)

## 31.查询课程编号为 01 且课程成绩在 80 分以上的学生的学号和姓名

解题思路：首先肯定需要join把两个表连起来，然后使用条件查询课程id为01且成绩大于80

知识点：join where

```sql
SELECT sc.sid,sname
FROM sc INNER JOIN student AS s
ON sc.sid = s.sid
WHERE sc.cid = 01 AND score >= 80;
```

![](https://pic3.zhimg.com/v2-6c15af5a0ca5a8c8ae1c6d64549f6e4e_r.jpg)

## 32.求每门课程的学生人数

解题思路：就是简单的按照课程分组然后求sid的人数

知识点：group by ，count

```sql
SELECT cid,COUNT(sid) AS 人数
FROM sc
GROUP BY cid;
```

![](https://pic3.zhimg.com/v2-b06ba803ccbcc861636e58516e5d28f2_r.jpg)

## 33.成绩不重复，查询选修「张三」老师所授课程的学生中，成绩最高的学生信息及其成绩

解题思路：这题首先要找到张三老师所授课程，然后找到选择这门课程的学生，在成绩按照从大到小的顺序排列，然后只取最高的成绩，当然还需要使用子查询

知识点：join，limit

```sql
SELECT 
  s.*,
  score 
FROM
  student AS s 
  INNER JOIN sc 
    ON s.sid = sc.sid 
WHERE sc.cid = 
  (SELECT 
    cid 
  FROM
    teacher AS t 
    INNER JOIN course AS c 
      ON t.tid = c.cid 
      AND tname = '张三') 
ORDER BY score DESC 
LIMIT 1 ;
```

![](https://pic3.zhimg.com/v2-b72541b50b6435b1767f970d53627a2a_r.jpg)

现在慢慢有点感觉了，能够很快写出来，果然这种语言相关的还是要多敲代码，不要怕错，就是乌龟的速度，只要一直往前也有机会超过兔子

## 34.成绩有重复的情况下，查询选修「张三」老师所授课程的学生中，成绩最高的学生

信息及其成绩

解题思路：有重复成绩，我们就选择一个最大成绩然后使用子查询，让成绩在各科成绩的最大值里面做筛选

知识点：多重join，以及子查询的应用

```sql
SELECT 
  student.*,
  sc.cid,
  score 
FROM
  student 
  INNER JOIN sc 
    ON student.sid = sc.sid 
  JOIN course 
    ON sc.cid = course.cid 
  JOIN teacher 
    ON course.tid = teacher.tid 
WHERE tname = '张三' 
  AND score IN 
  (SELECT 
    MAX(score) 
  FROM
    sc 
    INNER JOIN course 
      ON sc.cid = course.cid 
    JOIN teacher 
      ON course.tid = teacher.tid 
  WHERE tname = '张三') ;
```

![](https://pic3.zhimg.com/v2-b72541b50b6435b1767f970d53627a2a_r.jpg)

由于我创建的表格里面没有重复的成绩，所以返回结果没有发生变化

## 35.查询不同课程成绩相同的学生的学生编号、课程编号、学生成绩

解题思路：首先join两张表，然后做筛选1成绩相同，2课程id不相同

知识点：join，以及where的使用

```sql
SELECT 
  DISTINCT a.* 
FROM
  sc AS a 
  INNER JOIN sc AS b 
WHERE a.score = b.score 
  AND a.cid != b.cid ;
```

由于可能使用的是inner join，所以可能会有很多重复值，因此我用了一个distinct的做法。

![](https://pic2.zhimg.com/v2-3406025b118a347f2179d91ae2908eb5_r.jpg)

## 36.查询每门成绩最好的前两名

解题思路：这题和前面有一题有很相似的地方，就是找到大于某个成绩只有两人就是前两名

知识点：关联子查询

```sql
SELECT 
  * 
FROM
  sc 
WHERE 
  (SELECT 
    COUNT(*) 
  FROM
    sc AS a 
  WHERE sc.cid = a.cid 
    AND sc.score < a.score) < 2 
ORDER BY cid ASC,
  sc.score DESC ;
```

![](https://pic4.zhimg.com/v2-0e98656cf98edf78778cfb99a80d1b6f_r.jpg)

我觉得这个题还是相对来说好理解的，同样的查询前3名等等，只需改一个数字。

## 37.统计每门课程的学生选修人数（超过 5 人的课程才统计）。

解题思路：按照课程分组，然后count人数，使用having做一个筛选

知识点：groupby having

```sql
SELECT 
  cid,
  COUNT(sid) 
FROM
  sc 
GROUP BY cid 
HAVING COUNT(sid) > 5 ;
```

![](https://pic4.zhimg.com/v2-cff5f172db42d064cf00ac5d735fa1df_r.jpg)

## 38.检索至少选修两门课程的学生学号

解题思路：和上一题基本类似按照学生id分组然后count cid，筛选出大于等于2的学生

知识点：groupby having

```sql
SELECT 
  sid,
  COUNT(cid) 
FROM
  sc 
GROUP BY sid 
HAVING COUNT(cid) >= 2 ;
```

![](https://pic4.zhimg.com/v2-8ea2bdc14e112d7cfaba457e8840500f_r.jpg)

## 39.查询选修了全部课程的学生信息

解题思路：同样的我们要知道总共有多少门课，不光是为了回答这一道题，可能真实情况会有很多的课，那么就需要把course表中所有的课程计数，还要求学生信息，那就需要使用join了

知识点：join，where，子查询

```sql
SELECT 
  s.* 
FROM
  sc 
  INNER JOIN student AS s 
    ON sc.sid = s.sid 
WHERE cid = 
  (SELECT 
    COUNT(*) 
  FROM
    course) ;
```

![](https://pic4.zhimg.com/v2-ccd190a598b4e3f1216dba69de834553_r.jpg)

## 40.查询各学生的年龄，只按年份来算

解题思路：这题开始就要使用时间函数了，不难，还算比较简单

知识点：date函数的使用

```sql
SELECT 
  sname,
  YEAR(NOW()) - YEAR(sage) AS 年纪 
FROM
  student ;
```

![](https://pic3.zhimg.com/v2-7f6a5f8b1d8bbeb5a212227b37792392_r.jpg)

## 41.按照出生日期来算，当前月日 < 出生年月的月日则，年龄减一

解题思路：这个可以使用case when语句，然后使用时间戳求差值

知识点：case when ，year，date\_format

```sql
SELECT 
  sname,
  CASE
    WHEN (
      DATE_FORMAT(NOW(), '%m-%d') - DATE_FORMAT(sage, '%m-%d')
    ) < 0 
    THEN YEAR(NOW()) - YEAR(sage) + 1 
    ELSE YEAR(NOW()) - YEAR(sage) 
  END AS 年龄 
FROM
  student ;
```

![](https://pic3.zhimg.com/v2-9f47206597bfada847f9a81ac7469e32_r.jpg)

## 42.查询本周过生日的学生

解题思路：使用week函数

知识点：week，now

```sql
SELECT sname
FROM student
WHERE WEEK(sage) = WEEK(NOW());
```

![](https://pic1.zhimg.com/v2-6a2d9c3478420a335d7740b3c46732a8_r.jpg)

可以发现没有结果，为了验证准确性，我们把student表格单独拉出来

```sql
SELECT 
  * 
FROM
  student ; 
```

![](https://pic1.zhimg.com/v2-dedfcafb7b073112b51c3f619c6078bc_r.jpg)

可以看到果然没有，那么为了进一步验证，我们向student表中新增一行数据

```sql
INSERT INTO Student VALUES('09' , '关羽' , '1990-07-15' , '男');
SELECT 
  * 
FROM
  student ;
```

看一下执行结果

![](https://pic3.zhimg.com/v2-c8a0ea07d01b8e2be303694270991c66_r.jpg)

在执行以下我们之前的语句

![](https://pic4.zhimg.com/v2-c4605d3daa1f22885736a8faefb3ac27_r.jpg)

ok看来语句是没有毛病的，我也不知道为啥非要验证以下，可能是强迫症吧。。。

## 43.查询下周过生日的学生

解题思路：好的嘛，下周过生日，代表着在上题的基础上再加1

知识点：week，now

```sql
SELECT 
  sname 
FROM
  student 
WHERE WEEK(sage) = WEEK(NOW()) + 1 ;
```

![](https://pic1.zhimg.com/v2-da3098c45b3fab24bdb43c47293ada50_r.jpg)

当然还是没有结果的，不过这里就不做测试了

## 44.查询本月过生日的学生

解题思路：基本类似的操作，使用month函数

知识点：month，now

```sql
SELECT 
  sname,
  sage 
FROM
  student 
WHERE MONTH(sage) = MONTH(NOW());
```

![](https://pic2.zhimg.com/v2-043285a3f4e94c191939a6d7e179d465_r.jpg)

## 45.查询下月过生日的学生

解题思路：同样的套路，同样的解法

知识点：month，now

```sql
SELECT 
  sname,
  sage 
FROM
  student 
WHERE MONTH(sage) = MONTH(NOW()) + 1 ;
```

![](https://pic1.zhimg.com/v2-98aedc7fdcbbaa4a2a78e35263a1cc9c_r.jpg)

---
