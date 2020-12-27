package com.ibeifeng.sparkproject.spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.ibeifeng.sparkproject.conf.ConfigurationManager
import com.ibeifeng.sparkproject.constant.Constants
import com.ibeifeng.sparkproject.dao.factory.DAOFactory
import com.ibeifeng.sparkproject.domain.SessionAggrStat
import com.ibeifeng.sparkproject.test.MockData
import com.ibeifeng.sparkproject.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}




/**
  * 用户访问session分析Spark作业
  *
  * 接收用户创建的分析任务，用户可能 指定的条件如下：
  * 1.时间范围：起始日期-结束日期
  * 2.性别 ：男/女
  * 3.年龄范围
  * 4.职业：多选
  * 5.城市：多选
  * 6.搜索词：多个，只要某个session中的任何一个action搜索过指定的关键词，那么该session就符合条件
  * 7.点击 品类：多个品类，只要某个session中的任何一个action点击 过某个品类，那么该session就符合条件
  *  我们的spark作业如何 接受用户创建的任务？
  *
  *  J2EE平台在接收用户创建任务的请求之后 ，会将任务信息插入MySQL的task表中，任务参数以JSON格式封闭在task_param字段 中
  *
  *  接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
  *  spark-submit shell在执行时，是可以接收参数的，并将接收的参数传递给Spark作业的main函数
  *  参数就封闭在main函数的args数组 中
  *
  */
object UserVisitSessionAnalyzeSpark {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext=getSQLContext(sc)

    //生成模拟测试数据
    mockData(sc,sqlContext)

    //创建需要使用的DAO组件
    val taskDAO=DAOFactory.getTaskDAO()
    //如果要进行session粒度的数据 聚合，
    // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
    //如果要根据用户在创建任务时指定的参数，来进行数据 过滤和筛选

    //那么就首先得查询 出指定的任务,并获取 任务的查询 参数
    val taskid=ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION)
    val task=taskDAO.findById(taskid)
    val taskParam=JSON.parseObject(task.getTaskParam)


    val actionRDD=getActionRDDByDateRange(sqlContext,taskParam)

//    println("actionRDD:"+actionRDD.count())
//
//    actionRDD.take(10).foreach(println)
    //首先，可以将行为数据 ，按照session_id进行groupByKey分组
    //此时的数据 的粒度就是session粒度了，然后，可以将session粒度的数据
    //与用户信息数据 ，进行join
    //然后就可以获取 到session粒度的数据 ，财时，数据 里面还包含了session对应的user信息
    // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
    val sessionid2AggrInfoRdd= aggregateBySession(sqlContext,actionRDD)

    //打印测试
//    println("sessionid2AggrInfoRdd.count():"+sessionid2AggrInfoRdd.count())

//    sessionid2AggrInfoRdd.take(10).foreach(println)

    //接着，就要针对 session粒度的聚合数据 ，按照使用者指定的筛选参数进行数据 过滤
    //相当于自己编写的算子，是要访问外面 的任务参数对象的
    //匿名内部类（算子函数 ），访问外部对象，是要给外部对象使用final修饰的
    //重构，同时进行过滤和统计
    val sessionAggrStatAccumulator=sc.accumulator("")(SessionAggrStatAccumulator)
     val filteredSessionid2AggrInfoRDD= filterSessionAndAggrStat(sessionid2AggrInfoRdd,taskParam,sessionAggrStatAccumulator)


    //action行为，触发RDD执行计算
    filteredSessionid2AggrInfoRDD.count()
    println("sessionAggrStatAccumulator.value:"+sessionAggrStatAccumulator.value)


//  计算出各个范围的session占比，并写入MySQL
    calculateAndPersistAggrStat(sessionAggrStatAccumulator.value,taskid)


    /**
      * session聚合统计（统计出访问时长和访问步长，各个区间的session数量 占总session数量 的比例）
      *
      * 如果不进行重构，直接来实现，思路：
      * 1、actionRDD,映射 成<sessionid,Row>格式
      * 2、按sessionid聚合，计算每个session的访问时长和访问步长，生成一个新的RDD
      * 3、遍历 新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中对应的值
      * 4、使用自定义Accumulator中的统计值 ，去计算各个区间的比例
      * 5、将最后计算出来 的结果 ，写入MySQL对应的表中
      *
      * 普通 实现思路的问题：
      * 1、为什么还要用actionRDD去映射 ？其实我们之前 在session聚合的时候，映射 已经做过了，多此一举
      * 2、是不是一定要为了session聚合这个功能，单独去遍历 一遍session？其实没有必要，已经有session数据了
      *
      * 重构实现思路：
      * 1、不要去生成任何新的RDD（处理上亿的数据）
      * 2、不要去单独遍历 一遍session的数据 （处理上千万的数据 ）
      * 3、可以在进行session聚合的时候 ，就直接计算出每个session的访问时长和访问步长
      * 4、在进行过滤的时候，本来就要遍历 所有的聚合session信息，此时，就可以在某个session通过筛选条件后
      * 将其访问时长和访问步长，累加到自定义的Accumulator上面去
      *
      * 开发Spark大型复杂项目的一些经验准则：
      * 1、尽量少生成RDD
      * 2、尽量少对RDD进行算子操作，如果有可能 ，尽量在一个算子里面，实现多个需要做的功能
      * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey(能用map就用map)
      * shuffle操作，会导致大量的磁盘读写，严重降低性能
      * 有shuffle的算子和没有shuffle的算子，性能达到几十分钟和数小时的差距
      * 有shuffle的算子，很容易 导致数据 倾斜，一旦数据倾斜，性能杀手
      */
    //关闭spark上下文
    sc.stop()
  }

  /**
    * 获取 SQLContext
    * 如果 是本地测试环境，生成SQLContext对象
    * 如果 是在生产环境，生成HiveContext对象
    * @param sc
    * @return
    */
  def getSQLContext(sc:SparkContext):SQLContext={
      val local=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local){
      new SQLContext(sc)
    }else{
      new HiveContext(sc)
    }
  }

  /**
    * 生成模拟 数据 （只有是本地模式，才会生成模拟 数据 ）
    * @param context
    * @param context1
    */
  def mockData(sc: SparkContext, sqlContext: SQLContext)={
    val local=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local){
      MockData.mock(sc,sqlContext)
    }
  }

  /**
    * 获取 指定日期范围内的用户访问行为数据
    * @param sqlContext
    * @param taskParam  任务参数
    */
  def getActionRDDByDateRange(sqlContext:SQLContext,taskParam:JSONObject):RDD[Row]={

    val startDate=ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    val sql="select * from user_visit_action "+
    " where date>='"+startDate +"' " +"and date<='"+endDate+"'"

    val actionDF=sqlContext.sql(sql)
    actionDF.rdd
  }

  /**
    * 对行为数据 按session粒度进行聚合
    * @param actionRDD
    */
  def aggregateBySession(sqlContext: SQLContext,actionRDD:RDD[Row]):RDD[(String,String)]={

    //现在actionRdd中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击 或者搜索
    //我们现在需要将这个Row映射 成<sessionid,Row>的格式
    val sessionid2ActionRDD=actionRDD.map(row=>(row.get(2),row))


    //对行为数据 按session粒度进行分组<sessionid,Iterable<Row>>
   val sessionid2ActionsRDD=sessionid2ActionRDD.groupByKey()
  //对每个session分组进行聚合，将session中所有的搜索词和点击 品类都聚合起来
    val userid2PartAggrInfoRDD=sessionid2ActionsRDD.map(tuple=>{
      val sessionid=tuple._1
      val iterator=tuple._2.iterator

      val searchKeywordsBuffer=new StringBuilder("")
      val clickCategoryIdsBuffer=new StringBuilder("")

      /**
        * The difference between val and var is that val makes a variable immutable — like final in Java — and var makes a variable mutable
        */
      //scala 的Long相当于java的long，不能为null
      var userid:Long=0

      var startTime:java.util.Date=null
      var endTime:java.util.Date=null
      var stepLength:Long=0
      //遍历 session所有的访问行为
      while(iterator.hasNext){
        //提取每个访问行为的搜索词字段 和点击 品类字段
        val row=iterator.next()
        if(row.getLong(1)!=0){
          userid=row.getLong(1)
        }

        val searchKeyword=row.getString(5)
        //getLong(null)返回0
        val clickCategoryId=row.getLong(6)

        //实际上这里要对数据 说明一下
        //并不是每一行访问行为都有以上两个字段
        //实际 只有搜索行为才有searchKeyword
        //只有点击 品类行为，是有clickCategoryId
        //所以任何一行数据 都不可能 两个都有，即存在null值 的情况

        //首先要满足：不能是null值
        //其次，之前 的字符串中还没有搜索词或者点击 品类id
        if(StringUtils.isNotEmpty(searchKeyword)){
          if(!searchKeywordsBuffer.toString().contains(searchKeyword)){
            searchKeywordsBuffer.append(searchKeyword+",")
          }
        }
        if(clickCategoryId!=null&&clickCategoryId!=0){
          if(!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
            clickCategoryIdsBuffer.append(clickCategoryId+",")
          }
        }

        //计算session开始和结束 时间
        val actionTime=DateUtils.parseTime(row.getString(4))
        if(startTime==null){
          startTime=actionTime
        }
        if(endTime==null){
          endTime=actionTime
        }

        //保证startTime是最前，endTime最后
        if(actionTime.before(startTime)){
          startTime=actionTime
        }

        if(actionTime.after(endTime)){
          endTime=actionTime
        }

        //计算session访问步长
        stepLength=stepLength+1

      }
      val searchKeywords=StringUtils.trimComma(searchKeywordsBuffer.toString())
      val clickCategoryIds=StringUtils.trimComma(clickCategoryIdsBuffer.toString())

      //计算session访问时长（秒）
      val visitLength=(endTime.getTime-startTime.getTime)/1000

      //返回的数据格式，即<sessionid,partAggrInfo>
      //但是，这一步聚合完了以后，其实，还需要将每一行数据 ，跟对应的用户信息进行聚合
      //问题就来了，如果跟用户信息进行聚合的话，那么key，就不应该是sessionid
      //而应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
      //如果 我们这里直接返回<seesionid,partAggrInfo>，还得再做一次map
      //将RDD映射 成<userid,partAggrInfo>格式，就多些一举了

      //所以，我们这里其实可以直接返回<userid,partAggrInfo>
      //然后跟用户信息join的时候，将partAggrInfo关联上userInfo
      //然后再直接将返回的Tuple的key设置成sessionid
      //最后的数据 格式，还是<sessionid,fullAggrInfo>

      //聚合数据 ，用什么样的格式进行拼接
      //这里统一定义，使用key=value|key=value的格式拼接
        val partAggrInfo=Constants.FIELD_SESSION_ID+"="+sessionid+"|"+Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeywords+"|" +
        Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds+"|"+Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"+Constants.FIELD_STEP_LENGTH+"="+stepLength

        Tuple2(userid, partAggrInfo)

    })


    //查询 所有用户数据,并映射 成<userid,Row>的格式
    val sql="select * from user_info"
    val userInfoRDD=sqlContext.sql(sql).rdd

    val userid2InfoRDD=userInfoRDD.map(row=>(row.getLong(0),row))

    //将session粒度聚合数据，与用户信息进行join
    val userid2FullInfoRDD=userid2PartAggrInfoRDD.join(userid2InfoRDD)

    //对join起来的数据 进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    val sessionid2FullAggrInfoRDD=userid2FullInfoRDD.map(tuple=>{
      //tuple._1是关联的key，即userid
      val partAggrInfo=tuple._2._1
      val userInfoRow=tuple._2._2

      val sessionid=StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID)
      val age=userInfoRow.getInt(3)
      val professional=userInfoRow.getString(4)
      val city=userInfoRow.getString(5)
      val sex=userInfoRow.getString(6)

      val fullAggrInfo=partAggrInfo+"|"+Constants.FIELD_AGE+"=" +
        age+"|"+Constants.FIELD_PROFESSIONAL+"="+professional+"|" +
        Constants.FIELD_CITY+"="+city+"|"+Constants.FIELD_SEX+"="+sex

      Tuple2(sessionid, fullAggrInfo)
    })

    sessionid2FullAggrInfoRDD
  }


  /**
    * 过滤session数据
    * @param session2AggrInfoRdd
    * @return
    */
  def filterSessionAndAggrStat(session2AggrInfoRdd:RDD[(String,String)],taskParam:JSONObject,sessionAggrStatAccumulator: Accumulator[String]):RDD[(String,String)]={


    //为了使用后面的ValidUtils，所以，首先将所有的筛选参数拼接成一个连接串
    //为后面性能优化埋下伏笔
    val startAge=ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE)
    val endAge=ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE)
    val professionals=ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS)
    val cities=ParamUtils.getParam(taskParam,Constants.PARAM_CITIES)
    val sex=ParamUtils.getParam(taskParam,Constants.PARAM_SEX)
    val keywords=ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS)
    val categoryIds=ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS)
    var _parameter=""
    if(startAge!=null){
      _parameter=_parameter+Constants.PARAM_START_AGE+"="+startAge+"|"
    }
    if(endAge!=null){
      _parameter=_parameter+Constants.PARAM_END_AGE+"="+endAge+"|"
    }

    if(professionals!=null){
      _parameter=_parameter+Constants.PARAM_PROFESSIONALS+"="+professionals+"|"
    }
    if(cities!=null){
      _parameter=_parameter+Constants.PARAM_CITIES+"="+cities+"|"
    }
    if(sex!=null){
      _parameter=_parameter+Constants.PARAM_SEX+"="+sex+"|"
    }
    if(keywords!=null){
      _parameter=_parameter+Constants.PARAM_KEYWORDS+"="+keywords+"|"
    }
    if(categoryIds!=null){
      _parameter=_parameter+Constants.PARAM_CATEGORY_IDS+"="+categoryIds
    }
    //确保去除最后一个 |
    //这里是一个坑，如果用java版的_parameter.endsWith("\\|")会返回false
    if(_parameter.endsWith("|")){
      _parameter=_parameter.substring(0,_parameter.length-1)
    }

    val parameter=_parameter


    val filteredSessionid2AggrInfoRDD= session2AggrInfoRdd.filter(tuple=>{
      //首先，从tuple中，获取 聚合数据
      val aggrInfo=tuple._2
      //接着，依次按照筛选条件进行过滤
      //按照 年龄范围进行过滤（startAge、endAge）

      //如果task里面的筛选条件为null，则ValidUtils会返回true
      if(!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
        false
      }else if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter,Constants.PARAM_PROFESSIONALS)){
        false
      }else if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,Constants.PARAM_CITIES)){
        false
      }else if(!ValidUtils.equal(aggrInfo,Constants.FIELD_SEX,parameter,Constants.PARAM_SEX)){
        false
      }else if(!ValidUtils.in(aggrInfo,Constants.FIELD_SEARCH_KEYWORDS,parameter,Constants.PARAM_KEYWORDS)){
        false
      }else if(!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,parameter,Constants.PARAM_CATEGORY_IDS)){
        false
      }
      else{
        //如果经过之前多个过滤条件之后 ，程序能走到这里
        //那么说明，该session是通过了用户指定的筛选条件，是我们需要保留的
        //根据session对应的范围，相等相应的累加计数
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

        //计算出session的访问时长和访问步长的范围，并进行相应的累加
        val visitLength=StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong
        val stepLength=StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong
        calculateVisitLength(sessionAggrStatAccumulator,visitLength)
        calculateStepLength(sessionAggrStatAccumulator,stepLength)
        true
      }
    })

    filteredSessionid2AggrInfoRDD
  }

  /**
    * 计算访问时长范围
    * @param sessionAggrStatAccumulator
    * @param visitLength
    */
  def calculateVisitLength(sessionAggrStatAccumulator: Accumulator[String],visitLength:Long)={
    if(visitLength >=1 && visitLength <= 3) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
    } else if(visitLength >=4 && visitLength <= 6) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
    } else if(visitLength >=7 && visitLength <= 9) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
    } else if(visitLength >=10 && visitLength <= 30) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
    } else if(visitLength > 30 && visitLength <= 60) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
    } else if(visitLength > 60 && visitLength <= 180) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
    } else if(visitLength > 180 && visitLength <= 600) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
    } else if(visitLength > 600 && visitLength <= 1800) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
    } else if(visitLength > 1800) {
      sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
    }
  }

  /**
    * 计算访问步长范围
    * @param sessionAggrStatAccumulator
    * @param stepLength
    */
  def calculateStepLength(sessionAggrStatAccumulator: Accumulator[String],stepLength:Long)={
    if(stepLength >= 1 && stepLength <= 3) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
    } else if(stepLength >= 4 && stepLength <= 6) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
    } else if(stepLength >= 7 && stepLength <= 9) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
    } else if(stepLength >= 10 && stepLength <= 30) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
    } else if(stepLength > 30 && stepLength <= 60) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
    } else if(stepLength > 60) {
      sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
    }
  }


  /**
    * 计算各session范围占比，并写入MYSQL
    * @param value
    */
  def calculateAndPersistAggrStat(value: String,taskid:Long) = {

    // 从Accumulator统计串中获取值
    // 从Accumulator统计串中获取值

    val session_count = StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT).toLong

    val visit_length_1s_3s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s).toLong
    val visit_length_4s_6s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s).toLong
    val visit_length_7s_9s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s).toLong
    val visit_length_10s_30s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s).toLong
    val visit_length_30s_60s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s).toLong
    val visit_length_1m_3m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m).toLong
    val visit_length_3m_10m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m).toLong
    val visit_length_10m_30m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m).toLong
    val visit_length_30m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m).toLong

    val step_length_1_3 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3).toLong
    val step_length_4_6 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6).toLong
    val step_length_7_9 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9).toLong
    val step_length_10_30 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30).toLong
    val step_length_30_60 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60).toLong
    val step_length_60 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60).toLong

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

    // 调用对应的DAO插入统计结果
    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)
  }


}
