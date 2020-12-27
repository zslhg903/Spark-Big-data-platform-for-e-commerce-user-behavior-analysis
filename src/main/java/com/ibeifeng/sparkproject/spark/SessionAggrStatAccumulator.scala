package com.ibeifeng.sparkproject.spark

import com.ibeifeng.sparkproject.constant.Constants
import com.ibeifeng.sparkproject.util.StringUtils
import org.apache.spark.AccumulatorParam

/**
  * session聚合统计Accumulator
  *
  * 可以使用自定义的数据 格式，比如String，我们可以自定义Model，自己定义的类（必须 可序列化）
  * 可以基于这种特殊的数据 格式（自定义的String格式），可以实现自己复杂 的分布式的计算逻辑
  * 各个task，分布式运行，可以根据你的需求，task给Accumulator 做复杂 的逻辑
  *
  * Spark Core里面实用的高端技术
  */
@SerialVersionUID(6311074555136039130L)
object SessionAggrStatAccumulator extends AccumulatorParam[String]{

  /**
    * 主要用于数据 的初始化
    * 那么，这里就返回一个值 ，就是初始化中，所有范围区间的数量 ，即 零0
    * @param initialValue
    * @return
    */
  override def zero(v: String): String = {
    return Constants.SESSION_COUNT + "=0|"+
     Constants.TIME_PERIOD_1s_3s + "=0|"+
      Constants.TIME_PERIOD_4s_6s + "=0|"+
     Constants.TIME_PERIOD_7s_9s + "=0|"+
     Constants.TIME_PERIOD_10s_30s + "=0|"+
      Constants.TIME_PERIOD_30s_60s + "=0|" +
    Constants.TIME_PERIOD_1m_3m + "=0|"+
     Constants.TIME_PERIOD_3m_10m + "=0|"+
    Constants.TIME_PERIOD_10m_30m + "=0|"+
     Constants.TIME_PERIOD_30m + "=0|"  +
    Constants.STEP_PERIOD_1_3 + "=0|" +
     Constants.STEP_PERIOD_4_6 + "=0|"+
    Constants.STEP_PERIOD_7_9 + "=0|"+
      Constants.STEP_PERIOD_10_30 + "=0|"+
      Constants.STEP_PERIOD_30_60 + "=0|" +
    Constants.STEP_PERIOD_60 + "=0"
  }

  /**
    *addInPlace和 addAccumulator可以理解 为一样的
    *
    * v1 就是初始化的连接串
    * v2 就是遍历 session时，判断 出某个session对应的区间，会用Constants.TIME_PERIOD_1s_3s
    * 在v1中，找到v2应用的value，累加1，然后再更新回连接串里面去
    * @param v1
    * @param v2
    * @return
    */
  override def addInPlace(v1: String, v2: String): String = {
    return add(v1,v2)
  }

  /**
    *
    * @param v1
    * @param v2
    * @return
    */
  override def addAccumulator(v1: String, v2: String): String = {
    return add(v1,v2)
  }

  /**
    * session统计计算逻辑
    * @param v1 连接串
    * @param v2 范围区间
    * @return
    */
  def add(v1:String,v2: String):String={
    //校验：v1为空的话，直接返回v2
    if(StringUtils.isEmpty(v1)){
      return v2
    }
    //使用StringUtils工具，从v1中，提取v2对应的值 ，并累加1
    val oldValue=StringUtils.getFieldFromConcatString(v1,"\\|",v2)
    if(oldValue!=null){
      val newValue=Integer.valueOf(oldValue)+1

      //使用StringUtils工具类，将v1中，v2对应的值 ，设置成新的累加后的值
      return StringUtils.setFieldInConcatString(v1,"\\|",v2,String.valueOf(newValue))
    }
    return v1
  }
}
