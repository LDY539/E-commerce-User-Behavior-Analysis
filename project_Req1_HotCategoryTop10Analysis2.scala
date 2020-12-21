package project

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object project_Req1_HotCategoryTop10Analysis2 {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis2")
    val sc = new SparkContext(sparConf)

    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    val acc = new HotCategoryAccumulator
    sc.register(acc, "HotCategory")

    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {

          acc.add((datas(6), "click"))
        } else if (datas(8) != "null") {

          val ids = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add( (id, "order") )
            }
          )
        } else if (datas(10) != "null") {

          val ids = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add( (id, "pay") )
            }
          )
        }
      }
    )

    val accVal: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)

    val sort = categories.toList.sortWith(
      (left, right) => {
        if ( left.clickCnt > right.clickCnt ) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if ( left.orderCnt > right.orderCnt ) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )

    sort.take(10).foreach(println)

    sc.stop()
  }
  case class HotCategory( cid:String, var clickCnt : Int, var orderCnt : Int, var payCnt : Int )

  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]{

    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid = v._1
      val actionType = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0,0,0))
      if ( actionType == "click" ) {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1 = this.hcMap
      val map2 = other.value

      map2.foreach{
        case ( cid, hc ) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0,0,0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }
}

