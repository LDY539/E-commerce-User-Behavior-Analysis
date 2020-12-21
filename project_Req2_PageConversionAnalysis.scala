package project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object project_Req2_PageConversionAnalysis {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local[*]").setAppName("PageConversionAnalysis")
    val sc = new SparkContext(sparConf)

    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionDataRDD.cache()


    val ids = List[Long](1,2,3,4,5,6,7,8,9,10)
    val PageConversionIds: List[(Long, Long)] = ids.zip(ids.tail)

    val pageidToCountMap: Map[Long, Long] = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

        val ConversionIds: List[Long] = sortList.map(_.page_id)
        val PageIds: List[(Long, Long)] = ConversionIds.zip(ConversionIds.tail)


        PageIds.filter(
          t => {
            PageConversionIds.contains(t)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
      }
    )

    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list=>list)

    val dataRDD = flatRDD.reduceByKey(_+_)


    dataRDD.foreach{
      case ( (pageid1, pageid2), sum ) => {
        val lon: Long = pageidToCountMap.getOrElse(pageid1, 0L)

        println(s"Page Conversion between page ${pageid1} and page ${pageid2} is:" + (sum.toDouble/lon))
      }
    }


    sc.stop()
  }


  case class UserVisitAction(
                              date: String,
                              user_id: Long,
                              session_id: String,
                              page_id: Long,
                              action_time: String,
                              search_keyword: String,
                              click_category_id: Long,
                              click_product_id: Long,
                              order_category_ids: String,
                              order_product_ids: String,
                              pay_category_ids: String,
                              pay_product_ids: String,
                              city_id: Long
                            )
}
