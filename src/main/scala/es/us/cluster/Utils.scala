package es.us.cluster

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD

/**
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object Utils {

  def whatTimeIsIt(): String = {
    return new SimpleDateFormat("yyyyMMddhhmm").format(Calendar.getInstance().getTime())
  }

  def whatDayIsIt(): String = {
    return new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())
  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }

  def calculateMedian(listado: List[Double]): Double = {

    val count = listado.length

    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (listado.apply(l) + listado.apply(r)) / 2
    } else listado.apply(count / 2)

    return median

  }

  def printRDD(dataRDD: RDD[Unit], nameFile: String): Unit = {
    new PrintWriter(nameFile) {
      dataRDD.foreach(println)
      close
    }
  }

}
