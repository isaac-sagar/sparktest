package functions

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.security.MessageDigest
import javax.xml.bind.annotation.adapters.HexBinaryAdapter
import scala.io.Source


object Encoder {

  val toUpper = (orderId:String, raw:String) =>
  {
    val result = {
      if (orderId.toDouble % 2 == 0)
      {
        var res = raw
        for (i <- 0 to raw.count(_ == 'A'))
        {
          res = (new HexBinaryAdapter()).marshal(MessageDigest.getInstance("SHA-256").digest(res.getBytes("UTF-8")))
        }
        res
      }
      else
      {
        (new HexBinaryAdapter()).marshal(MessageDigest.getInstance("MD5").digest(raw.getBytes("UTF-8")))
      }
    }
    result
  }

}
