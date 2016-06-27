import com.guanghe.MongoDB
import org.mongodb.scala.model.Filters._

object Main extends App {
  val mongo = MongoDB("""
    mongodb://root:unitedmaster@dds-bp1866b723420d142.mongodb.rds.aliyuncs.com:3717,
    dds-bp1866b723420d141.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-1109427""", "test")

//  val mongo = MongoDB("10.8.8.8", "test")

  mongo.selectCollection("test")

  // test json(array)
  val json= """[{ "omg": "JSON source" }, {"omg": {"name": {"tt": "link..."}}}]"""
  val res = mongo.batchInsert(json)
  println(s"res - $res")

  // check result
  mongo.findPrint(exists("omg"))
}


