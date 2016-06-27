import com.guanghe.MongoDB
import org.mongodb.scala.model.Filters._

object Main extends App {
//  val mongo = MongoDB("""root:unitedmaster@dds-bp1866b723420d142.mongodb.rds.aliyuncs.com:3717, dds-bp1866b723420d141.mongodb.rds.aliyuncs.com:3717/?authSource=admin&replicaSet=mgset-1109427""", "test")

  val mongo = MongoDB("10.8.8.8:27017", "test")

  mongo.config(2, 20)
  mongo.selectCollection("test")

  // test json(array)
  val json= """[{ "omg": "JSON source" }, {"omg": {"name": {"tt": "link..."}}}]"""
  var res = mongo.batchInsert(json)

  // simulate seconds
  Thread.sleep(2000)
  val json1= """[{ "omg": "JSON source" }, {"omg": {"name": {"tt": "link..."}}}]"""
  res = mongo.batchInsert(json1)

  println(s"res - $res")

  // check result
  mongo.findPrint(exists("omg"))
}


