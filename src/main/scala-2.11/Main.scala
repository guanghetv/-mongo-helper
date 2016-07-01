import com.guanghe.MongoDB
import org.mongodb.scala.model.Filters._

object Main extends App {
//  val mongo = MongoDB("""root:unitedmaster@dds-bp1866b723420d142.mongodb.rds.aliyuncs.com:3717, dds-bp1866b723420d141.mongodb.rds.aliyuncs.com:3717/?authSource=admin&replicaSet=mgset-1109427""", "test")

  val mongo = MongoDB("mongodb://10.8.8.8:27017", "test")

  // col
  val test = mongo.getModel("test")
  test.config(batchSize = 10, seconds = 10)

  // test json(array)
  val json= """[{ "omg": "JSON source" }, {"omg": {"name": {"tt": "link..."}}}]"""
  var res = test.batchInsert(json)

  val json1= """[{ "omg": "JSON source" }, {"omg": {"name": {"tt": "link..."}}}]"""

  res = test.batchInsert(json1)

  val list = List[String]("""{"omg": "JSON source1"}""", """{ "omg": "JSON source2" }""")
  val res1 = test.batchInsert(list)

  // simulate seconds
  Thread.sleep(2000)

  test.flush()
  test.flush()

  // check result
  test.findPrint(exists("omg"))

  mongo.close()
}


