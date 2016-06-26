import com.guanghe.MongoDB
import org.mongodb.scala.model.Filters._

object Main extends App {
  val mongo = MongoDB("10.8.8.8", 27017, "test")
  mongo.selectCollection("test")

  // test json(array)
  val json= """[{ "omg": "JSON source" }, {"omg": {"name": {"tt": "link..."}}}]"""
  val res = mongo.batchInsert(json)
  println(s"res - $res")

  // check result
  mongo.findPrint(exists("omg"))
}


