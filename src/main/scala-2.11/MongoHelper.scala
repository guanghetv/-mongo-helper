/**
  * Created by jack on 16-6-22.
  */

package com.guanghe

import java.util.concurrent.TimeUnit

import com.mongodb.ConnectionString
import com.mongodb.connection.ConnectionPoolSettings
import org.json4s.native.JsonMethods._
import org.mongodb.scala._
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.connection.ClusterSettings

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MongoDB(connStr: String, database: String = "") {
  val poolSettings = ConnectionPoolSettings.builder().maxSize(500).minSize(50).maxWaitQueueSize(1000).build()

  val clusterSettings: ClusterSettings = ClusterSettings.builder()
    .applyConnectionString(new ConnectionString(s"mongodb://$connStr")).build()

  val settings: MongoClientSettings = MongoClientSettings.builder()
    .connectionPoolSettings(poolSettings)
    .clusterSettings(clusterSettings).build()

  val client: MongoClient = MongoClient(settings)

//  val client: MongoClient = MongoClient(s"mongodb://$connStr")

//  val client: MongoClient = MongoClient(s"mongodb://$host:$port")
  val db = client.getDatabase(database)
  var collection: MongoCollection[Document] = null

  // config
  var batchSize = 0
  var seconds = 0
  var opTime: Long = System.currentTimeMillis / 1000

  // cache
  var cache: List[Document] = List[Document]()

  def config(batchSize: Int, seconds: Int) = {
    this.batchSize= batchSize
    this.seconds = seconds
    this.opTime= System.currentTimeMillis / 1000 + seconds
  }

  def selectCollection(col: String): MongoCollection[Document] = {
    collection = db.getCollection(col)
    collection
  }

  def batchInsert(json: String) = {
    import Helpers._

    val ch = parse(json).children
    val docs: IndexedSeq[Document] = ch.map(d => Document(compact(render(d)))).toIndexedSeq
    cache = cache ++ docs

     // check
    if (opTime < (System.currentTimeMillis / 1000) || batchSize < cache.length) {
      collection.insertMany(cache).results()

      // clear cache
      cache = List[Document]()
      opTime = System.currentTimeMillis / 1000 + seconds
    }
  }

  def findPrint(filter: Bson) = {
    import Helpers._

    collection.find(filter).printResults()
  }
}

object MongoDB {
  var instance: MongoDB = null
  def apply(connStr: String, database: String) = {
    if (instance == null) {
      instance = new MongoDB(connStr, database)
    }
    instance
  }
}

object Helpers {
  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson()
  }

  implicit  class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString()
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = {
      try {
        Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
      }
      catch {
        case e: Exception => {
          e.printStackTrace()
          throw e
        }
      }
    }
    def headResult() = {
      try {
        Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
      }
      catch {
        case e: Exception => {
          e.printStackTrace()
          throw e
        }
      }
    }

    def printResults() = results().foreach(res => println(converter(res)))
  }
}
