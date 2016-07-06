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
import org.mongodb.scala.connection._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class MongoDB(connStr: String, database: String = "") {
  val connectionString = new ConnectionString(connStr)
  val settings = MongoClientSettings.builder()
    .codecRegistry(DEFAULT_CODEC_REGISTRY)
    .clusterSettings(ClusterSettings.builder().applyConnectionString(connectionString).build())
    .connectionPoolSettings(ConnectionPoolSettings.builder()
      .minSize(50).maxSize(500).maxWaitQueueSize(1000).applyConnectionString(connectionString).build())
    .serverSettings(ServerSettings.builder().build())
    .credentialList(connectionString.getCredentialList)
    .sslSettings(SslSettings.builder().applyConnectionString(connectionString).build())
    .socketSettings(SocketSettings.builder().applyConnectionString(connectionString).build())
    .build()

  var client: MongoClient = null
  var db: MongoDatabase = null

  try {
    client = MongoClient(settings)
    db = client.getDatabase(database)
  }
  catch {
    case e: Exception => {
      e.printStackTrace()
      throw e
    }
  }

  def getModel(collection: String): CollectionModel = {
     new CollectionModel(db, collection)
  }

  def close() = {
    client.close()
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

class CollectionModel(db: MongoDatabase, name: String) {
  val collection = db.getCollection(name)
  var cache = List[Document]()

  var opTime = 0L
  var batchSize = 100
  var seconds = 2
  private object lock

  def config(batchSize: Int, seconds: Int) = {
    this.batchSize = batchSize
    this.seconds = seconds
    opTime= System.currentTimeMillis / 1000 + seconds
  }

  def flush(): Unit = {
    import Helpers._
    if (cache.length == 0) return
    collection.insertMany(cache).results()
    cache = List[Document]()
  }

  def batchInsert(json: String) = lock.synchronized {
    import Helpers._

    val children = parse(json).children
    val docs: IndexedSeq[Document] = children.map(d => Document(compact(render(d)))).toIndexedSeq

    cache = cache ++ docs

    // check
    if (opTime < (System.currentTimeMillis / 1000) || batchSize < cache.length) {
      // clear cache

      val docs = cache
      cache = List[Document]()

      this.collection.insertMany(docs).results()
      opTime = System.currentTimeMillis / 1000 + seconds
    }
  }

  def batchInsert(list: List[String]) = {
    import Helpers._

    val docs: IndexedSeq[Document] = list.map(doc => Document(doc)).toIndexedSeq
    cache = cache ++ docs

    if (opTime < (System.currentTimeMillis / 1000) || batchSize < cache.length) {
      // clear cache
      val docx = cache
      cache = List[Document]()

      this.collection.insertMany(docx).results()
      opTime = System.currentTimeMillis / 1000 + seconds
    }
  }

  def findPrint(filter: Bson) = {
    import Helpers._

    collection.find(filter).printResults()
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
        Await.result(observable.toFuture(), Duration(30, TimeUnit.SECONDS))
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
        Await.result(observable.head(), Duration(30, TimeUnit.SECONDS))
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
