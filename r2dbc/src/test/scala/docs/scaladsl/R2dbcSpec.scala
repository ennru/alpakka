/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.r2dbc.scaladsl.R2dbc
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.stream.scaladsl._
import akka.testkit.TestKit
import io.r2dbc.spi.{Connection, ConnectionFactories, ConnectionFactory, ConnectionFactoryMetadata, Statement}
import org.reactivestreams.Publisher
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class R2dbcSpec
    extends AnyWordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Matchers
    with LogCapturing {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val connectionFactory: ConnectionFactory =
    ConnectionFactories.get("r2dbc:h2:file:///./target/database?DB_CLOSE_DELAY=1")
//  val connectionFactory: ConnectionFactory = ConnectionFactories.get("r2dbc:h2:mem:///testdb?MULTI_THREADED=4")

  case class User(id: Int, name: String)

  implicit val ec = system.dispatcher
  implicit val defaultPatience = PatienceConfig(timeout = 3.seconds, interval = 50.millis)

  val users = (1 to 40).map(i => User(i, s"Name$i"))

  val createTable = """CREATE TABLE ALPAKKA_USERS(ID INTEGER, NAME VARCHAR(50))"""
  val dropTable = """DROP TABLE ALPAKKA_USERS"""
  val insertUser = """INSERT INTO ALPAKKA_USERS VALUES($1, $2)"""
  val selectAllUsers = "SELECT ID, NAME FROM ALPAKKA_USERS"

  private def execute(sql: String): Future[Done] =
    R2dbc
      .query(connectionFactory, sql, (s: Statement) => {}, (row, meta) => {
        println(sql + ":  " + meta)
        ()
      })
      .runWith(Sink.ignore)

  def populate() = {
    Source(users)
      .via(R2dbc.queryFlow(connectionFactory, insertUser, (u: User, s: Statement) => {
        s.bind(0, u.id)
        s.bind(1, u.name)
      }, (row, meta) => ()))
      .runWith(Sink.ignore)
      .futureValue

  }
//
  override def beforeEach(): Unit = execute(createTable).futureValue
  override def afterEach(): Unit = execute(dropTable).futureValue

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "R2dbc" should {
    "create connection and select a constant" in {
      val result =
        R2dbc
          .query(connectionFactory, "select true", (s: Statement) => {}, (row, meta) => {
            row.get(0, classOf[java.lang.Boolean])
          })
          .runWith(Sink.head)

      result.futureValue shouldBe true
    }
  }

  "Slick.source(...)" must {

    "stream the result of a Slick plain SQL query" in {
      populate()
      val result =
        R2dbc
          .query(
            connectionFactory,
            selectAllUsers,
            params = (s: Statement) => {},
            read = (row, meta) => {
              val id = row.get("ID", classOf[java.lang.Integer])
              val name = row.get("NAME", classOf[java.lang.String])
              User(id, name)
            }
          )
          .runWith(Sink.seq)
      result.futureValue should contain theSameElementsAs users
    }

    "stream the result of a Slick plain SQL query (prepareStatement)" in {
      populate()
      val result =
        R2dbc
          .query2(
            connectionFactory,
            (c: Connection) => c.createStatement(selectAllUsers),
            (row, meta) => {
              val id = row.get("ID", classOf[java.lang.Integer])
              val name = row.get("NAME", classOf[java.lang.String])
              User(id, name)
            }
          )
          .runWith(Sink.seq)
      result.futureValue should contain theSameElementsAs users
    }

    "insert/select" in {
      val result =
        R2dbc
          .query2(
            connectionFactory,
            (c: Connection) => c.createStatement(selectAllUsers),
            (row, meta) => {
              val id = row.get("ID", classOf[java.lang.Integer])
              val name = row.get("NAME", classOf[java.lang.String])
              User(id, name)
            }
          )
          .runWith(Sink.seq)
      result.futureValue should contain theSameElementsAs users
    }

    "stream the result of a Slick plain SQL query that results in no data" in {
      val result =
        R2dbc
          .query(
            connectionFactory,
            selectAllUsers,
            (s: Statement) => {},
            (row, meta) => {
              val id = row.get("ID", classOf[java.lang.Integer])
              val name = row.get("NAME", classOf[java.lang.String])
              User(id, name)
            }
          )
          .runWith(Sink.seq)
      result.futureValue shouldBe Symbol("empty")
    }

    "support multiple materializations" in {
      populate()

      val source = R2dbc
        .query(
          connectionFactory,
          selectAllUsers,
          (s: Statement) => {},
          (row, meta) => {
            import akka.stream.alpakka.r2dbc.scaladsl.R2dbc.RowReader
            val id = row.getInt("ID")
            val name = row.getString("NAME")
            User(id, name)
          }
        )

      source.runWith(Sink.seq).futureValue should contain theSameElementsAs users
      source.runWith(Sink.seq).futureValue should contain theSameElementsAs users
    }
  }
//
//  "Slick.flow(..)" must {
//    "insert 40 records into a table (no parallelism)" in {
//      //#init-db-config-session
//      val databaseConfig = DatabaseConfig.forConfig[JdbcProfile]("slick-h2")
//      implicit val session = SlickSession.forConfig(databaseConfig)
//      //#init-db-config-session
//
//      val inserted = Source(users)
//        .via(R2dbc.flow(insertUser))
//        .runWith(Sink.seq)
//        .futureValue
//
//      inserted must have size (users.size)
//      inserted.toSet mustBe Set(1)
//
//      getAllUsersFromDb.futureValue mustBe users
//    }
//
//    "insert 40 records into a table (parallelism = 4)" in {
//      val inserted = Source(users)
//        .via(R2dbc.flow(parallelism = 4, insertUser))
//        .runWith(Sink.seq)
//        .futureValue
//
//      inserted must have size (users.size)
//      inserted.toSet mustBe Set(1)
//
//      getAllUsersFromDb.futureValue mustBe users
//    }
//
//    "insert 40 records into a table faster using Flow.grouped (n = 10, parallelism = 4)" in {
//      val inserted = Source(users)
//        .grouped(10)
//        .via(
//          R2dbc.flow(parallelism = 4, (group: Seq[User]) => group.map(insertUser(_)).reduceLeft(_.andThen(_)))
//        )
//        .runWith(Sink.seq)
//        .futureValue
//
//      inserted must have size (4)
//      // we do single inserts without auto-commit but it only returns the result of the last insert
//      inserted.toSet mustBe Set(1)
//
//      getAllUsersFromDb.futureValue mustBe users
//    }
//  }
//
//  "Slick.flowWithPassThrough(..)" must {
//    "insert 40 records into a table (no parallelism)" in {
//      val inserted = Source(users)
//        .via(R2dbc.flowWithPassThrough { user =>
//          insertUser(user).map(insertCount => (user, insertCount))
//        })
//        .runWith(Sink.seq)
//        .futureValue
//
//      inserted must have size (users.size)
//      inserted.map(_._1).toSet mustBe (users)
//      inserted.map(_._2).toSet mustBe Set(1)
//
//      getAllUsersFromDb.futureValue mustBe users
//    }
//
//    "insert 40 records into a table (parallelism = 4)" in {
//      val inserted = Source(users)
//        .via(R2dbc.flowWithPassThrough(parallelism = 4, user => {
//          insertUser(user).map(insertCount => (user, insertCount))
//        }))
//        .runWith(Sink.seq)
//        .futureValue
//
//      inserted must have size (users.size)
//      inserted.map(_._1).toSet mustBe (users)
//      inserted.map(_._2).toSet mustBe Set(1)
//
//      getAllUsersFromDb.futureValue mustBe users
//    }
//
//    "insert 40 records into a table faster using Flow.grouped (n = 10, parallelism = 4)" in {
//      val inserted = Source(users)
//        .grouped(10)
//        .via(
//          R2dbc.flowWithPassThrough(
//            parallelism = 4,
//            (group: Seq[User]) => {
//              val groupedDbActions = group.map(user => insertUser(user).map(insertCount => Seq((user, insertCount))))
//              DBIOAction.fold(groupedDbActions, Seq.empty[(User, Int)])(_ ++ _)
//            }
//          )
//        )
//        .runWith(Sink.fold(Seq.empty[(User, Int)])((a, b) => a ++ b))
//        .futureValue
//
//      inserted must have size (users.size)
//      inserted.map(_._1).toSet mustBe (users)
//      inserted.map(_._2).toSet mustBe Set(1)
//
//      getAllUsersFromDb.futureValue mustBe users
//    }
//
//    "kafka-example - store documents and pass Responses with passThrough" in {
//
//      //#kafka-example
//      // We're going to pretend we got messages from kafka.
//      // After we've written them to a db with Slick, we want
//      // to commit the offset to Kafka
//
//      case class KafkaOffset(offset: Int)
//      case class KafkaMessage[A](msg: A, offset: KafkaOffset) {
//        // map the msg and keep the offset
//        def map[B](f: A => B): KafkaMessage[B] = KafkaMessage(f(msg), offset)
//      }
//
//      val messagesFromKafka = users.zipWithIndex.map { case (user, index) => KafkaMessage(user, KafkaOffset(index)) }
//
//      var committedOffsets = List[KafkaOffset]()
//
//      def commitToKafka(offset: KafkaOffset): Future[Done] = {
//        committedOffsets = committedOffsets :+ offset
//        Future.successful(Done)
//      }
//
//      val f1 = Source(messagesFromKafka) // Assume we get this from Kafka
//        .via( // write to db with Slick
//          R2dbc.flowWithPassThrough { kafkaMessage =>
//            insertUser(kafkaMessage.msg).map(insertCount => kafkaMessage.map(_ => insertCount))
//          }
//        )
//        .mapAsync(1) { kafkaMessage =>
//          if (kafkaMessage.msg == 0) throw new Exception("Failed to write message to db")
//          // Commit to kafka
//          commitToKafka(kafkaMessage.offset)
//        }
//        .runWith(Sink.seq)
//
//      Await.ready(f1, Duration.Inf)
//
//      // Make sure all messages was committed to kafka
//      committedOffsets.map(_.offset).sorted mustBe ((0 until (users.size)).toList)
//
//      // Assert that all docs were written to db
//      getAllUsersFromDb.futureValue mustBe users
//    }
//  }
//
//  }

}
