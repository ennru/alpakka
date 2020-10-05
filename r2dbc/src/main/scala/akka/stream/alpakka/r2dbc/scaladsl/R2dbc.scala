/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.r2dbc.scaladsl

import scala.concurrent.{ExecutionContext, Future}
import akka.Done
import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import io.r2dbc.spi.{Connection, ConnectionFactories, ConnectionFactory, Row, RowMetadata, Statement}

import scala.collection.immutable

object R2dbc {

  val connectionFactory: ConnectionFactory = ConnectionFactories.get("r2dbc:h2:mem:///testdb")

// ConnectionPool is a ConnectionFactory, but brings in Reactor
//  import io.r2dbc.pool.ConnectionPool
//  import io.r2dbc.pool.ConnectionPoolConfiguration
//  import java.time.Duration
//  val configuration: ConnectionPoolConfiguration =
//    ConnectionPoolConfiguration.builder(connectionFactory).maxIdleTime(Duration.ofMillis(1000)).maxSize(20).build
//
//  val pool = new ConnectionPool(configuration)

  trait ParamBinder {
    def bind(statement: Statement): Unit
  }

  trait FlowParamBinder[I] {
    def apply(in: I): ParamBinder
  }

  class MapBinder(params: Map[String, AnyRef], nulls: Map[String, Class[_]]) extends ParamBinder {

    def bind(statement: Statement): Unit = {
      params.foreach {
        case (name, null) =>
          throw new IllegalArgumentException(s"value of $name was null, the type must be specified")
        case (name, value) =>
          statement.bind(name, value)
      }
      nulls.foreach {
        case (name, clazz) =>
          statement.bindNull(name, clazz)
      }
    }
  }

  class PositionBinder(params: immutable.Seq[AnyRef]) extends ParamBinder {

    def bind(statement: Statement): Unit = {
      params.zipWithIndex {
        case (clazz: Class[_], pos: Int) =>
          statement.bindNull(pos, clazz)
        case (value, pos: Int) =>
          statement.bind(pos, value)
      }
    }
  }

  object ParamBinder {
    def apply(params: Map[String, AnyRef]) = new MapBinder(params, nulls = Map())
  }

  implicit class RowReader[T](row: Row) {
    def getString(name: String): String = row.get(name, classOf[String])
    def getInt(name: String): Int = row.get(name, classOf[Integer])
  }

  def query[T](connectionFactory: ConnectionFactory,
               sql: String,
               params: ParamBinder,
               read: (Row, RowMetadata) => T): Source[T, NotUsed] = {
    Source
      .fromPublisher(connectionFactory.create())
      .flatMapConcat { c =>
        val q = doQuery(c, sql, params)
        q.watchTermination() { (_, future) =>
          future.onComplete { _ =>
            c.close()
          }(ExecutionContexts.parasitic)
        }
        q
      }
      .flatMapConcat { r =>
        Source.fromPublisher {
          r.map {
            case (row, rowMetadata) =>
              read(row, rowMetadata)
          }
        }
      }
  }

  private def doQuery[T](c: Connection, sql: String, params: ParamBinder) = {
    val statement = c.createStatement(sql)
    params.bind(statement)
    Source.fromPublisher(statement.execute())
  }

  def queryFlow[I, T](connectionFactory: ConnectionFactory,
                      sql: String,
                      params: FlowParamBinder[I],
                      read: (Row, RowMetadata) => T): Flow[I, T, NotUsed] = {
    Flow
      .setup { (mat, attr) =>
        Flow[I]
          .flatMapConcat { in =>
            query(connectionFactory, sql, params(in), read)
          }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  val s: Source[String, NotUsed] =
    query(connectionFactory,
          "SELECT firstname FROM PERSON WHERE age > $1",
          ParamBinder(Map("$1" -> 42)),
          (row, rowMetadata) => {
            row.getString("firstname")
          })

}
