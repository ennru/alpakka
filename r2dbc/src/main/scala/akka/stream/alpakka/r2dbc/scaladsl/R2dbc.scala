/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.r2dbc.scaladsl

import scala.concurrent.{ExecutionContext, Future}
import akka.Done
import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.r2dbc.scaladsl.R2dbc.doQuery
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import io.r2dbc.spi.{Connection, ConnectionFactories, ConnectionFactory, Result, Row, RowMetadata, Statement}

import scala.collection.immutable

object R2dbc {

// ConnectionPool is a ConnectionFactory, but brings in Reactor
//  import io.r2dbc.pool.ConnectionPool
//  import io.r2dbc.pool.ConnectionPoolConfiguration
//  import java.time.Duration
//  val configuration: ConnectionPoolConfiguration =
//    ConnectionPoolConfiguration.builder(connectionFactory).maxIdleTime(Duration.ofMillis(1000)).maxSize(20).build
//
//  val pool = new ConnectionPool(configuration)

  type ParamBinder = Statement => Unit

  type FlowParamBinder[I] = (I, Statement) => Unit

  implicit class RowReader[T](row: Row) {
    def getString(name: String): String = row.get(name, classOf[java.lang.String])
    def getInt(name: String): Int = row.get(name, classOf[java.lang.Integer])
    def getBoolean(name: String): Boolean = row.get(name, classOf[java.lang.Boolean])
  }

  def withConnection(connectionFactory: ConnectionFactory,
                     q: (Connection) => Source[Result, NotUsed]): Source[Result, Future[Done]] = {
    Source
      .setup { (mat, _) =>
        val value: Source[Result, NotUsed] = Source
          .fromPublisher(connectionFactory.create())
          .flatMapConcat[Result, Future[Done]] { connection =>
            q(connection)
              .watchTermination()(Keep.right)
              .mapMaterializedValue { termination =>
                termination.transformWith { _ =>
                  Source.fromPublisher(connection.close()).runWith(Sink.ignore)(mat)
                }(mat.system.dispatcher)
              }
          }
        value
      }
      .mapMaterializedValue(_.map(_ => Done)(ExecutionContexts.parasitic))

  }

  def withConnectionFlow[I](connectionFactory: ConnectionFactory,
                            q: Flow[(I, Connection), (I, Result), NotUsed]): Flow[I, (I, Result), Future[Done]] = {
    Flow
      .setup { (mat, attr) =>
        val connectionSource = Source
          .fromPublisher(connectionFactory.create())
          .runWith(Sink.head)(mat)
        Flow[I]
          .mapAsync(1) { i =>
            connectionSource.map(c => (i, c))(mat.executionContext)
          }
          .via(q)
          .watchTermination()(Keep.right)
          .mapMaterializedValue { f =>
            f.transform { tr =>
              connectionSource.map { c =>
                Source.fromPublisher(c.close()).runWith(Sink.ignore)(mat)
              }(mat.executionContext)
              tr
            }(mat.executionContext)
          }

      }
      .mapMaterializedValue(_.flatten)

  }

  def flow[I](connectionFactory: ConnectionFactory): Flow[I, Result, NotUsed] =
    withConnectionFlow(connectionFactory)

  def query[T](connectionFactory: ConnectionFactory,
               sql: String,
               params: ParamBinder,
               read: (Row, RowMetadata) => T): Source[T, Future[Done]] = {
    query2(connectionFactory, (c: Connection) => {
      val statement = c.createStatement(sql)
      params(statement)
      statement
    }, read)
  }

  def query2[T](connectionFactory: ConnectionFactory,
                prepareStatement: (Connection) => Statement,
                read: (Row, RowMetadata) => T): Source[T, Future[Done]] = {
    withConnection(connectionFactory, c => Source.fromPublisher(prepareStatement(c).execute()))
      .flatMapConcat { result =>
        Source.fromPublisher {
          result.map {
            case (row, rowMetadata) =>
              read(row, rowMetadata)
          }
        }
      }
  }

  def queryFlow[I, T](connectionFactory: ConnectionFactory,
                      sql: String,
                      params: FlowParamBinder[I],
                      read: (Row, RowMetadata) => T): Flow[I, T, NotUsed] = {
    Flow[I]
      .flatMapConcat { in =>
        query(connectionFactory, sql, params(in, _), read)
      }
  }
}
