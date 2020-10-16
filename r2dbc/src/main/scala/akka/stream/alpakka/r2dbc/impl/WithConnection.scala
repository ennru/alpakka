package akka.stream.alpakka.r2dbc.impl

import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}
import io.r2dbc.spi.{Connection, ConnectionFactory}

final class WithConnection[In, Out](connectionFactory: ConnectionFactory)
    extends GraphStage[BidiShape[In, (Connection, In), Out, Out]] {
  private val externalIn = Inlet[In]("WithConnection.externalIn")
  private val externalOut = Outlet[Out]("WithConnection.externalOut")

  private val internalOut = Outlet[(Connection, In)]("WithConnection.internalOut")
  private val internalIn = Inlet[Out]("WithConnection.internalIn")

  override val shape: BidiShape[In, (Connection, In), Out, Out] =
    BidiShape(externalIn, internalOut, internalIn, externalOut)

  override def createLogic(attributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var connectionRequested = false
    private val connectionAvailable = getAsyncCallback[Connection](install)

    setHandler(
      externalIn,
      handler = new InHandler {
        override def onPush(): Unit =
          throw new IllegalStateException("external push before a connection was available")

        override def onUpstreamFinish(): Unit =
          if (!connectionRequested) {
            completeStage()
          } else {
            // completed by Inhandler
          }

        override def onUpstreamFailure(ex: Throwable): Unit =
          if (!connectionRequested) {
            super.onUpstreamFailure(ex)
          } else {
            ???
          }
      }
    )

    setHandler(
      internalOut,
      new OutHandler {

        override def onPull(): Unit = {
          pull(externalIn)
        }

        override def onDownstreamFinish(): Unit = {
          super.onDownstreamFinish()
        }

      }
    )

    setHandler(
      internalIn,
      new InHandler {
        override def onPush(): Unit = {
          throw new IllegalStateException("internalIn push before connection")
        }

        override def onUpstreamFinish(): Unit = super.onUpstreamFinish()
      }
    )

    setHandler(
      externalOut,
      new OutHandler {
        override def onPull(): Unit = {
          connectionRequested = true
          Source
            .fromPublisher(connectionFactory.create())
            .runWith(Sink.foreach(connectionAvailable.invoke))(materializer)
        }

        override def onDownstreamFinish(): Unit = {
          if (!connectionRequested) {
            super.onDownstreamFinish()
          } else {
            // close connection and cancel
          }
        }
      }
    )

    private def install(connection: Connection): Unit = {
      // if completed release connection and complete

      setHandler(
        externalIn,
        new InHandler {
          override def onPush(): Unit = {
            push(internalOut, (connection, grab(externalIn)))
          }

          override def onUpstreamFinish(): Unit = {
            closeConnection(connection, _ => super.onUpstreamFinish())
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            closeConnection(connection, _ => super.onUpstreamFailure(ex))
          }
        }
      )

      setHandler(
        internalIn,
        new InHandler {
          override def onPush(): Unit = {
            val result = grab(internalIn)
            push(externalOut, result)
            if (isClosed(externalIn)) {
              closeConnection(connection, _ => completeStage())
            }
          }
        }
      )

      setHandler(
        externalOut,
        new OutHandler {
          override def onPull(): Unit =
            pull(internalIn)

          override def onDownstreamFinish(): Unit = {
            closeConnection(connection, _ => super.onDownstreamFinish())
          }

        }
      )

      pull(internalIn)
    }

    private def closeConnection(connection: Connection, exec: Unit => Unit): Unit = {
      val than: AsyncCallback[Unit] = getAsyncCallback[Unit](exec)
      Source
        .fromPublisher(connection.close())
        .runWith(Sink.ignore)(materializer)
        .onComplete { _ =>
          than.invoke()
        }(materializer.executionContext)
    }
  }
}
