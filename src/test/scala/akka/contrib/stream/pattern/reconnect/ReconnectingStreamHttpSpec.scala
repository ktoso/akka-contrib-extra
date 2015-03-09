/**
 * Copyright (C) 2009-2015 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.stream.pattern.reconnect

import akka.actor.ActorSystem
import akka.event.Logging
import akka.event.Logging.Info
import akka.http.Http
import akka.http.client.RequestBuilding._
import akka.http.model.{ HttpRequest, HttpResponse }
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.testkit.{ TestKitExtension, TestProbe }
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

import scala.languageFeature.postfixOps
import scala.util.control.NoStackTrace

class ReconnectingStreamHttpSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {

  import scala.concurrent.duration._

  implicit val system = ActorSystem("test")

  implicit val timeout: Timeout = TestKitExtension(system).DefaultTimeout

  override protected def afterAll(): Unit = {
    system.shutdown()
    system.awaitTermination(timeout.duration)
  }

  val Localhost = "127.0.0.1"

  implicit val mat = ActorFlowMaterializer()

  "ReconnectingStreamHttp" must {

    "keep trying to connect" ignore {
      val p = TestProbe()

      val reconnectInterval = 200 millis
      val maxRetries = 4

      system.eventStream.subscribe(p.ref, classOf[Logging.Info])

      // notice how we keep the APIs similar:
      // val singleConnection = Http().outgoingConnection(Localhost)
      val initial = Http.reconnecting.outgoingConnection(Localhost, reconnectInterval, maxRetries) { connected =>
        Source.failed(new TestException("Acting as if unable to connect!"))
          .via(connected.flow)
          .runWith(Sink.ignore())
      }

      p.expectMsgType[Logging.Info].message.toString should startWith("Opening initial connection to: /127.0.0.1:80")
      (1 to maxRetries) foreach { i =>
        p.expectMsgType[Info].message.toString should startWith("Connection to localhost/127.0.0.1:80 was closed abruptly, reconnecting!")

        val reconncetingMsg = p.expectMsgType[Info].message.toString
        reconncetingMsg should startWith("Reconnecting to localhost/127.0.0.1:80")
        reconncetingMsg should include(s", ${maxRetries - i} retries remaining")
      }

      p.expectNoMsg(2.seconds)

      initial.futureValue // it must be completed
    }

    "connect to server, which dies, so it should reconnect" in {
      val p = TestProbe()
      val testPort = 1337

      val server = Http().bindAndstartHandlingWith(Flow[HttpRequest].map(_ => HttpResponse(200, entity = "OK")), "127.0.0.1", port = testPort)

      val serverAddress = server.futureValue.localAddress
      println("serverAddress = " + serverAddress)

      Http.reconnecting.outgoingConnection(serverAddress.getHostName, 200.millis, port = serverAddress.getPort) { connected =>
        println("connected = " + connected)
        Source(() => Iterator.continually(Get(s"http://127.0.0.1:${serverAddress.getPort}")))
          .map { req => println("req = " + req); req }
          .via(connected.flow)
          .runForeach { response =>
            println("response = " + response)
          }
      }

      system.eventStream.subscribe(p.ref, classOf[Logging.Info])

      Thread.sleep(10000)
      server.futureValue.unbind()
      println("!!! unbind !!!")
      p.fishForMessage(10.seconds, "Awaiting reconnection") {
        case i: Info => i.message.toString.contains("Reconnecting")
        case i       => info(i.toString); false
      }
      Thread.sleep(10000)
    }

  }

  class TestException(msg: String) extends Exception(msg) with NoStackTrace
}

/*
[akkaContribExtra]> testOnly *Http*
[info] Formatting 1 Scala source {file:/Users/ktoso/code/akka-contrib-extra/}akkaContribExtra(compile) ...
[info] Reformatted 1 Scala source {file:/Users/ktoso/code/akka-contrib-extra/}akkaContribExtra(compile).
[info] Compiling 1 Scala source to /Users/ktoso/code/akka-contrib-extra/target/scala-2.11/classes...
[info] Compiling 2 Scala sources to /Users/ktoso/code/akka-contrib-extra/target/scala-2.11/classes...
serverAddress = /127.0.0.1:1337
[INFO] [03/09/2015 20:06:20.897] [test-akka.actor.default-dispatcher-3] [akka://test/user/reconnector-http-1] Opening initial connection to: localhost/127.0.0.1:1337
connected = HttpConnected(akka.stream.scaladsl.Flow@6cb7a2d8,<function0>)
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
!!! unbind !!!
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
[INFO] [03/09/2015 20:06:30.916] [test-akka.actor.default-dispatcher-16] [akka://test/system/IO-TCP-STREAM/server-1-%2F127.0.0.1%3A1337/$a] Message [akka.io.Tcp$Received] from Actor[akka://test/system/IO-TCP/selectors/$a/2#621593561] to Actor[akka://test/system/IO-TCP-STREAM/server-1-%2F127.0.0.1%3A1337/$a#683380400] was not delivered. [1] dead letters encountered. This logging can be turned off or adjusted with configuration settings 'akka.log-dead-letters' and 'akka.log-dead-letters-during-shutdown'.
[INFO] [03/09/2015 20:06:30.917] [test-akka.actor.default-dispatcher-16] [akka://test/system/IO-TCP-STREAM/server-1-%2F127.0.0.1%3A1337/$a] Message [akka.io.Tcp$Closed$] from Actor[akka://test/system/IO-TCP/selectors/$a/2#621593561] to Actor[akka://test/system/IO-TCP-STREAM/server-1-%2F127.0.0.1%3A1337/$a#683380400] was not delivered. [2] dead letters encountered. This logging can be turned off or adjusted with configuration settings 'akka.log-dead-letters' and 'akka.log-dead-letters-during-shutdown'.
[INFO] [03/09/2015 20:06:30.917] [test-akka.actor.default-dispatcher-16] [akka://test/user/$a/flow-2-14-Broadcast-flexiMerge] Message [akka.dispatch.sysmsg.Terminate] from Actor[akka://test/user/$a/flow-2-14-Broadcast-flexiMerge#72952581] to Actor[akka://test/user/$a/flow-2-14-Broadcast-flexiMerge#72952581] was not delivered. [3] dead letters encountered. This logging can be turned off or adjusted with configuration settings 'akka.log-dead-letters' and 'akka.log-dead-letters-during-shutdown'.
[INFO] [03/09/2015 20:06:30.917] [test-akka.actor.default-dispatcher-16] [akka://test/system/IO-TCP/selectors/$a/2] Message [akka.dispatch.sysmsg.DeathWatchNotification] from Actor[akka://test/system/IO-TCP/selectors/$a/2#621593561] to Actor[akka://test/system/IO-TCP/selectors/$a/2#621593561] was not delivered. [4] dead letters encountered. This logging can be turned off or adjusted with configuration settings 'akka.log-dead-letters' and 'akka.log-dead-letters-during-shutdown'.
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
req = HttpRequest(HttpMethod(GET),http://127.0.0.1:1337,List(),HttpEntity.Strict(none/none,ByteString()),HttpProtocol(HTTP/1.1))
[INFO] [03/09/2015 20:06:30.921] [test-akka.actor.default-dispatcher-5] [akka://test/user/$a/flow-2-8-Broadcast-flexiMerge] Message [akka.dispatch.sysmsg.Terminate] from Actor[akka://test/user/$a/flow-2-8-Broadcast-flexiMerge#-507467068] to Actor[akka://test/user/$a/flow-2-8-Broadcast-flexiMerge#-507467068] was not delivered. [5] dead letters encountered. This logging can be turned off or adjusted with configuration settings 'akka.log-dead-letters' and 'akka.log-dead-letters-during-shutdown'.
[INFO] [03/09/2015 20:06:30.922] [test-akka.actor.default-dispatcher-12] [akka://test/system/IO-TCP/selectors/$a/1] Message [akka.io.Tcp$Abort$] from Actor[akka://test/system/IO-TCP-STREAM/client-2-localhost%2F127.0.0.1%3A1337#-763583708] to Actor[akka://test/system/IO-TCP/selectors/$a/1#-9009950] was not delivered. [6] dead letters encountered. This logging can be turned off or adjusted with configuration settings 'akka.log-dead-letters' and 'akka.log-dead-letters-during-shutdown'.
[info] ReconnectingStreamHttpSpec:
[info] ReconnectingStreamHttp
[info] - must keep trying to connect !!! IGNORED !!!
[info] - must connect to server, which dies, so it should reconnect *** FAILED ***
[info]   java.lang.AssertionError: assertion failed: timeout (10 seconds) during fishForMessage, hint: Awaiting reconnection
 */