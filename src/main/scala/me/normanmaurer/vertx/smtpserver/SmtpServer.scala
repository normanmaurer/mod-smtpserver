package me.normanmaurer.vertx.smtpserver

import org.vertx.scala.platform.Verticle
import scala.concurrent.Promise
import org.vertx.scala.core.net.{NetServer, NetSocket}
import org.vertx.java.core.AsyncResult
import org.vertx.scala.core.parsetools.RecordParser
import org.vertx.scala.core.buffer.Buffer
import org.vertx.java.core.eventbus.Message
import org.vertx.java.core.json.JsonObject
import org.vertx.java.core.parsetools.{RecordParser => JRecordParser}

class SmtpServer extends Verticle {
  override def start(promise: Promise[Unit]): Unit = {
    val config = container.config()
    val address = config.getString("ipaddress", "0.0.0.0")
    val port = config.getInteger("port", 1025)
    val name = config.getString("name", "mod-smtpserver")
    val eventBusAddress = config.getString("address", "mod-smtpserver")
    val server = vertx.createNetServer()
    server.connectHandler { socket: NetSocket =>
      val id = ""
      val parser: JRecordParser = RecordParser.newDelimited("\r\n", { buffer: Buffer =>
        val s = buffer.toString("US_ASCII")
        val parts = s.split(":", 2)
        parts.length match {
          case 2 => {
            parts(0).toUpperCase match {
              case "HELO" => helo(eventBusAddress, id, name, parts(2).trim)
              case "MAIL FROM" => mail(eventBusAddress, id, parts(2).trim)
              case "RCPT TO" => rcpt(eventBusAddress, id, rcpt(parts(2).trim))
              case "DATA" => parser.delimitedMode("\r\n.\r\n")
              case _ => socket.write("502 unimplemented\r\n")
            }
          }
          case _ => socket.write("554 Syntax error\r\n");
        }

      })
      socket.dataHandler { buffer: Buffer => parser.handle(buffer.toJava()) }

      // pause the socket until we know if it should be accepted or not
      socket.pause()

      val json = new JsonObject()
      json.putString("id", "0")
      json.putString("command", "CONNECT")
      json.putString("value", socket.remoteAddress())
      // timeout ?
      vertx.eventBus.send("mod-smtpserver", json, { msg: Message[JsonObject] =>
       msg.body().getInteger("action", 0) match {
         case 0 =>  {
           socket.resume()
           socket.write("220 " + name + " ESMTP\r\n")
         }
         case _ => socket.close()
       }
      })
    }
    server.listen(port, address, { result: AsyncResult[NetServer] =>
      result.succeeded match {
        case true => promise.success(None)
        case false => promise.failure(result.cause())
      }
    })
  }

  def helo(eventBusAddress: String, id: String, name: String, helo: String): Unit = {
    val json = new JsonObject()
    json.putString("id", id)
    json.putString("command", "HELO")
    json.putString("value", helo)
    vertx.eventBus.send(eventBusAddress, json, { msg: Message[JsonObject] =>
      msg.body().getInteger("action", 0) match {
        case 0 => socket.write("250-" + name + "\r\n + 250 PIPELINING\r\n")
        case 1 => {
          val details = msg.body().getString("details", "Permanent error")
          socket.write("554 " + details + "\r\n")
        }
        case _ => {
          val details = msg.body().getString("details", "Temporary error")
          socket.write("451 " + details + "\r\n")
        }
      }
    })
  }

  def mail(eventBusAddress: String, id: String, from: String): Unit => {

  }

  def rcpt(eventBusAddress: String, id: String, to: String): Unit => {

  }


  def data_complete(eventBusAddress: String, id: String, buf: Buffer): Unit => {

  }
}
