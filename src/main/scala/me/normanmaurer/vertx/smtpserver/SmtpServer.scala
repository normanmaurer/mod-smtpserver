package me.normanmaurer.vertx.smtpserver

import org.vertx.scala.platform.Verticle
import scala.concurrent.Promise
import org.vertx.scala.core.net.{NetServer, NetSocket}
import org.vertx.java.core.AsyncResult
import org.vertx.scala.core.parsetools.RecordParser
import org.vertx.scala.core.buffer.Buffer
import org.vertx.java.core.json.JsonObject
import org.vertx.java.core.parsetools.{RecordParser => JRecordParser}
import org.vertx.scala.core.eventbus.Message
import java.util.UUID

class SmtpServer extends Verticle {
  val crlf = "\r\n"

  override def start(promise: Promise[Unit]): Unit = {
    val config = container.config()
    val address = config.getString("ipaddress", "0.0.0.0")
    val port = config.getInteger("port", 1025)
    val name = config.getString("name", "mod-smtpserver")
    val eventBusAddress = config.getString("address", "mod-smtpserver")
    val server = vertx.createNetServer()
    server.connectHandler { socket: NetSocket =>
      // use something better
      val id = UUID.randomUUID().toString
      val parser: JRecordParser = RecordParser.newDelimited(crlf, { buffer: Buffer =>
        val s = buffer.toString("US_ASCII")
        val parts = s.split(":", 2)
        parts.length match {
          case 2 => {
            parts(0).toUpperCase match {
              case "EHLO" => ehlo(socket, eventBusAddress, id, name, parts(2).trim)
              case "DATA" => {
                parser.delimitedMode(crlf + "." + crlf)
                writeResponse(socket, 354, "go ahead")
              }
              case "NOOP" => writeResponse(socket, 250, "ok")
              case _ => writeResponse(socket, 502,  "unimplemented")
            }
          }
          case _ => writeResponse(socket, 554, "Syntax error")
        }
      })
      socket.dataHandler { buffer: Buffer => parser.handle(buffer.toJava()) }

      // pause the socket until we know if it should be accepted or not
      socket.pause()
      // timeout ?
      vertx.eventBus.send(eventBusAddress +  ".connect", json(id, "CONNECT", socket.remoteAddress().toString), { msg: Message[JsonObject] =>
        msg.body().getInteger("action", 0) match {
          case 0 =>  {
            writeResponse(socket, 250, name + " ESMTP")
            socket.resume()
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

  def writeResponse(socket: NetSocket, msg: Message[JsonObject], success: String* ) {
    msg.body().getInteger("action", 0) match {
      case 0 => writeResponse(socket, 250, success:_*)
      case 1 => writeResponse(socket, 554, msg.body().getString("details", "Permanent error"))
      case _ => writeResponse(socket, 451, msg.body().getString("details", "Temporary error"))
    }
  }

  def writeResponse(socket: NetSocket, code: Int, msgs: String*) {
    val buffer = Buffer()
    msgs.dropRight(1).foreach {
      buffer.append(code + "-" + _+ crlf)
      return
    }
    msgs.last.map {
      buffer.append(code + " " + _ + crlf)
      return
    }
    socket.write(buffer)
  }

  def ehlo(socket: NetSocket, eventBusAddress: String, id: String, name: String, helo: String) {
    vertx.eventBus.send(eventBusAddress +  ".ehlo", json(id, "EHLO", helo), { msg: Message[JsonObject] =>
      writeResponse(socket, 250, name, "PIPELINING", "8BITMIME")
    })
  }
  
  def json(id: String, command: String, argument: String) = new JsonObject().putString("id", id)
    .putString("command", command).putString("argument", argument)
}
