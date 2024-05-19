package cicero.serve


import cicero.statements.DB2Connector

import java.io.PrintStream
import java.net.{ServerSocket, Socket}
import scala.annotation.experimental
import scala.io.BufferedSource
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import cicero.http.HttpRequestParser
@experimental
trait Servable:



  def listen(port: Int): Unit = {
    val server = new ServerSocket(port)
    while true do
      val s = server.accept()

      Future { handleRequest(s) } onComplete{
        case Success(value) => println("Finsihed handling Request")
        case Failure(exception) => exception.printStackTrace()
      }
  }

  def handleRequest(socket: Socket): Unit = {
    val inputStream = socket.getInputStream()
    println("got  stream")

    val request = HttpRequestParser.parse(inputStream)

    println("parsing requests")


    val in = new BufferedSource(socket.getInputStream()).getLines()
    val out = new PrintStream(socket.getOutputStream())



   // while Try(in.hasNext).getOrElse(false) do
     // println(in.next())
     val start = """HTTP/1.1 200 OK
       |Content-Type: text/plain
       |Transfer-Encoding: chunked
       |
       |""".stripMargin
    val chunked = """HTTP/1.1 200 OK
                    |Content-Type: text/plain
                    |Transfer-Encoding: chunked
                    |
                    |7
                    |Mozilla
                    |0
                    |
                    |""".stripMargin

    //out.write(chunked.getBytes("utf-8"))
    //out.write("0\r\n\r\n".getBytes("utf-8"))

    
    out.write(start.getBytes("utf-8"))
    out.flush()

    DB2Connector.getUsers(out)
    out.write("0\r\n\r\n".getBytes("utf-8"))
    out.flush()
    out.close()


  }
@experimental
object Main extends App with Servable:
  listen(8080)
