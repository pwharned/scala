package cicero.http


import java.io.InputStream
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.combinator.*

object HttpRequestParser {
  private val parser = new HttpRequestParserImpl

  def parse(inputStream: InputStream): HttpRequest = {
    println("hello from parsing")
    val buffer = new Array[Byte](1024)
    var bytesRead = inputStream.read(buffer)
    var requestBytes = new ArrayBuffer[Byte]

    requestBytes ++= buffer.take(bytesRead)


    parser.parse(requestBytes.toArray)
  }
}

class HttpRequestParserImpl extends RegexParsers {
  override def skipWhitespace = true

  def request: Parser[HttpRequest] = {
    method ~ url ~ protocol ~ headers ~ body ^^ {
      case m ~ u ~ p ~ h ~ b => HttpRequest(m, u, p, h, b)
    }
  }

  def method: Parser[ByteBuffer] = """[A-Z]+""".r ^^ (s => ByteBuffer.wrap(s.getBytes))


  def url: Parser[ByteBuffer] = """[^ ]+""".r ^^ ( s=> ByteBuffer.wrap(s.getBytes))

  def protocol: Parser[ByteBuffer] = """[A-Z]+/[0-9.]+""".r ^^ (s => ByteBuffer.wrap(s.getBytes))

  def headers: Parser[Map[ByteBuffer, ByteBuffer]] = repsep(header, """\n""".r) ^^ (_.toMap)

  def header: Parser[(ByteBuffer, ByteBuffer)] = key ~ value ^^ { case k ~ v => (k, v) }

  def key: Parser[ByteBuffer] = """[^:]+""".r ^^ (s => ByteBuffer.wrap(s.getBytes))

  def value: Parser[ByteBuffer] = """[^\\n]+""".r ^^ (s => ByteBuffer.wrap( s.getBytes))

  def body: Parser[Option[ByteBuffer]] = opt(rep(ch) ^^ (s => ByteBuffer.wrap(s.toArray))) ^^ (_.map(_.compact()))

  def ch: Parser[Byte] = """.""".r ^^ (_.charAt(0).toByte)

  def parse(requestBytes: Array[Byte]): HttpRequest = {
    parseAll(request, ByteBuffer.wrap(requestBytes).asCharBuffer()) match {
      case Success(result, _) => result
      case Failure(msg, _) => throw new ParseException(msg)
      case Error(msg, _) => throw new ParseException(msg)
    }
  }
}

case class HttpRequest(method: ByteBuffer, url: ByteBuffer, protocol: ByteBuffer, headers: Map[ByteBuffer, ByteBuffer], body: Option[ByteBuffer])

class ParseException(message: String) extends RuntimeException(message)
