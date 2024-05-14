package test


import java.net.*
import java.io.*
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import concurrent.duration.DurationInt
object HttpRequest{
  def sendRequest(): Unit = {
    val url = new URL("http://localhost:8080/")
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.connect()

    val responseCode = connection.getResponseCode
    val responseMessage = connection.getResponseMessage

    //println(s"Response Code: $responseCode")
    //println(s"Response Message: $responseMessage")

    val inputStream = connection.getInputStream
    val reader = new BufferedReader(new InputStreamReader(inputStream))

    val response = new StringBuilder
    var line: String = null
    while ({ line = reader.readLine(); line != null }) {
      response.append(line)
    }

    println(s"Response Body: $response")

    reader.close()
  }

  def sendRequests(n: Int): Unit = {
    for (_ <- 1 to n) {
      Future(sendRequest()).onComplete{
        case Success(value) => println(value)
        case Failure(exception) => println(exception)
      }
    }
  }




}

object Test extends App:
  val futures = for (_ <- 1 to 50000) yield(Future(HttpRequest.sendRequest()))
  val result = Future.sequence(futures)
  Await.result(result, 30.seconds)
