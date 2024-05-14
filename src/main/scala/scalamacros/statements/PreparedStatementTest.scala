package scalamacros.statements
import reflect.Selectable.reflectiveSelectable
import scalamacros.logast.logAST

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.annotation.experimental
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

@experimental
object DB2Connector {
  val url = "jdbc:db2://localhost:50000/bludb"
  val username = "db2inst1"
  val password = "password"
  //connectAndRunDDL(url, username, password)


  def connectAndRunDDL(url: String, username: String, password: String): Unit = {
    val conn: Connection = DriverManager.getConnection(url, username, password)
    try {
      val ddlStatements: List[String] = loadDDLStatements()
      ddlStatements.foreach { ddlStatement =>
        val stmt: Statement = conn.createStatement()
        scala.util.Try {
          stmt.execute(ddlStatement)
        } match
          case Failure(exception) => {
            println(exception)
            stmt.close()
          }
          case Success(value) => {
            println(value)
            stmt.close()
          }

      }
    } finally {
      conn.close()
    }
  }

  private def loadDDLStatements(): List[String] = {
    val resourceStream = getClass.getResourceAsStream("/schema.sql")
    val reader = new BufferedReader(new InputStreamReader(resourceStream))
    val ddlStatements = reader.lines().toArray.mkString(" ").split(";")
      .map(_.trim).filter(_.nonEmpty)
    reader.close()
    ddlStatements.toList
  }


  def connectAndRunPreparedStatement(query: String, params: Seq[Any]): ResultSet = {


    val conn: Connection = DriverManager.getConnection(url, username, password)
    val pstmt: java.sql.PreparedStatement = conn.prepareStatement(query)
    params.zipWithIndex.foreach { case (param, idx) =>
      pstmt.setObject(idx + 1, param)
    }
    pstmt.execute()
    val resultSet: ResultSet = pstmt.getResultSet
    resultSet
  }

//@main def preparedStatemntTest(): Unit = {

  // Type given explicitly - better error messages than if the type is inferred.
  // Replace the name of the table or the argument with a variable - an error reported by the compiler
  // due to the value isn't know on compile time.
  // Change the type or the name of the column to be different that in sql/schema.sql - a compilation error.
 // val statement: PreparedStatement[(Int, String)] = StatementGenerator.insertPreparedStatement("user")(ColDef[Int]("id"), ColDef[String]("username"))

  type B = Tuple1[Int]
  //statement.insert(1, "John")
  def iterateResultSet(rs: ResultSet, f:ResultSet => String, out: PrintStream): String = {
    var string = ""
      if (rs.next()) {
        // process the current row

        val jsonBytes = f(rs)
        val chunkSize = jsonBytes.length.toHexString

        // Write the chunk size and newline character
        //out.write(s"$chunkSize\r\n".getBytes("UTF-8"))
        // Write the JSON object itself
        //out.write(jsonBytes.getBytes("utf-8"))

        // Write another newline character to separate the chunk
        //out.write("\r\n".getBytes("UTF-8"))
        string = string + s"$chunkSize\r\n" + jsonBytes + "\r\n"

      }

    string


  }

  val selectStatement2 = StatementGenerator.selectPreparedStatement("user")(ColDef[Int]("id"), ColDef[String]("username")) ( Tuple(ColDef[Int]("id") ))

  //  print(selectStatement2.asInstanceOf[{def func(s: String): String}].func(s = "Test"))
  val queryJson: String = selectStatement2.select(Tuple(1))

  val rsJson: ResultSet = DB2Connector.connectAndRunPreparedStatement(queryJson, Seq(1))

  def getUsers(out: PrintStream): String = {

    val rsJson: ResultSet = DB2Connector.connectAndRunPreparedStatement(queryJson, Seq(1))
    iterateResultSet(rsJson, selectStatement2.retrieveJson,out)
  }
  // Logged AST that was used to find out the structure of the code that was matched in the macro.
  logAST {
    (ColDef[Int]("id"), ColDef[String]("lastName"))
  }  
}
