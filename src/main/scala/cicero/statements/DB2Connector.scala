package cicero.statements
import reflect.Selectable.reflectiveSelectable
import cicero.logast.logAST

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

    val pstmt: java.sql.PreparedStatement = conn.prepareStatement(query,ResultSet.TYPE_SCROLL_INSENSITIVE,  ResultSet.CONCUR_READ_ONLY)
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

  def iterateResultSet(rs: ResultSet, f:ResultSet => String, out: PrintStream) = {

    val chunkSize = (1).toHexString
    val jsonBytes  = "["

    var js = s"$chunkSize\r\n" + jsonBytes + "\r\n"

    out.write(js.getBytes("utf-8"))
      while (rs.next()) {

        // process the current row

        var jsonBytes = f(rs)

          if(!rs.isLast){
           jsonBytes = jsonBytes + ","
          }


        val chunkSize = (jsonBytes.length).toHexString
        val string = s"$chunkSize\r\n" + jsonBytes + "\r\n"
        out.write(string.getBytes("utf-8"))

      }
    js = s"$chunkSize\r\n" + "]" + "\r\n"

    out.write(js.getBytes("utf-8"))
  }

  /*
  A

  Server will accept
  (string which will be matched to a function call)
  (input parameters: rowItems[CallArgs])

   */

  val selectStatement2 = StatementGenerator.selectPreparedStatement("user")(ColDef[Int]("id"), ColDef[String]("username")) ( Tuple(ColDef[Int]("id") ))

  val conn: Connection = DriverManager.getConnection(url, username, password)
  val test = selectStatement2.database.get("getuser").get.apply(Seq(("id", 1)),conn)
  val test2  = selectStatement2.database.get("createuser").get.apply(Seq(("username", "danny"),("id", 1) ),conn)

      test.foreach(x => println(selectStatement2.retrieveJson(x)))

      //  print(selectStatement2.asInstanceOf[{def func(s: String): String}].func(s = "Test"))
      val queryJson: String = selectStatement2.select(Tuple(1))
      def rsJson: ResultSet = DB2Connector.connectAndRunPreparedStatement(queryJson, Seq(1))


  def getUsers(out: PrintStream): Unit = {

    val rsJson: ResultSet = DB2Connector.connectAndRunPreparedStatement(queryJson, Seq(1))
    iterateResultSet(rsJson, selectStatement2.retrieveJson, out)
  }
      // Logged AST that was used to find out the structure of the code that was matched in the macro.
      logAST {
        (ColDef[Int]("id"), ColDef[String]("lastName"))
      }
}
