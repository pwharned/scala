package scalamacros.statements

// All supported database column types, in this example limited to String and Int.
enum DbType {
  case DbInt
  case DbString
}



// Auxiliary class to keep parsed column information.
case class ColumnInfo(val dbType: DbType, name: String)
// Very simple parser just to work with the example. Programmer perfection assumed - no errors in sql script.
object SqlParser {
  val parsed: Map[String, Seq[ColumnInfo]] = {
    //val content = scala.io.Source.fromURL(getClass.getResource("schema.sql")).getLines().mkString
    val content = scala.io.Source.fromFile( "/home/pat/Projects/personalProjects/workspace/scala/src/main/resources/schema.sql").getLines().mkString

   // val content = scala.io.Source.fromFile(path).mkString
    val statements = content.split(";")
    statements.map(parseStatement).toMap

    //((x:Int) => x + 2).apply(10)


  }

  private def parseStatement(statement: String) ={
    val splatStatement = statement.split("[\\s,:]").filterNot(_.isBlank).map(_.toLowerCase)
    println(splatStatement.mkString(","))
    val tableName = splatStatement(2)
    val columns = splatStatement.drop(4)//.dropRight(1)
    val colInfo = columns.toSeq
      .sliding(2, 2)
      .map(col => ColumnInfo(typeByName(col(1)), col.head))
      .toSeq

    tableName -> colInfo
  }



  private def typeByName(name: String) = {
    if(name == "int") {
      DbType.DbInt
    } else if(name.startsWith("varchar")) {
      DbType.DbString
    } else {
      throw RuntimeException("Sql parsing error, wrong type: " + name)
    }
  }
}
