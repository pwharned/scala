package scalamacros.statements

import scala.annotation.experimental
import scala.compiletime.error
import scala.quoted.*
import scala.compiletime.{error, summonInline}
import scala.deriving.Mirror

type RetrieveData[A] = [T] =>> java.sql.ResultSet => T

// Creating a class with Quotes argument allows to share quotes and reflect import across all the
// methods of the class and easily split the logic.
class StatementGenerator(using Quotes) {
  // In case of problems with finding the file, set it to absolute path.
  val SchemaPath = "sql/schema.sql"

  // This import is dependent on Quotes that are visible in the scope.
  import quotes.reflect.*

  // This class will create DbType enum based on TypeRepr of the type.
  // Note that the type is matched, unknown is captured (any lowercase name) and used to print its name.
  private def getDbType(typeRepr: TypeRepr): DbType = typeRepr.asType match {
    case '[Int]    => DbType.DbInt
    case '[String] => DbType.DbString
    case '[unknown] =>
      report.throwError("Unsupported type as DB column " + Type.show[unknown])
  }


  // Validation with the schema that is present in sql/schema.sql. In case of mismatch
  // a compilation error is returned.
  private def validateWithSchema(
      tableName: String,
      columnInfo: Seq[ColumnInfo]
  ): Unit = {
    val parsed = SqlParser.parse(SchemaPath)
    
    val schemaColumns = parsed.getOrElse(
      tableName,
      report.throwError(
        "Schema doesn't have table definded with name: " + tableName
      )
    )
    
    columnInfo.foreach(ci => {
      val schemaColumn = schemaColumns
        .find(_.name == ci.name)
        .getOrElse(
          report.throwError("Table doesn't have column with name: " + ci.name)
        )
      if (schemaColumn.dbType != ci.dbType) {
        report.throwError(
          s"Invalid type for $tableName.${ci.name}: ${ci.dbType} != ${schemaColumn.dbType}"
        )
      }
    })
  }

  private def parseColumDef(columnDefTerm: Term) : ColumnInfo = {

    // The second argument of Apply is a list of terms representing a single call argument.
    // This time quoted expression is matched as it is simpler than full AST.
    // Parameter 'a' captures the type and $name expression repesenting the name argument of the contructor.
    columnDefTerm.asExprOf[ColDef[_]] match {

      case '{ ColDef[a]($name) } =>
        val paramType = TypeRepr.of[a]

        // It is possible (and for this case required) to obtain compile time values. It
        // will fail in case of the name is a variable.
        val nameValue = name.valueOrError
        println(
          s"Type: ${Type.show[a]} , value: $nameValue, name AST: ${name.asTerm
            .show(using Printer.TreeStructure)}"
        )

        ColumnInfo(getDbType(paramType), name.valueOrError)

      case _ =>
        report.throwError(
          "Statically parsed ColDef items needed e.g. ColDef[Int](\"a\")"
        )
    }
  }

  private def parseColumnInfo(paramMapping: Term): Seq[ColumnInfo] = {

    // Low level AST is matched in the case below, the structure can be seen with show(using Printer.TreeStructure).
    paramMapping match {

      case Inlined(None, Nil, Apply(TypeApply(_, _), columnDefs)) =>
        columnDefs.map(columnDefsTerm =>
          println(
            "Parameters term: " + columnDefsTerm.show(using Printer.TreeStructure)
          )

          parseColumDef(columnDefsTerm)
        )

      case _ =>
        report.throwError(
          "Provide ColDef list e.g.: StatementGenerator.createPreparedStatement(ColDef[Int](\"c1\"), ColDefl[String](\"c2\"))"
        )
    }
  }

  private def buildInsertSql(
      tableName: String,
      columns: Seq[ColumnInfo]
  ): String = {
    val columnNames = columns.map(_.name).mkString(", ")
    val placeholders = "?".repeat(columns.length).mkString(", ")
    s"INSERT INTO $tableName ($columnNames) VALUES ($placeholders)"
  }

  private def buildSelectSql(
                              tableName: String,
                              columns: Seq[ColumnInfo],
                              filters: Seq[ColumnInfo]
                            ): String = {
    val columnNames = columns.map(_.name).mkString(", ")
    val filterNames = filters.map(_.name + "=?" ).mkString(" AND ")

    f"SELECT  $columnNames from $tableName WHERE $filterNames"
  }


  def insertPreparedStatementImpl[A <: Tuple: Type](
      table: Expr[String],
      columnMapping: Expr[A]
  ): Expr[PreparedStatement[CallArgs[A]]] = {

    // There are utilies for output, but println and throwing custom errors also work.
    println(
      "Generating prepared statement for arguments: " + columnMapping.asTerm
        .show(using Printer.TreeStructure)
    )

    val tableName = table.valueOrError

    val columns = parseColumnInfo(columnMapping.asTerm)

    println("Columns: " + columns)

    validateWithSchema(tableName, columns)

    val sql = Expr(buildInsertSql(tableName, columns))

    // As the types was verified, unsafe statement can be safefy created and passed to the
    // Prepared statemnt cont
    '{
      new PreparedStatement[CallArgs[A]](UnsafeStatement($sql))
    }.asExprOf[PreparedStatement[CallArgs[A]]]
  }

  def selectPreparedStatementImpl[A <: Tuple : Type, B<:Tuple:Type](
                                                      table: Expr[String],
                                                      columnMapping: Expr[A],
                                                      filterMapping: Expr[B]
                                                    )(using Quotes): Expr[PreparedStatementFiltered[CallArgs[A],CallArgs[B] ]] = {

    // There are utilies for output, but println and throwing custom errors also work.
    println(
      "Generating select prepared statement for arguments: " + columnMapping.asTerm
        .show(using Printer.TreeStructure)
    )
    //import quotes.reflect.*

    val tableName = table.valueOrError

    val columns = parseColumnInfo(columnMapping.asTerm)

    println("Columns: " + columns)

    val filters = parseColumnInfo(filterMapping.asTerm)
    println("Filters: " + filters)

    validateWithSchema(tableName, columns)
    validateWithSchema(tableName, filters)

    val sql = Expr(buildSelectSql(tableName, columns, filters))

    val name: String = TypeRepr.of[PreparedStatementFiltered[CallArgs[A],CallArgs[B]]].typeSymbol.name
    val columnInfos: Seq[ColumnInfo] = parseColumnInfo(columnMapping.asTerm)


    val retrieveMethodBody: List[Expr[java.sql.ResultSet => Tuple]] = columnInfos.toList.map(colInfo =>
      colInfo.dbType match {
        case DbType.DbInt =>
          val columnName: Expr[String] = Expr(colInfo.name) // todo: get column name from ColumnInfo
          val r: Expr[java.sql.ResultSet => Tuple1[Int] ] = '{ ((rs: java.sql.ResultSet) => Tuple(rs.getInt(${ columnName }) ) ) }
          r
        case DbType.DbString =>
          val columnName: Expr[String] = Expr(colInfo.name) // todo: get column name from ColumnInfo
          val r: Expr[java.sql.ResultSet => Tuple1[String]] = '{ ((rs: java.sql.ResultSet) => Tuple(rs.getString(${ columnName }))) }
          r
      })


    def combineResultSetExprs(exprs: List[Expr[java.sql.ResultSet => Tuple]])(using Quotes): scala.quoted.Expr[java.sql.ResultSet => Tuple] =
      '{ (rs: java.sql.ResultSet) =>
        ${ exprs match {
          case Nil => '{ ??? } // Handle the case when the list is empty
          case head :: tail =>
            tail.foldLeft(head) { (acc, next) =>
              '{ rs => scala.Tuple.fromProduct(${ acc }.apply(rs) ++ ${ next }.apply(rs)) }
            }
        }
        }.apply(rs)
      }


    val combinedRsGet: Expr[java.sql.ResultSet => Tuple] = '{ ${ combineResultSetExprs(retrieveMethodBody) }}//.asExprOf[java.sql.ResultSet => A]

    val retrieveToJsonMethodBody: List[Expr[java.sql.ResultSet => String]] = columnInfos.toList.map(colInfo =>
      colInfo.dbType match {
        case DbType.DbInt =>
          val columnName: Expr[String] = Expr(colInfo.name)
          val r: Expr[java.sql.ResultSet => String] = '{ ((rs: java.sql.ResultSet) => f"\"${${columnName}}\":${rs.getInt(${ columnName })}"  ) }
          r
        case DbType.DbString =>
          val columnName: Expr[String] = Expr(colInfo.name)
          val r: Expr[java.sql.ResultSet => String] = '{ ((rs: java.sql.ResultSet) => f"\"${${columnName}}\":\"${rs.getString(${ columnName })}\"") }
          r
      })


    def combineResultSetToJsonExprs(exprs: List[Expr[java.sql.ResultSet => String]])(using Quotes): scala.quoted.Expr[java.sql.ResultSet => String] =
      '{ (rs: java.sql.ResultSet) =>
        ${ exprs match {
          case Nil => '{ ??? } // Handle the case when the list is empty
          case head :: tail =>
            tail.foldLeft(head) { (acc, next) =>
              '{ rs => "{"+ ${ acc }.apply(rs) +","+ ${ next }.apply(rs) + "}" }
            }
        }
        }.apply(rs)
      }


    val combinedRsJsonGet: Expr[java.sql.ResultSet => String] = '{ ${ combineResultSetToJsonExprs(retrieveToJsonMethodBody) } } //.asExprOf[java.sql.ResultSet => A]


    '{
      new PreparedStatementFiltered[CallArgs[A], CallArgs[B]](UnsafeStatementFiltered($sql)){
        def go: String = "hello"
        override def retrieve(rs: java.sql.ResultSet): Tuple = ${ combinedRsGet }.apply(rs)
        override def retrieveJson(rs: java.sql.ResultSet): String = ${ combinedRsJsonGet }.apply(rs)

      }
    }.asExprOf[PreparedStatementFiltered[CallArgs[A],CallArgs[B]]]
  }

}

object StatementGenerator {

  // Auxilirary function to create the class as the inlined main maro function needs a single expression.
  private def insertCallImplementation[A <: Tuple: Type](
      tableName: Expr[String],
      columnMapping: Expr[A]
  )(using
      Quotes
  ): Expr[PreparedStatement[CallArgs[A]]] =
    new StatementGenerator()
      .insertPreparedStatementImpl[A](tableName, columnMapping)

  inline def insertPreparedStatement[A <: Tuple](inline tableName: String)(
      inline columnMapping: A
  ): PreparedStatement[CallArgs[A]] = ${
    insertCallImplementation[A]('tableName, 'columnMapping)
  }

  private def selectCallImplementation[A <: Tuple : Type, B<: Tuple: Type](
                                                           tableName: Expr[String],
                                                           columnMapping: Expr[A],
                                                           filterMapping: Expr[B]

                                                         )(using
                                                           Quotes
                                                         ): Expr[PreparedStatementFiltered[CallArgs[A],CallArgs[B]]] =
    new StatementGenerator()
      .selectPreparedStatementImpl[A,B](tableName, columnMapping, filterMapping)

  inline def selectPreparedStatement[A <: Tuple, B<:Tuple](inline tableName: String)(
    inline columnMapping: A)(inline filterMapping: B
  ): PreparedStatementFiltered[CallArgs[A], CallArgs[B]] = ${
    selectCallImplementation[A, B]('tableName, 'columnMapping, 'filterMapping)
  }
}

