package scalamacros.statements

import scalamacros.statements.DbType.DbString

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import scala.compiletime.error
import scala.quoted.*
import scala.compiletime.{error, summonInline}
import scala.deriving.Mirror
import scala.language.postfixOps

type RetrieveData[A] = [T] =>> java.sql.ResultSet => T

// Creating a class with Quotes argument allows to share quotes and reflect import across all the
// methods of the class and easily split the logic.
class StatementGenerator(using Quotes) {
  // In case of problems with finding the file, set it to absolute path.
  val parsed = SqlParser.parsed

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

  @experimental
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

    def retrieveToJsonMethodBody(columnInfos: Seq[ColumnInfo]): List[Expr[java.sql.ResultSet => String]] = columnInfos.toList.map(colInfo =>
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


    @experimental
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




    val combinedRsJsonGet: Expr[java.sql.ResultSet => String] = combineResultSetToJsonExprs(retrieveToJsonMethodBody(columnInfos)) //.asExprOf[java.sql.ResultSet => A]


    def foo =  {

      //      import quotes.reflect.*

      val clsName: Symbol = TypeRepr.of[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]].typeSymbol
      val parents = List(TypeTree.of[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]])
      val vals = List(("statement", TypeTree.of[UnsafeStatement]))

      def decls(cls: Symbol): List[Symbol] =
        vals.map(va => Symbol.newVal(cls, va._1, va._2.tpe, Flags.Mutable, Symbol.noSymbol)) ++
          List(Symbol.newMethod(cls, "retrieveJson", MethodType(List("rs"))(_ => List(TypeRepr.of[java.sql.ResultSet]), _ => TypeRepr.of[String]), Flags.Override, Symbol.noSymbol),
            Symbol.newMethod(cls, "retrieve", MethodType(List("rs"))(_ => List(TypeRepr.of[java.sql.ResultSet]), _ => TypeRepr.of[Tuple]), Flags.Override, Symbol.noSymbol),
            Symbol.newMethod(cls, "select", MethodType(List("rowItems"))(_ => List(TypeRepr.of[B]), _ => TypeRepr.of[String]), Flags.Override, Symbol.noSymbol),
            Symbol.newMethod(cls, "insert", MethodType(List("rowItems"))(_ => List(TypeRepr.of[A]), _ => TypeRepr.of[String]), Flags.Override, Symbol.noSymbol)


          )

      val retrieveSymbol = Symbol.newMethod(Symbol.spliceOwner, "retrieve", MethodType(List("rs"))(rs => List(TypeRepr.of[java.sql.ResultSet]), _ => TypeRepr.of[scala.Tuple]), Flags.Override, Symbol.noSymbol)

      val retrieveDef = DefDef(
        retrieveSymbol, {
          case List(List(paramTerm: Term)) => Some(Typed.unapply(Typed(Apply(Select.unique(combinedRsGet.asTerm, "apply"), List(paramTerm)), TypeTree.of[String]))._1.changeOwner(retrieveSymbol))
        }
      )

      val retrieveJsonSymbol = Symbol.newMethod(Symbol.spliceOwner, "retrieveJson", MethodType(List("rs"))(rs => List(TypeRepr.of[java.sql.ResultSet]), _ => TypeRepr.of[String]), Flags.Override, Symbol.noSymbol)


      val retrieveJsonDef = DefDef(
        retrieveJsonSymbol, {
          case List(List(paramTerm: Term)) => Some(Typed.unapply(Typed(Apply(Select.unique(combinedRsJsonGet.asTerm, "apply"), List(paramTerm)), TypeTree.of[String]))._1.changeOwner(retrieveJsonSymbol))
        }
      )
      //def retrieveJson(rs: java.sql.ResultSet): java.lang.String = ((rs: java.sql.ResultSet) => ((rs: java.sql.ResultSet) => "{".+(((rs: java.sql.ResultSet) => _root_.scala.StringContext.apply("\\\"", "\\\":", "").f[scala.Any]("id", rs.getInt("id"))).apply(rs)).+(",").+(((rs: java.sql.ResultSet) => _root_.scala.StringContext.apply("\\\"", "\\\":\\\"", "\\\"").f[scala.Any]("username", rs.getString("username"))).apply(rs)).+("}")).apply(rs))

      val selectSymbol = Symbol.newMethod(Symbol.spliceOwner, "select", MethodType(List("rowItems"))(rs => List(TypeRepr.of[A]), _ => TypeRepr.of[String]), Flags.Override, Symbol.noSymbol)
      val selectDef = DefDef(selectSymbol, argss => Some(sql.asTerm.changeOwner(retrieveSymbol)))

      val insertSymbol = Symbol.newMethod(Symbol.spliceOwner, "insert", MethodType(List("rowItems"))(rs => List(TypeRepr.of[B]), _ => TypeRepr.of[String]), Flags.Override, Symbol.noSymbol)
      val insertDef = DefDef(insertSymbol, args => Some(sql.asTerm.changeOwner(insertSymbol)))


      val initArgs = List(Apply(Select(New(TypeIdent(TypeRepr.of[UnsafeStatementFiltered].typeSymbol)), TypeRepr.of[UnsafeStatementFiltered].typeSymbol.primaryConstructor), List(Inlined(None, Nil, Literal(StringConstant("SELECT  id, username from user WHERE id=?"))))))
      val initArgs2 = Apply(
        Select(
          New(
            TypeIdent(
              TypeRepr.of[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]].typeSymbol
            )
          ),
          TypeRepr.of[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]].typeSymbol.primaryConstructor
        ),
        initArgs
      )


      val cls = Symbol.newClass(Symbol.spliceOwner, "$anon", parents.map(_.tpe), decls, selfType = None)


      val clsDef = ClassDef(cls, List(initArgs2), body = List(retrieveDef, retrieveJsonDef, selectDef, insertDef))

      println(clsDef.show)


      //Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(ClassDef("$anon", DefDef("<init>", List(TermParamClause(Nil)), Inferred(), None), List(Apply(TypeApply(Select(New(Applied(TypeIdent("PreparedStatementFiltered"), List(Applied(TypeIdent("CallArgs"), List(Inferred())), Applied(TypeIdent("CallArgs"), List(Inferred()))))), "<init>"), List(Inferred(), Inferred())), List(Apply(Select(New(TypeIdent("UnsafeStatementFiltered")), "<init>"), List(Inlined(None, Nil, Literal(StringConstant("SELECT  id, username from user WHERE id=?")))))))), None, List(DefDef("go", Nil, Inferred(), Some(Literal(StringConstant("hello")))), DefDef("retrieve", List(TermParamClause(List(ValDef("rs", TypeSelect(Select(Ident("java"), "sql"), "ResultSet"), None)))), TypeIdent("Tuple"), Some(Apply(Select(Inlined(None, Nil, Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(DefDef("$anonfun", List(TermParamClause(List(ValDef("rs", TypeSelect(Select(Ident("java"), "sql"), "ResultSet"), None)))), Inferred(), Some(Block(Nil, Apply(Select(Inlined(None, Nil, Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(DefDef("$anonfun", List(TermParamClause(List(ValDef("rs", Inferred(), None)))), Inferred(), Some(Block(Nil, Apply(Select(Select(Ident("scala"), "Tuple"), "fromProduct"), List(Apply(TypeApply(Select(Apply(Select(Inlined(None, Nil, Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(DefDef("$anonfun", List(TermParamClause(List(ValDef("rs", TypeSelect(Select(Ident("java"), "sql"), "ResultSet"), None)))), Inferred(), Some(Apply(TypeApply(Select(Ident("Tuple"), "apply"), List(Inferred())), List(Apply(Select(Ident("rs"), "getInt"), List(Inlined(None, Nil, Literal(StringConstant("id")))))))))), Closure(Ident("$anonfun"), None)))), "apply"), List(Ident("rs"))), "++"), List(Inferred())), List(Apply(Select(Inlined(None, Nil, Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(DefDef("$anonfun", List(TermParamClause(List(ValDef("rs", TypeSelect(Select(Ident("java"), "sql"), "ResultSet"), None)))), Inferred(), Some(Apply(TypeApply(Select(Ident("Tuple"), "apply"), List(Inferred())), List(Apply(Select(Ident("rs"), "getString"), List(Inlined(None, Nil, Literal(StringConstant("username")))))))))), Closure(Ident("$anonfun"), None)))), "apply"), List(Ident("rs"))))))))))), Closure(Ident("$anonfun"), None)))), "apply"), List(Ident("rs"))))))), Closure(Ident("$anonfun"), None)))), "apply"), List(Ident("rs"))))), DefDef("retrieveJson", List(TermParamClause(List(ValDef("rs", TypeSelect(Select(Ident("java"), "sql"), "ResultSet"), None)))), Inferred(), Some(Apply(Select(Inlined(None, Nil, Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(DefDef("$anonfun", List(TermParamClause(List(ValDef("rs", TypeSelect(Select(Ident("java"), "sql"), "ResultSet"), None)))), Inferred(), Some(Block(Nil, Apply(Select(Inlined(None, Nil, Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(DefDef("$anonfun", List(TermParamClause(List(ValDef("rs", Inferred(), None)))), Inferred(), Some(Block(Nil, Apply(Select(Apply(Select(Apply(Select(Apply(Select(Literal(StringConstant("{")), "+"), List(Apply(Select(Inlined(None, Nil, Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(DefDef("$anonfun", List(TermParamClause(List(ValDef("rs", TypeSelect(Select(Ident("java"), "sql"), "ResultSet"), None)))), Inferred(), Some(Apply(TypeApply(Select(Apply(Select(Select(Select(Ident("_root_"), "scala"), "StringContext"), "apply"), List(Typed(Repeated(List(Literal(StringConstant("\"")), Literal(StringConstant("\":")), Literal(StringConstant(""))), Inferred()), Inferred()))), "f"), List(Inferred())), List(Typed(Repeated(List(Inlined(None, Nil, Literal(StringConstant("id"))), Apply(Select(Ident("rs"), "getInt"), List(Inlined(None, Nil, Literal(StringConstant("id")))))), Inferred()), Inferred())))))), Closure(Ident("$anonfun"), None)))), "apply"), List(Ident("rs"))))), "+"), List(Literal(StringConstant(",")))), "+"), List(Apply(Select(Inlined(None, Nil, Inlined(Some(TypeIdent("StatementGenerator")), Nil, Block(List(DefDef("$anonfun", List(TermParamClause(List(ValDef("rs", TypeSelect(Select(Ident("java"), "sql"), "ResultSet"), None)))), Inferred(), Some(Apply(TypeApply(Select(Apply(Select(Select(Select(Ident("_root_"), "scala"), "StringContext"), "apply"), List(Typed(Repeated(List(Literal(StringConstant("\"")), Literal(StringConstant("\":\"")), Literal(StringConstant("\""))), Inferred()), Inferred()))), "f"), List(Inferred())), List(Typed(Repeated(List(Inlined(None, Nil, Literal(StringConstant("username"))), Apply(Select(Ident("rs"), "getString"), List(Inlined(None, Nil, Literal(StringConstant("username")))))), Inferred()), Inferred())))))), Closure(Ident("$anonfun"), None)))), "apply"), List(Ident("rs"))))), "+"), List(Literal(StringConstant("}")))))))), Closure(Ident("$anonfun"), None)))), "apply"), List(Ident("rs"))))))), Closure(Ident("$anonfun"), None)))), "apply"), List(Ident("rs")))))))), Typed(Apply(Select(New(TypeIdent("$anon")), "<init>"), Nil), Inferred())))


      val t2 = Typed(
        Apply(
          Select(
            New(TypeIdent(cls)), cls.primaryConstructor),
          List(Literal(StringConstant("SELECT  id, username from user WHERE id=?")))
        ), TypeTree.of[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]]
      )

      val t3 = Typed(
        Apply(
          Select(
            New(TypeIdent(cls)), cls.primaryConstructor),
          Nil
        ), TypeTree.of[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]])

      print(t3.show)
      /*
val t = Typed(Apply(
  TypeApply(
    Select(
      New(TypeIdent(cls)), cls.primaryConstructor),
    List(TypeTree.of[CallArgs[A]], TypeTree.of[CallArgs[B]])
  ),
  initArgs
), TypeTree.of[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]])
 */


      val newClass = Block(List(clsDef), t3).asExprOf[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]]

    }

    implicit def mapToExpr[K, V](implicit kType: scala.quoted.Type[K], vType: scala.quoted.Type[V], kToExpr: quoted.ToExpr[K], vToExpr: quoted.ToExpr[V]): quoted.ToExpr[Map[K, V]] = new quoted.ToExpr[Map[K, V]] {
      def apply(map: Map[K, V])(using Quotes): Expr[Map[K, V]] = {
        val elems = map.map { case (k, v) => val kExpr = kToExpr.apply(k)
          val vExpr = vToExpr.apply(v)
          '{ ($kExpr, $vExpr) }
        }
        val elemsExpr: Expr[List[(K, V)]] = map.map { case (k: K, v: V) =>
          val kExpr: Expr[K] = kToExpr.apply(k)
          val vExpr: Expr[V] = vToExpr.apply(v)
          '{ ($kExpr, $vExpr) }
        }.foldLeft(Expr[List[(K, V)]](Nil)) { (acc, elem) =>
          '{ $acc :+ $elem }
        }

        val mapExpr: Expr[Map[K, V]] = '{ Map.apply($elemsExpr: _*) }
        mapExpr
      }
    }



    implicit val DbTypeToExpr: quoted.ToExpr[DbType] = new quoted.ToExpr[DbType] {
      def apply(dbtype: DbType)(using Quotes): Expr[DbType] = {

       dbtype match {
         case  DbType.DbInt => '{DbType.DbInt}
         case DbType.DbString => '{DbType.DbString}
       }
        // implement the expression creation logic here
      }
    }

    implicit val columnInfoToExpr: quoted.ToExpr[ColumnInfo] = new quoted.ToExpr[ColumnInfo] {
      def apply(columnInfo: ColumnInfo)(using Quotes): Expr[ColumnInfo] = {
        // implement the expression creation logic here
        '{ ColumnInfo(${ Expr(columnInfo.dbType) }, ${ Expr(columnInfo.name) }) }
      }
    }

    implicit val seqColumnInfoToExpr: quoted.ToExpr[Seq[ColumnInfo]] = quoted.ToExpr.SeqToExpr[ColumnInfo](quoted.Type.of[ColumnInfo], columnInfoToExpr)

    val parsedExpr = '{${mapToExpr(implicitly[quoted.Type[String]], implicitly[quoted.Type[Seq[ColumnInfo]]], implicitly[quoted.ToExpr[String]], implicitly[quoted.ToExpr[Seq[ColumnInfo]]]).apply(parsed)} }


    def combineFullResultSetToJsonExprs(exprs: Expr[Map[String, Seq[ColumnInfo]]])(using Quotes) = '{
      $exprs.flatMap { expr =>
        val actionSeq = Seq("get", "create").map(y => y + expr._1)
        actionSeq.map { action =>
           {
            val exprs: Seq[java.sql.ResultSet => String] =
            expr._2.map {
              colInfo =>
                colInfo.dbType match {
                  case DbType.DbInt => {
                    ((rs: java.sql.ResultSet) => f"\"${colInfo.name}\":${rs.getInt(colInfo.name)}")
                  }
                  case DbType.DbString => {
                    ((rs: java.sql.ResultSet) => f"\"${colInfo.name}\":${rs.getString(colInfo.name)}")
                  }

                }
            }
            val func: java.sql.ResultSet => String = (rs: java.sql.ResultSet) => exprs match {
              case x: List[java.sql.ResultSet => String] =>
                x match
                  case head :: tail => tail.foldLeft(head) { (acc, next) =>
                    rs => "{" + acc.apply(rs) + "," + next.apply(rs) + "}"
                  }.apply(rs)
                  case Nil => ???

            }

            (expr._1, func)
          }
        }
      }
    }

    def filterAndSort(
                       tuples: Seq[(String, Any)],
                       strings: Seq[String]
                     ): Seq[(String, Any)] = {
      val filteredTuples = tuples.filter { case (str, _) =>
        strings.contains(str)
      }
      val sortedTuples = filteredTuples.sortBy { case (str, _) =>
        strings.indexOf(str)
      }
      sortedTuples
    }

    def FullResultSetToJsonExprs (exprs: Expr[Map[String, Seq[ColumnInfo]]])(using Quotes) = '{
      $exprs.flatMap { expr =>
        val actionSeq = Seq("get", "create").map(y => y + expr._1)
        actionSeq.map { action =>
          val func = {
            {
              val exprs: Seq[(ColumnInfo, java.sql.ResultSet => String)] = {
                expr._2.map { colInfo =>
                  colInfo.dbType match {
                    case DbType.DbInt =>
                      (colInfo, (rs: java.sql.ResultSet) => f"${colInfo.name}:${rs.getInt(colInfo.name)}")
                    case DbType.DbString =>
                      (colInfo, (rs: java.sql.ResultSet) => f"${colInfo.name}:${rs.getString(colInfo.name)}")
                  }
                }
              }

              val funcExprs: Seq[java.sql.ResultSet => String] = exprs.map(x => x._2)


              def functions: java.sql.ResultSet => String = {
                println("I am invoking funcions")
                val res = (rs: java.sql.ResultSet) => funcExprs match
                  case head :: tail => tail.foldLeft(head) { (acc, next) =>
                    rs => "{" + acc.apply(rs) + "," + next.apply(rs) + "}"
                  }.apply(rs)
                  case _ => "error"

                res
              }




            }
          }

            val getResultSet: (Seq[(String, Any)], Connection) => Iterator[ResultSet] = (inputParameters: Seq[(String, Any)], c: java.sql.Connection) => {
              val columnNames = expr._2.map(_.name)
              val pstmt: java.sql.PreparedStatement = {
                action match {
                  case x: String if (x.startsWith("get")) => {
                    val query = {
                      val tableName = expr._1
                      val filterNames = inputParameters.map(j => j._1 + "=?").mkString(" AND ")
                      f"SELECT ${columnNames.mkString(",")} from $tableName WHERE $filterNames"
                    }

                    c.prepareStatement(query,ResultSet.TYPE_SCROLL_INSENSITIVE,  ResultSet.CONCUR_READ_ONLY)
                  }
                  case x: String if (x.startsWith("create")) => {
                    val query = {
                      val tableName = expr._1
                      val placeholders = inputParameters.map(j => "?").mkString(" , ")
                      val query = s"INSERT INTO $tableName (${columnNames.mkString(",")} ) VALUES ($placeholders)"
                      query
                    }

                    c.prepareStatement(query,ResultSet.TYPE_SCROLL_INSENSITIVE,  ResultSet.CONCUR_READ_ONLY)
                  }
                }
              }

                val filteredTuples = inputParameters.filter { case (str, _) =>
                  columnNames.contains(str)
                }
                val sortedTuples = filteredTuples.sortBy { case (str, _) =>
                  columnNames.indexOf(str)
                }

              sortedTuples.map(z => z._2).zipWithIndex.foreach { case (param, idx) =>
                pstmt.setObject(idx + 1, param)
              }

              pstmt.execute()



              new Iterator[ResultSet] {

                val resultSet = pstmt.getResultSet


                def hasNext: Boolean = resultSet.next()

                def next(): ResultSet = {
                  resultSet
                }


              }




            }


          (action, getResultSet)
        }



        }
        }









    val cl = '{
      new PreparedStatementFiltered[CallArgs[A], CallArgs[B]](UnsafeStatementFiltered($sql)) {
        def go: String = "hello"

         override val database: Map[String, (Seq[(String, Any)], java.sql.Connection) => Iterator[ResultSet]]   = ${FullResultSetToJsonExprs(parsedExpr)}



        override def retrieve(rs: java.sql.ResultSet): Tuple = ${ combinedRsGet }.apply(rs)

        override def retrieveJson(rs: java.sql.ResultSet): String = ${ combinedRsJsonGet }.apply(rs)


      }
    }.asExprOf[PreparedStatementFiltered[CallArgs[A], CallArgs[B]]]



   cl
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
  @experimental
  private def selectCallImplementation[A <: Tuple : Type, B<: Tuple: Type](
                                                           tableName: Expr[String],
                                                           columnMapping: Expr[A],
                                                           filterMapping: Expr[B]

                                                         )(using
                                                           Quotes
                                                         ): Expr[PreparedStatementFiltered[CallArgs[A],CallArgs[B]]] =
    new StatementGenerator()
      .selectPreparedStatementImpl[A,B](tableName, columnMapping, filterMapping)
  @experimental
  inline def selectPreparedStatement[A <: Tuple, B<:Tuple](inline tableName: String)(
    inline columnMapping: A)(inline filterMapping: B
  ): PreparedStatementFiltered[CallArgs[A], CallArgs[B]] = ${
    selectCallImplementation[A, B]('tableName, 'columnMapping, 'filterMapping)
  }
}

