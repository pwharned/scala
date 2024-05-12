package scalamacros.statements
import java.sql.ResultSet
// Type matching - a new Scala 3 feature - is used to construct the type that will be used
// to pass runtime arguments to the generated prepared statement.
type CallArgs[Xs <: Tuple] <: Tuple = Xs match
  case EmptyTuple   => Xs
  case ColDef[b] *: xs => b *: CallArgs[xs]

// A class to simulate an unsafe statement.
class UnsafeStatement(sql: String) {
  def insert(rowItems: Any*) = {
    println(s"Executing SQL: '${sql}' for args: [${rowItems.mkString(", ")}]")
  }

}

// Type safe prepared statemnet.
class PreparedStatement[A <: Tuple](statement: UnsafeStatement) {
  def insert(rowItems: A) : Unit ={
    statement.insert(rowItems.toList:_*)
  }

}


class UnsafeSelectStatement(sql: String) {


  def select(rowItems: Any*) = {
   // println(s"Executing SQL: '${sql}' for args: [${rowItems.mkString(", ")}]")
   sql
  }
}

// Type safe prepared statemnet.
class PreparedSelectStatement[A <: Tuple](statement: UnsafeSelectStatement) {


  def select(rowItems: A): String = {
    statement.select(rowItems.toList: _*)
  }
}

class UnsafeStatementFiltered(sql: String) {
  def insert(rowItems: Any*) = {
    println(s"Executing SQL: '${sql}' for args: [${rowItems.mkString(", ")}]")
  }

  def select(rowItems: Any*): String = {
    println(s"Executing SQL: '${sql}' for args: [${rowItems.mkString(", ")}]")
    sql
  }
}

// Type safe prepared statemnet.
class PreparedStatementFiltered[A <: Tuple, B<: Tuple](statement: UnsafeStatementFiltered) {
  def insert(rowItems: A) : Unit ={
    statement.insert(rowItems.toList:_*)
  }

  def select(rowItems: B): String = {
    statement.select(rowItems.toList: _*)
  }
  
  def retrieveJson(rs: java.sql.ResultSet): String = ???


}

// Definintion of the column passed to the macro - a name with the bound type.
case class ColDef[A](name: String)

