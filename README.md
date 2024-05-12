## Scala 3 macros

This repository is based on the https://github.com/plewand/scala3-macros.git

I use examples originally developed by plewand to expand to more complicated use cases, particularly around developing typesafe backend applications for interacting with SQL databases.

Like Plewand, The program generates type safe prepared statements on compilation time aligned with externally loaded SQL script.
Additionally I add type safe methods for retrieving data from a database by inlining higher-order functions (at least I think thats what I'm doing)

```
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
```

In the above example we pattern match on the database column type which is checked at compile time against the schema. We then inline a "retrieve" method with the appropriate calls to the `java.sql.ResultSet` based on the types. We can thereby retrieve the data from the database in a type safe manner without having to do pattern matching at runtime. 

I added a `retrieveJson` method as well which returns each row as a json formatted string. This theroretically should save significant overhead that comes from json serialization/deserialization as the transformation for each row is done inline.


Also, it contains some other macro examples explaining macros features. 

Please visit Plewand's  blog [here](https://pawel7.medium.com/scala-3-macros-without-pain-ce54d116880a) for more information - some of the code is shamelessly copied from his repository.


* [Prepared statement - the main project](src/main/scala/scalamacros/statements)

### Usage

This is a normal sbt project. 

* Compile the code with 
```
sbt compile
``` 
During compilation many messages are printed related to the macros.

* Run
```
sbt run
```
The list of the entry points of the examples with appear, choose one.

### Tips
* Try to use Visual Studio code with Metals if IntelliJ fails to work (still pending work on the time of writing).
* In case of the SQL schema file cannot be found, use its absolute path.

More information on Scala 3 metaprogramming can be found 
[here](https://docs.scala-lang.org/scala3/reference/metaprogramming.html).
