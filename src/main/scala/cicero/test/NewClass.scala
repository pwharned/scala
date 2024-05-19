package cicero.test

import scala.annotation.experimental
import scala.quoted.*

object NewClass {
  @experimental
  transparent inline def classNamed(inline name: String) = ${ classNamedExpr('name) }
  @experimental
  def classNamedExpr(nameExpr: Expr[String])(using Quotes): Expr[Any] = {
    import quotes.reflect.*

    val name = nameExpr.valueOrAbort
    val parents = List(TypeTree.of[Base])

    def decls(cls: Symbol): List[Symbol] = List(Symbol.newMethod(cls, "func", MethodType(List("s"))(_ => List(TypeRepr.of[String]), _ => TypeRepr.of[String])))


    // put something interesting here

    val symbol = Symbol.newClass(Symbol.spliceOwner, name, parents.map(_.tpe), decls, selfType = None)
    val funcSym = symbol.declaredMethod("func").head

    val funcDef = DefDef(funcSym, argss => Some('{ "" }.asTerm))
    val body = List(funcDef)                //.and here

    val classDef = ClassDef(symbol, parents, body)
    val ctor = Select(New(TypeIdent(symbol)), symbol.primaryConstructor)
    val newClass = Typed(Apply(ctor, Nil), TypeTree.of[Base])

    Block(List(classDef), newClass).asExprOf[Base]
  }
}