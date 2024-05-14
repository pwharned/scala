package scalamacros.test

import scala.annotation.experimental
import scala.quoted.*
import scala.compiletime.*
class Base:
  def func(s: String): String = ""


object NewClass2 {

  @experimental
  transparent inline def classNamed2(inline name: String) = ${ newClassImpl('name) }

  @experimental
  def newClassImpl(nameExpr: Expr[String])(using Quotes): Expr[Base] = {
    import quotes.reflect.*
    //val name: String = TypeRepr.of[A].typeSymbol.name + "Impl"
    val name = nameExpr.valueOrAbort

    var parents = List(TypeTree.of[Base])

    def decls(cls: Symbol): List[Symbol] =
      List(Symbol.newMethod(cls, "func", MethodType(List("s"))(_ => List(TypeRepr.of[String]), _ => TypeRepr.of[String]), Flags.Override, Symbol.noSymbol),
      Symbol.newMethod(cls, "func2", MethodType(List("s"))(_ => List(TypeRepr.of[String]), _ => TypeRepr.of[String])))

    val cls = Symbol.newClass(Symbol.spliceOwner, name, parents = parents.map(_.tpe), decls, selfType = None)
    val funcSyms = cls.declaredMethods
    val funcDefs = funcSyms.map { sym =>
      DefDef(sym, argss => Some('{ "go" }.asTerm))
    }
    //val funcDef = DefDef(funcSym, argss => Some('{"go"}.asTerm))
    //val funcDef2 = DefDef(funcSym2, argss => Some('{"go2"}.asTerm))

    val funcTypes = funcSyms.map(_.typeRef)


    val intersectionType = funcTypes.foldLeft(TypeRepr.of[Any]) { (acc, tpe) =>
      AndType(acc, tpe)
    }

    val clsDef = ClassDef(cls, parents, body = funcDefs)
    val newCls = Typed(Apply(Select(New(TypeIdent(cls)), cls.primaryConstructor), Nil), TypeTree.of[Base])
    //    val newCls = Typed(Apply(Select(New(TypeIdent(cls)), cls.primaryConstructor), Nil), AppliedType(TypeRef[TypeRepr.of[cls.type]], List(intersectionType)))

    Block(List(clsDef), newCls).asExprOf[Base]
  }
}

