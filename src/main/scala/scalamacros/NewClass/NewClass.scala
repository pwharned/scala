package scalamacros.NewClass

import scala.annotation.experimental
import scala.quoted.*

object NewClass {
  @experimental
  inline def newClass[A]: A = ${newClassImpl[A]}

  @experimental
  def newClassImpl[A: Type](using Quotes): Expr[A] = {
    import quotes.reflect.*

    val name: String = TypeRepr.of[A].typeSymbol.name + "Impl"
    val parents = List(TypeTree.of[A])

    def decls(cls: Symbol): List[Symbol] =
      List(Symbol.newMethod(cls, "func", MethodType(List("s"))(_ => List(TypeRepr.of[String]), _ => TypeRepr.of[String]), Flags.Override, Symbol.noSymbol))

    val cls = Symbol.newClass(Symbol.spliceOwner, name, parents.map(_.tpe), decls, selfType = None)
    val funcSym = cls.declaredMethod("func").head

    val funcDef = DefDef(funcSym, argss => Some('{"override"}.asTerm))
    
    val clsDef = ClassDef(cls, parents, body = List(funcDef))
    val newCls = Typed(Apply(Select(New(TypeIdent(cls)), cls.primaryConstructor), Nil), TypeTree.of[A])

    Block(List(clsDef), newCls).asExprOf[A]
  }
}

/*
val name: String = s"${sym.name}AutoImpl"

val parents = if (isAbstract){
  List(tree)
} else {
  List(TypeTree.of[Object], tree)
}


def mkConstr(cls: Symbol) = Symbol.newMethod(cls,
  "<init>",
  MethodType(abstractMethods.map(_.symbol.name))(_ => abstractMethods.map(_.returnTpt.symbol.typeRef), _ => cls.typeRef), Flags.Method | Flags.StableRealizable, Symbol.noSymbol)
def decls(cls: Symbol): List[Symbol] = List(mkConstr(cls))
val cls = Symbol.newClass(Symbol.spliceOwner, name, parents = parents.map(_.tpe), (cls: Symbol) => decls(cls), selfType = None)


def vals(f: Flags) = abstractMethods.map {
  m =>
    Symbol.newVal(cls, m.symbol.name, m.returnTpt.symbol.typeRef, f, Symbol.noSymbol).tree.asInstanceOf[ValDef]
}
val clsDef = ClassDef(cls, parents, body =  vals(Flags.Override | Flags.ParamAccessor))


val consDef = DefDef(mkConstr(cls), argss => None)
val consDef2 = DefDef.copy(consDef)(consDef.name, List(TermParamClause(vals(Flags.Param))), consDef.returnTpt, None)


val clsDef2 = ClassDef.copy(clsDef)(name, consDef2, parents, None, vals(Flags.Override | Flags.ParamAccessor))
 */