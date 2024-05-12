package scalamacros.test

import scala.annotation.experimental
import scala.language.reflectiveCalls

@experimental
@main def test(): Unit =
  inline val className = "Foo"
  
  val foo: Base = NewClass2.classNamed2(className)
  print(foo.func("test"))
  //println(updated.func2("test"))
 // Should print "Hello, world!"
