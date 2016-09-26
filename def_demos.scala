/* ~/sparkapps/def_demos.scala

Simple demos of creating methods.

Demo:
spark-shell -i def_demos.scala

ref:
http://stackoverflow.com/questions/25029386/remove-first-and-last-element-from-scala-collection-immutable-iterablestring
*/

def removeFirstAndLast[A](xs: Iterable[A]) = xs.drop(1).dropRight(1)

removeFirstAndLast(List("one", "two", "three", "four")) map println
// Notice lack of dot and parens above
