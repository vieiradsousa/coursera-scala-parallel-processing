package reductions

import org.scalameter._

import scala.annotation.tailrec

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    isBalancedV2(chars, 0)
  }

  @tailrec
  def isBalancedV2(chars: Array[Char], current: Int): Boolean = {
    if (chars.isEmpty) current == 0
    else if (chars.head == '(') isBalancedV2(chars.tail, current + 1)
    else if (chars.head == ')') current > 0 && isBalancedV2(chars.tail, current - 1)
    else isBalancedV2(chars.tail, current)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) /*: ???*/ = {
      def traverseV2(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
        val opening = chars(idx) match {
          case '(' => arg1 + 1
          case ')' => arg1 - 1
          case _ => arg1
        }

        val closing = chars(idx) match {
          case ')' => if (opening < 0) arg2 - 1 else arg2
          case _ => arg2
        }

        traverseV2(idx + 1, until, opening, closing)
      }

      if (idx == until) (arg1, arg2) else traverseV2(idx, until, arg1, arg2)
    }

    def reduce(from: Int, until: Int): (Int, Int) /*: ???*/ = {
      if (until - from <= threshold)
        traverse(from, until, 0, 0)
      else {
        val mid = (from + until) / 2
        val ((opening, closing), (opening_, closing_)) =
          parallel(reduce(from, mid), reduce(mid, until))
        (opening + opening_, closing + closing_)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
