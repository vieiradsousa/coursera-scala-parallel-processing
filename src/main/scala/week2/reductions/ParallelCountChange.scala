package reductions

import org.scalameter._

object ParallelCountChangeRunner {

  @volatile var seqResult = 0

  @volatile var parResult = 0

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 80,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val amount = 250
    val coins = List(1, 2, 5, 10, 20, 50)
    val seqtime = standardConfig measure {
      seqResult = ParallelCountChange.countChange(amount, coins)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential count time: $seqtime")

    def measureParallelCountChange(threshold: => ParallelCountChange.Threshold): Unit = try {
      val fjtime = standardConfig measure {
        parResult = ParallelCountChange.parCountChange(amount, coins, threshold)
      }
      println(s"parallel result = $parResult")
      println(s"parallel count time: $fjtime")
      println(s"speedup: ${seqtime.value / fjtime.value}")
    } catch {
      case e: NotImplementedError =>
        println("Not implemented.")
    }

    println("\n# Using moneyThreshold\n")
    measureParallelCountChange(ParallelCountChange.moneyThreshold(amount))
    println("\n# Using totalCoinsThreshold\n")
    measureParallelCountChange(ParallelCountChange.totalCoinsThreshold(coins.length))
    println("\n# Using combinedThreshold\n")
    measureParallelCountChange(ParallelCountChange.combinedThreshold(amount, coins))
  }
}

object ParallelCountChange extends ParallelCountChangeInterface {

  /** Returns the number of ways change can be made from the specified list of
   *  coins for the specified amount of money.
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    def count(money: Int, coins: List[Int]): Int =
      if (money == 0) 1
      else if (money < 0 || coins.isEmpty) 0
      else count(money, coins.tail) + count(money - coins.head, coins)

    count(money, coins)
  }

  type Threshold = (Int, List[Int]) => Boolean

  /** In parallel, counts the number of ways change can be made from the
   *  specified list of coins for the specified amount of money.
   */
  def parCountChange(money: Int, coins: List[Int], threshold: Threshold): Int = {
    if (money < 0 || coins.isEmpty) 0
    else if (money == 0) 1
    else if (threshold(money, coins)) countChange(money, coins)
    else if (coins.head > money) parCountChange(money, coins.tail, threshold)
    else {
      val (x, y) = parallel(
        parCountChange(money, coins.tail, threshold),
        parCountChange(money - coins.head, coins, threshold)
      )
      x + y
    }

  }

  /** Threshold heuristic based on the starting money. */
  def moneyThreshold(startingMoney: Int): Threshold = {
    val threshold = (startingMoney * 2) / 3
    (money: Int, _: List[Int]) => money <= threshold
  }

  /** Threshold heuristic based on the total number of initial coins. */
  def totalCoinsThreshold(totalCoins: Int): Threshold = {
    val threshold = 2 * totalCoins / 3
    (_: Int, coins: List[Int]) => coins.length <= threshold
  }

  /** Threshold heuristic based on the starting money and the initial list of coins. */
  def combinedThreshold(startingMoney: Int, allCoins: List[Int]): Threshold = {
    val threshold = startingMoney * allCoins.length / 2
    (money: Int, coins: List[Int]) => money * coins.length <= threshold
  }
}
