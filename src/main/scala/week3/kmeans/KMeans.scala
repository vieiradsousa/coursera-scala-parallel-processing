package kmeans

import org.scalameter._

import scala.annotation.tailrec
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.{ParMap, ParSeq}
import scala.collection.{Map, Seq, mutable}
import scala.util.Random

class KMeans extends KMeansInterface {

  def generatePoints(k: Int, num: Int): Seq[Point] = {
    val randx = new Random(1)
    val randy = new Random(3)
    val randz = new Random(5)
    (0 until num)
      .map({ i =>
        val x = ((i + 1) % k) * 1.0 / k + randx.nextDouble() * 0.5
        val y = ((i + 5) % k) * 1.0 / k + randy.nextDouble() * 0.5
        val z = ((i + 7) % k) * 1.0 / k + randz.nextDouble() * 0.5
        new Point(x, y, z)
      }).to(mutable.ArrayBuffer)
  }

  def initializeMeans(k: Int, points: Seq[Point]): Seq[Point] = {
    val rand = new Random(7)
    (0 until k).map(_ => points(rand.nextInt(points.length))).to(mutable.ArrayBuffer)
  }

  def findClosest(p: Point, means: IterableOnce[Point]): Point = {
    val it = means.iterator
    assert(it.nonEmpty)
    var closest = it.next()
    var minDistance = p.squareDistance(closest)
    while (it.hasNext) {
      val point = it.next()
      val distance = p.squareDistance(point)
      if (distance < minDistance) {
        minDistance = distance
        closest = point
      }
    }
    closest
  }

  def classify(points: Seq[Point], means: Seq[Point]): Map[Point, Seq[Point]] = {
    val mWithP = points.groupBy(findClosest(_, means))
    means.map(mean => (mean, mWithP.getOrElse(mean, Seq()))).toMap
  }

  def classify(points: ParSeq[Point], means: ParSeq[Point]): ParMap[Point, ParSeq[Point]] = {
    val mWithP = points.groupBy(findClosest(_, means))
    means.par.map(mean => (mean, mWithP.getOrElse(mean, ParSeq()))).toMap
  }

  def findAverage(oldMean: Point, points: Seq[Point]): Point = if (points.isEmpty) oldMean else {
    var x = 0.0
    var y = 0.0
    var z = 0.0
    points.foreach { p =>
      x += p.x
      y += p.y
      z += p.z
    }
    new Point(x / points.length, y / points.length, z / points.length)
  }

  def findAverage(oldMean: Point, points: ParSeq[Point]): Point = if (points.isEmpty) oldMean else {
    var x = 0.0
    var y = 0.0
    var z = 0.0
    points.seq.foreach { p =>
      x += p.x
      y += p.y
      z += p.z
    }
    new Point(x / points.length, y / points.length, z / points.length)
  }

  def update(classified: Map[Point, Seq[Point]], oldMeans: Seq[Point]): Seq[Point] = {
    oldMeans.map(mean => findAverage(mean, classified.getOrElse(mean, Seq())))
  }

  def update(classified: ParMap[Point, ParSeq[Point]], oldMeans: ParSeq[Point]): ParSeq[Point] = {
    oldMeans.par.map(mean => findAverage(mean, classified.getOrElse(mean, ParSeq())))
  }

  def converged(eta: Double, oldMeans: Seq[Point], newMeans: Seq[Point]): Boolean = {
    oldMeans.zip(newMeans).map {
      case (old, neww) => old.squareDistance(neww)
    }.forall(distance => distance <= eta)
  }

  def converged(eta: Double, oldMeans: ParSeq[Point], newMeans: ParSeq[Point]): Boolean = {
    oldMeans.zip(newMeans).par.map {
      case (old, neww) => old.squareDistance(neww)
    }.forall(distance => distance <= eta)
  }

  @tailrec
  final def kMeans(points: Seq[Point], means: Seq[Point], eta: Double): Seq[Point] = {
    val classified = classify(points, means)
    val news = update(classified, means)

    if (!converged(eta, means, news)) kMeans(points, news, eta) else news
  }

  @tailrec
  final def kMeans(points: ParSeq[Point], means: ParSeq[Point], eta: Double): ParSeq[Point] = {
    val classified = classify(points, means)
    val news = update(classified, means)

    if (!converged(eta, means, news)) kMeans(points, news, eta) else news
  }
}

/** Describes one point in three-dimensional space.
 *
 *  Note: deliberately uses reference equality.
 */
class Point(val x: Double, val y: Double, val z: Double) {
  private def square(v: Double): Double = v * v
  def squareDistance(that: Point): Double = {
    square(that.x - x)  + square(that.y - y) + square(that.z - z)
  }
  private def round(v: Double): Double = (v * 100).toInt / 100.0
  override def toString = s"(${round(x)}, ${round(y)}, ${round(z)})"
}


object KMeansRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 20,
    Key.exec.maxWarmupRuns -> 40,
    Key.exec.benchRuns -> 25,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val kMeans = new KMeans()

    val numPoints = 500000
    val eta = 0.01
    val k = 32
    val points = kMeans.generatePoints(k, numPoints)
    val means = kMeans.initializeMeans(k, points)

    val seqtime = standardConfig measure {
      kMeans.kMeans(points, means, eta)
    }

    val parPoints = points.par
    val parMeans = means.par

    val partime = standardConfig measure {
      kMeans.kMeans(parPoints, parMeans, eta)
    }

    // Additional `println` to avoid bad interaction with JLine output
    println()
    println()
    println()
    println()
    println(s"sequential time: $seqtime")
    println(s"parallel time: $partime")
    println(s"speedup: ${seqtime.value / partime.value}")
    println()
    println()
    println()
  }

  // Workaround Dotty's handling of the existential type KeyValue
  implicit def keyValueCoerce[T](kv: (Key[T], T)): KeyValue = {
    kv.asInstanceOf[KeyValue]
  }
}
