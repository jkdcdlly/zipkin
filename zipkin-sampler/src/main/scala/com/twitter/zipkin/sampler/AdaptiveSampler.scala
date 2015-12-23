/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.twitter.zipkin.sampler

import com.twitter.finagle.http.HttpMuxer
import java.net.InetSocketAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import com.google.common.collect.EvictingQueue
import com.twitter.app.App
import com.twitter.conversions.time._
import com.twitter.finagle.{Filter, Service}
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util._
import com.twitter.zipkin.common.Span

import scala.reflect.ClassTag

/**
 * The adaptive sampler optimizes sampling towards a global rate. This state
 * is maintained in ZooKeeper.
 *
 * {{{
 * object MyCollectorServer extends TwitterServer
 *   with ..
 *   with AdaptiveSampler {
 *
 *   // Sampling will adjust dynamically towards a target rate.
 *   override def spanStoreFilter = newAdaptiveSamplerFilter()
 *
 *   def main() {
 *
 *     // Adds endpoints to adjust the sample rate via http
 *     configureAdaptiveSamplerHttpApi()
 *
 * --snip--
 * }}}
 *
 */
trait AdaptiveSampler { self: App =>
  val asBasePath = flag(
    "zipkin.sampler.adaptive.basePath",
    "/com/twitter/zipkin/sampler/adaptive",
    "Base path in ZooKeeper for the sampler to use")

  val asApiPath = flag(
    "zipkin.sampler.adaptive.apiPath",
    "/admin/sampler/adaptive",
    "Http path under for /targetStoreRate and /sampleRate, each accepting the newRate parameter")

  val asUpdateFreq = flag(
    "zipkin.sampler.adaptive.updateFreq",
    30.seconds,
    "Frequency with which to update the sample rate")

  val asWindowSize = flag(
    "zipkin.sampler.adaptive.windowSize",
    30.minutes,
    "Amount of request rate data to base sample rate on")

  val asSufficientWindowSize = flag(
    "zipkin.sampler.adaptive.sufficientWindowSize",
    10.minutes,
    "Amount of request rate data to gather before calculating sample rate")

  val asOutlierThreshold = flag(
    "zipkin.sampler.adaptive.outlierThreshold",
    5.minutes,
    "Amount of time to see outliers before updating sample rate")

  val zkServerLocations = flag(
    "zipkin.zookeeper.location",
    Seq(new InetSocketAddress(2181)),
    "Location of the ZooKeeper server")

  val zkServerCredentials = flag(
    "zipkin.zookeeper.credentials",
    "[none]",
    "Optional credentials of the form 'username:password'")

  lazy val zkClient = {
    val creds = zkServerCredentials.get map { creds =>
      val Array(u, p) = creds.split(':')
      (u, p)
    }
    new ZKClient(zkServerLocations(), creds)
  }

  def adaptiveSampleRateCalculator(
    targetStoreRate: Var[Int],
    curReqRate: Var[Int],
    sampleRate: Var[Double],
    stats: StatsReceiver,
    log: Logger
  ): (Option[Seq[Int]] => Option[Double]) = {
    new RequestRateCheck[Seq[Int]](curReqRate, stats.scope("reqRateCheck"), log) andThen
    new SufficientDataCheck[Int](asSufficientWindowSize().inSeconds / asUpdateFreq().inSeconds, stats.scope("sufficientDataCheck"), log) andThen
    new ValidDataCheck[Int](_ > 0, stats.scope("validDataCheck"), log) andThen
    new OutlierCheck(curReqRate, asOutlierThreshold().inSeconds / asUpdateFreq().inSeconds, stats = stats.scope("outlierCheck"), log = log) andThen
    new CalculateSampleRate(targetStoreRate, sampleRate, stats = stats.scope("sampleRateCalculator"), log = log)
  }

  /**
   * Adds an http api under the path [[asApiPath]] with the following endpoints:
   *
   * - /targetStoreRate
   *   - Adapt rate such that the collector will write this many spans to storage.
   * - /sampleRate
   *   - Manually set the sampling rate. If this is set to 0 it will not adjust towards targetStoreRate.
   *
   * Both endpoints respond to the query param `newRate`.
   *
   * For example, once invoked, the following endpoints will respond with corresponding rates:
   *
   * - http://collectorhost/admin/sampler/adaptive/targetStoreRate
   * - http://collectorhost/admin/sampler/adaptive/sampleRate
   */
  def configureAdaptiveSamplerHttpApi(): Unit = {
    HttpMuxer.addHandler(
      asApiPath() + "/targetStoreRate",
      new ZkPathUpdater[Int](zkClient, asBasePath() + "/targetStoreRate", _.toInt, _ >= 0))

    HttpMuxer.addHandler(
      asApiPath() + "/sampleRate",
      new ZkPathUpdater[Double](zkClient, asBasePath() + "/sampleRate", _.toDouble, _ >= 0.0))
  }

  def newAdaptiveSamplerFilter(
    electionPath: String = asBasePath() + "/election",
    reporterPath: String = asBasePath() + "/requestRates",
    sampleRatePath: String = asBasePath() + "/sampleRate",
    targetStoreRatePath: String = asBasePath() + "/targetStoreRate",
    stats: StatsReceiver = DefaultStatsReceiver.scope("adaptiveSampler"),
    log: Logger = Logger.get("adaptiveSampler")
  ): Filter[Seq[Span], Unit, Seq[Span], Unit] = {
    def translateNode[T](name: String, default: T, f: String => T): Array[Byte] => T = { bytes =>
      if (bytes.length == 0) {
        log.debug("node translator [%s] defaulted to \"%s\"".format(name, default))
        default
      } else {
        val str = new String(bytes)
        log.debug("node translator [%s] got \"%s\"".format(name, str))
        try {
          f(str)
        } catch {
          case e: Exception =>
            log.error(e, "node translator [%s] error".format(name))
            default
        }
      }
    }

    val targetStoreRateWatch = zkClient.watchData(targetStoreRatePath)
    val targetStoreRate = targetStoreRateWatch.data.map(translateNode("targetStoreRate", 0, _.toInt))

    val curReqRate = Var[Int](0)
    val reportingGroup = zkClient.joinGroup(reporterPath, curReqRate.map(_.toString.getBytes))

    val sampleRateWatch = zkClient.watchData(sampleRatePath)
    val sampleRate = sampleRateWatch.data.map(translateNode("sampleRate", 0.0, _.toDouble))

    val buffer = new AtomicRingBuffer[Int](asWindowSize().inSeconds / asUpdateFreq().inSeconds)

    val isLeader = new IsLeaderCheck[Double](zkClient, electionPath, stats.scope("leaderCheck"), log)
    val cooldown = new CooldownCheck[Double](asOutlierThreshold(), stats.scope("cooldownCheck"), log)

    val calculator =
      { v: Int => Some(buffer.pushAndSnap(v)) } andThen
      adaptiveSampleRateCalculator(targetStoreRate, curReqRate, sampleRate, stats, log) andThen
      isLeader andThen
      cooldown

    val globalSampleRateUpdater = new GlobalSampleRateUpdater(
      zkClient,
      sampleRatePath,
      reporterPath,
      asUpdateFreq(),
      calculator,
      stats.scope("globalRateUpdater"),
      log)

    onExit {
      val closer = Closable.all(
        targetStoreRateWatch,
        reportingGroup,
        sampleRateWatch,
        isLeader,
        globalSampleRateUpdater)

      Await.ready(closer.close())
    }

    new SpanSamplerFilter(new Sampler(sampleRate, stats.scope("sampler")), stats.scope("filter")) andThen
    new FlowReportingFilter(curReqRate.update(_), stats.scope("flowReporter"))
  }

}

class AtomicRingBuffer[T: ClassTag](maxSize: Int) {
  private[this] val underlying: EvictingQueue[T] = EvictingQueue.create[T](maxSize)

  def pushAndSnap(newVal: T): Seq[T] = synchronized {
    underlying.add(newVal)
    val arr = underlying.toArray.asInstanceOf[Array[T]]
    arr.reverse
  }
}

/**
 * A SpanStore Filter that updates `curVar` with the current
 * flow based every `freq`.
 */
class FlowReportingFilter(
  update: (Int => Unit),
  stats: StatsReceiver = DefaultStatsReceiver.scope("flowReporter"),
  freq: Duration = 30.seconds,
  timer: Timer = DefaultTimer.twitter
) extends Filter[Seq[Span], Unit, Seq[Span], Unit] with Closable {

  private[this] val spanCount = new AtomicInteger
  private[this] val countGauge = stats.addGauge("spanCount") { spanCount.get }

  private[this] val updateTask = timer.schedule(freq) {
    update((1.0 * spanCount.getAndSet(0) * 60.seconds.inNanoseconds / freq.inNanoseconds).toInt)
  }

  def apply(spans: Seq[Span], store: Service[Seq[Span], Unit]): Future[Unit] = {
    spanCount.addAndGet(spans.size)
    store(spans)
  }

  def close(deadline: Time): Future[Unit] =
    updateTask.close(deadline)
}

/**
 * A filter that uses ZK leader election to decide which node should be allowed
 * to operate on the incoming value.
 */
class IsLeaderCheck[T](
  zkClient: ZKClient,
  electionPath: String,
  stats: StatsReceiver = DefaultStatsReceiver.scope("leaderCheck"),
  log: Logger = Logger.get("IsLeaderCheck")
) extends (Option[T] => Option[T]) with Closable {
  private[this] val isLeader = new AtomicBoolean(false)
  private[this] val leadership = zkClient.offerLeadership(electionPath)
  leadership.data.changes.register(Witness(isLeader.set(_)))

  private[this] val isLeaderGauge = stats.addGauge("isLeader") { if (isLeader.get) 1 else 0 }

  def apply(in: Option[T]): Option[T] =
    in filter { _ =>
      log.debug("is leader check: " + isLeader.get)
      isLeader.get
    }

  def close(deadline: Time): Future[Unit] =
    leadership.close(deadline)
}

/**
 * Pulls group data from `reporterPath` every `updateFreq` and passes the sum to `calculate`. If
 * this instance is the current leader of `electionPath` and `calculate` returns Some(val) the
 * global sample rate at `sampleRatePath` will be updated.
 */
class GlobalSampleRateUpdater(
  zkClient: ZKClient,
  sampleRatePath: String,
  reporterPath: String,
  updateFreq: Duration,
  calculate: (Int => Option[Double]),
  stats: StatsReceiver = DefaultStatsReceiver.scope("globalSampleRateUpdater"),
  log: Logger = Logger.get("GlobalSampleRateUpdater")
) extends Closable {
  private[this] val globalRateCounter = stats.counter("rate")

  private[this] val dataWatcher = zkClient.groupData(reporterPath, updateFreq)
  dataWatcher.data.changes.register(Witness { vals =>
    val memberVals = vals map {
      case bytes: Array[Byte] =>
        try new String(bytes).toInt catch { case e: Exception => 0 }
      case _ => 0
    }

    val sum = memberVals.sum
    log.debug("global rate update: " + sum + " " + memberVals)
    globalRateCounter.incr((sum * 1.0 * updateFreq.inNanoseconds / 60.seconds.inNanoseconds).toInt)

    calculate(sum) foreach { rate =>
      log.debug("setting new sample rate: " + sampleRatePath + " " + rate)
      zkClient.setData(sampleRatePath, rate.toString.getBytes) onFailure { cause =>
        log.error(cause, s"could not set sample rate to $rate for $sampleRatePath")
      }
    }
  })

  def close(deadline: Time): Future[Unit] =
    dataWatcher.close(deadline)
}

class RequestRateCheck[T](
  reqRate: Var[Int],
  stats: StatsReceiver = DefaultStatsReceiver.scope("requestRateCheck"),
  log: Logger = Logger.get("RequestRateCheck")
) extends (Option[T] => Option[T]) {
  private[this] val curRate = new AtomicInteger(0)
  reqRate.changes.register(Witness(curRate.set(_)))

  private[this] val curRateGauge = stats.addGauge("curRate") { curRate.get }
  private[this] val validCounter = stats.counter("valid")
  private[this] val invalidCounter = stats.counter("invalid")

  def apply(in: Option[T]): Option[T] =
    in filter { _ =>
      val valid = curRate.get > 0
      (if (valid) validCounter else invalidCounter).incr()
      valid
    }
}

class SufficientDataCheck[T](
  sufficientThreshold: Int,
  stats: StatsReceiver = DefaultStatsReceiver.scope("sufficientDataCheck"),
  log: Logger = Logger.get("SufficientDataCheck")
) extends (Option[Seq[T]] => Option[Seq[T]]) {
  private[this] val sufficientCounter = stats.counter("sufficient")
  private[this] val insufficientCounter = stats.counter("insufficient")

  def apply(in: Option[Seq[T]]): Option[Seq[T]] =
    in filter { i =>
      val sufficient = i.length >= sufficientThreshold
      log.debug("checking for sufficient data: " + sufficient + " |  " + i.length + " | " + sufficientThreshold)
      (if (sufficient) sufficientCounter else insufficientCounter).incr()
      sufficient
    }
}

class ValidDataCheck[T](
  validate: T => Boolean,
  stats: StatsReceiver = DefaultStatsReceiver.scope("validDataCheck"),
  log: Logger = Logger.get("ValidDataCheck")
) extends (Option[Seq[T]] => Option[Seq[T]]) {
  private[this] val validCounter = stats.counter("valid")
  private[this] val invalidCounter = stats.counter("invalid")

  def apply(in: Option[Seq[T]]): Option[Seq[T]] =
    in filter { i =>
      val valid = i.forall(validate)
      log.debug("validating data: " + valid + " | " + i)
      (if (valid) validCounter else invalidCounter).incr()
      valid
    }
}

class CooldownCheck[T](
  period: Duration,
  stats: StatsReceiver = DefaultStatsReceiver.scope("cooldownCheck"),
  log: Logger = Logger.get("CooldownCheck"),
  timer: Timer = DefaultTimer.twitter
) extends (Option[T] => Option[T]) {
  private[this] val permit = new AtomicBoolean(true)
  private[this] val coolingGauge = stats.addGauge("cooling") { if (permit.get) 0 else 1 }

  def apply(in: Option[T]): Option[T] =
    in filter { _ =>
      val allow = permit.compareAndSet(true, false)
      log.debug("checking cooldown: " + allow)
      if (allow) timer.doLater(period) { permit.set(true) }
      allow
    }
}

class OutlierCheck(
  reqRate: Var[Int],
  requiredDataPoints: Int,
  threshold: Double = 0.15,
  stats: StatsReceiver = DefaultStatsReceiver.scope("outlierCheck"),
  log: Logger = Logger.get("OutlierCheck")
) extends (Option[Seq[Int]] => Option[Seq[Int]]) {
  private[this] val curRate = new AtomicInteger(0)
  reqRate.changes.register(Witness(curRate.set(_)))

  def apply(in: Option[Seq[Int]]): Option[Seq[Int]] =
    in filter { buf =>
      val outliers = buf.segmentLength(isOut(curRate.get, _), buf.length - requiredDataPoints)
      log.debug("checking for outliers: " + outliers + " " + requiredDataPoints)
      outliers == requiredDataPoints
    }

  private[this] def isOut(curRate: Int, datum: Int): Boolean =
    math.abs(datum - curRate) > curRate * threshold
}

object DiscountedAverage extends (Seq[Int] => Double) {
  def apply(vals: Seq[Int]): Double =
    calculate(vals, 0.9)

  def calculate(vals: Seq[Int], discount: Double): Double = {
    val discountTotal = (0 until vals.length).map(math.pow(discount, _)).sum
    vals.zipWithIndex.map { case (e, i) => math.pow(discount, i) * e }.sum / discountTotal
  }

  def truncate(x: Double): Double = (x * 1000).toInt.toDouble / 1000
}

class CalculateSampleRate(
  targetStoreRate: Var[Int],
  sampleRate: Var[Double],
  calculate: Seq[Int] => Double = DiscountedAverage,
  threshold: Double = 0.05,
  maxSampleRate: Double = 1.0,
  stats: StatsReceiver = DefaultStatsReceiver.scope("sampleRateCalculator"),
  log: Logger = Logger.get("CalculateSampleRate")
) extends (Option[Seq[Int]] => Option[Double]) {
  private[this] val tgtStoreRate = new AtomicInteger(0)
  targetStoreRate.changes.register(Witness(tgtStoreRate.set(_)))

  private[this] val cursampleRate = new AtomicReference[Double](1.0)
  sampleRate.changes.register(Witness(cursampleRate))

  private[this] val currentStoreRate = new AtomicInteger(0)

  private[this] val currentStoreRateGauge = stats.addGauge("currentStoreRate") { currentStoreRate.get }
  private[this] val tgtRateGauge = stats.addGauge("targetStoreRate") { tgtStoreRate.get }
  private[this] val curSampleRateGauge = stats.addGauge("currentSampleRate") { cursampleRate.get.toFloat }

  /**
   * Since we assume that the sample rate and storage request rate are
   * linearly related by the following:
   *
   * newSampleRate / targetStoreRate = currentSampleRate / currentStoreRate
   * thus
   * newSampleRate = (currentSampleRate * targetStoreRate) / currentStoreRate
   */
  def apply(in: Option[Seq[Int]]): Option[Double] = {
    log.debug("Calculating rate for: " + in)
    in flatMap { vals =>
      val curStoreRate = calculate(vals)
      currentStoreRate.set(curStoreRate.toInt)
      log.debug("Calculated current store rate: " + curStoreRate)
      if (curStoreRate <= 0) None else {
        val curSampleRateSnap = cursampleRate.get
        val newSampleRate = curSampleRateSnap * tgtStoreRate.get / curStoreRate
        val sr = math.min(maxSampleRate, newSampleRate)
        val change = math.abs(curSampleRateSnap - sr)/curSampleRateSnap
        log.debug(s"current store rate : $curStoreRate ; current sample rate : $curSampleRateSnap ; target store rate : $tgtStoreRate")
        log.debug(s"sample rate : old $curSampleRateSnap ; new : $sr ; threshold : $threshold ; execute : ${change >= threshold} ; % change : ${100*change}")
        if (change >= threshold) Some(sr) else None
      }
    }
  }
}
