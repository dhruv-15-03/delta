/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.commands.merge

import java.io.{File, FileInputStream, RandomAccessFile}
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable

import com.databricks.spark.util.Log4jUsageLogger
import jdk.jfr.Recording
import jdk.jfr.consumer.RecordingFile
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.functions.{col, concat, lit, sha2}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.Utils

/**
 * Optimistic literal-bound controls, not an implementation benchmark. Bounds are supplied from
 * known synthetic input; source-summary and additional transaction-read costs are not included.
 */
object MergeSourcePruningBenchmark {
  private val numRows = 1L << 20
  private val numFiles = 64
  private val repetitions = 3

  private case class Shape(name: String, sourceSql: String, membership: String, bound: String)

  private def shapes: Seq[Shape] = Seq(
    Shape("contiguous", "SELECT id key, 1L value FROM range(65536, 65600)",
      "key >= 65536L AND key < 65600L", "t.key >= 65536L AND t.key <= 65599L"),
    Shape("separated",
      s"SELECT id key, 1L value FROM range(32) UNION ALL " +
        s"SELECT id key, 1L value FROM range(${numRows - 32}, $numRows)",
      s"key < 32L OR key >= ${numRows - 32}L", s"t.key >= 0L AND t.key <= ${numRows - 1}L"),
    Shape("random", s"SELECT pmod(id * 7919L, $numRows) key, 1L value FROM range(128)",
      s"key IN (SELECT pmod(id * 7919L, $numRows) FROM range(128))",
      "t.key >= 0L AND t.key <= 1005713L"),
    Shape("dense", s"SELECT id key, 1L value FROM range($numRows)",
      "true", s"t.key >= 0L AND t.key <= ${numRows - 1}L"),
    Shape("empty", "SELECT 1L key, 1L value WHERE false", "false", "true"),
    Shape("all-null", "SELECT CAST(NULL AS BIGINT) key, 1L value", "false", "true"),
    Shape("disjoint", s"SELECT id key, 1L value FROM range($numRows, ${numRows + 64})",
      "false", s"t.key >= ${numRows}L AND t.key <= ${numRows + 63}L")
  )

  private[merge] def execute(spark: SparkSession, statement: String): (Array[Row], MergeStats) = {
    var rows = Array.empty[Row]
    val events = Log4jUsageLogger.track {
      rows = spark.sql(statement).collect()
    }.filter(_.tags.get("opType").contains("delta.dml.merge.stats"))
    require(events.size == 1, s"Expected one MERGE event, found ${events.size}.")
    (rows, JsonUtils.fromJson[MergeStats](events.head.blob))
  }

  private def readBytes(recordingPath: Path): Map[String, Long] = {
    val totals = mutable.Map.empty[String, Long].withDefaultValue(0L)
    val input = new RecordingFile(recordingPath)
    try {
      while (input.hasMoreEvents) {
        val event = input.readEvent()
        if (event.getEventType.getName == "jdk.FileRead") {
          val path = event.getString("path")
          require(path != null, "JFR FileRead event is missing its path.")
          val canonical = new File(path).getCanonicalPath
          val bytes = event.getLong("bytesRead")
          require(bytes >= 0, s"Unexpected negative JFR byte count: $bytes")
          totals(canonical) += bytes
        }
      }
      totals.toMap
    } finally {
      input.close()
    }
  }

  private def recording(): Recording = {
    val result = new Recording()
    result.enable("jdk.FileRead").withThreshold(Duration.ZERO).withoutStackTrace()
    result
  }

  private[merge] def verifyFileReadAccounting(directory: File): Unit = {
    val data = new File(directory, "accounting.bin")
    Files.write(data.toPath, Array.fill[Byte](8192)(7))
    val path = new File(directory, "accounting.jfr").toPath
    val recorder = recording()
    try {
      recorder.start()
      val stream = new FileInputStream(data)
      try {
        require(stream.read(new Array[Byte](4096)) == 4096)
      } finally {
        stream.close()
      }
      val positioned = new RandomAccessFile(data, "r")
      try {
        require(positioned.getChannel.read(ByteBuffer.allocate(4096), 4096) == 4096)
      } finally {
        positioned.close()
      }
      recorder.stop()
      recorder.dump(path)
      require(readBytes(path).get(data.getCanonicalPath).contains(8192L),
        "JFR must account for both stream and positioned FileChannel reads.")
    } finally {
      recorder.close()
      Files.deleteIfExists(path)
    }
  }

  private class WorkListener extends SparkListener {
    private val stages = mutable.Map.empty[Int, String]
    private val work = mutable.Map.empty[String, mutable.Map[String, Long]]
    private var jobs = 0L

    override def onJobStart(event: SparkListenerJobStart): Unit = synchronized {
      jobs += 1
      val description = Option(event.properties)
        .flatMap(p => Option(p.getProperty("spark.job.description"))).getOrElse("unspecified")
      event.stageIds.foreach(id => stages(id) = description.take(180))
    }

    override def onTaskEnd(event: SparkListenerTaskEnd): Unit = synchronized {
      val phase = stages.getOrElse(event.stageId, "unknown stage")
      val values = work.getOrElseUpdate(phase,
        mutable.Map.empty[String, Long].withDefaultValue(0L))
      values("taskAttempts") += 1
      if (event.taskInfo.attemptNumber > 0) values("retriedTaskAttempts") += 1
      if (event.taskInfo.speculative) values("speculativeTaskAttempts") += 1
      if (!event.taskInfo.successful) values("unsuccessfulTaskAttempts") += 1
      val metrics = event.taskMetrics
      if (metrics != null) {
        values("executorRunTimeMs") += metrics.executorRunTime
        values("executorCpuTimeNs") += metrics.executorCpuTime
        values("inputBytesAllPaths") += metrics.inputMetrics.bytesRead
        values("shuffleReadBytes") += metrics.shuffleReadMetrics.totalBytesRead
        values("shuffleWriteBytes") += metrics.shuffleWriteMetrics.bytesWritten
        values("memorySpillBytes") += metrics.memoryBytesSpilled
        values("diskSpillBytes") += metrics.diskBytesSpilled
        values("maxTaskPeakExecutionMemoryBytes") =
          math.max(values("maxTaskPeakExecutionMemoryBytes"), metrics.peakExecutionMemory)
      }
    }

    def result: Map[String, Any] = synchronized {
      Map("jobs" -> jobs, "stages" -> stages.size,
        "workByJobDescription" -> work.map { case (phase, values) => phase -> values.toMap }.toMap)
    }
  }

  private class PlanListener(retainPlans: Boolean) extends QueryExecutionListener {
    private val plans = mutable.ArrayBuffer.empty[Map[String, Any]]
    val failure = new AtomicReference[Exception]()

    override def onSuccess(name: String, execution: QueryExecution, duration: Long): Unit =
      synchronized {
        val plan = execution.executedPlan.toString
        if (plans.size < 16) {
          plans += Map(
            "name" -> name,
            "durationNs" -> duration,
            "sortMergeJoin" -> plan.contains("SortMergeJoin"),
            "broadcastHashJoin" -> plan.contains("BroadcastHashJoin"),
            "dynamicPruning" -> plan.toLowerCase(java.util.Locale.ROOT).contains("dynamicpruning"),
            "bloomFilter" -> plan.toLowerCase(java.util.Locale.ROOT).contains("bloomfilter"),
            "planPrefix" -> (if (retainPlans) plan.take(1536) else ""))
        }
      }

    override def onFailure(name: String, execution: QueryExecution, error: Exception): Unit = {
      failure.compareAndSet(null, error)
    }

    def result: Seq[Map[String, Any]] = synchronized { plans.toVector }
  }

  private def manifest(directory: File, snapshot: Snapshot): Map[String, Long] = {
    snapshot.allFiles.collect().map { file =>
      val uri = new java.net.URI(file.path)
      val local = if (uri.getScheme == "file") {
        new File(uri)
      } else {
        require(uri.getScheme == null, "Only synthetic local data is allowed.")
        val path = new File(uri.getPath)
        if (path.isAbsolute) path else new File(directory, uri.getPath)
      }
      local.getCanonicalPath -> file.size
    }.toMap
  }

  private def writeResult(destination: Path, value: Map[String, Any]): Unit = {
    val bytes = (JsonUtils.toJson(value) + "\n").getBytes(UTF_8)
    require(bytes.length <= 1536 * 1024, "Benchmark JSON exceeds its 1.5 MiB budget.")
    Files.write(destination, bytes)
  }

  private def sameRows(actual: DataFrame, expected: DataFrame): Unit = {
    require(actual.exceptAll(expected).limit(1).count() == 0, "Unexpected output rows.")
    require(expected.exceptAll(actual).limit(1).count() == 0, "Missing output rows.")
  }

  private def runOne(
      spark: SparkSession,
      base: File,
      inputs: Map[String, Long],
      shape: Shape,
      bounded: Boolean,
      retainPlans: Boolean): Map[String, Any] = {
    val root = Files.createTempDirectory(base.getParentFile.toPath, "run-").toFile
    val target = new File(root, "target")
    val jfr = new File(root, "reads.jfr").toPath
    val recorder = recording()
    val work = new WorkListener
    val plans = new PlanListener(retainPlans)
    try {
      spark.sql(s"CREATE TABLE delta.`${target.getCanonicalPath}` SHALLOW CLONE " +
        s"delta.`${base.getCanonicalPath}` VERSION AS OF 0").collect()
      val before = DeltaLog.forTable(spark, target).update()
      require(manifest(target, before) == inputs, "The cloned input file manifest changed.")
      val expected = spark.read.format("delta").load(base.getCanonicalPath)
        .selectExpr("key", s"IF(${shape.membership}, 1L, 0L) value", "payload")
      val expectedUpdates = expected.filter("value = 1L").count()
      val heapBefore = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
      spark.sparkContext.listenerBus.waitUntilEmpty(30000)
      spark.sparkContext.addSparkListener(work)
      spark.listenerManager.register(plans)
      recorder.start()
      val started = System.nanoTime()
      val predicate = if (bounded) s" AND (${shape.bound})" else ""
      val (rows, stats) = execute(spark,
        s"MERGE INTO delta.`${target.getCanonicalPath}` t USING pruning_benchmark_source s " +
          s"ON t.key = s.key$predicate WHEN MATCHED THEN UPDATE SET value = s.value")
      val duration = System.nanoTime() - started
      recorder.stop()
      spark.sparkContext.listenerBus.waitUntilEmpty(30000)
      spark.listenerManager.unregister(plans)
      spark.sparkContext.removeSparkListener(work)
      require(plans.failure.get() == null, "A measured query failed.")
      val heapAfter = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
      recorder.dump(jfr)
      val bytesByPath = readBytes(jfr)
      val targetBytes = bytesByPath.iterator.filter(e => inputs.contains(e._1)).map(_._2).sum
      require(rows.toSeq == Seq(Row(expectedUpdates, expectedUpdates, 0L, 0L)),
        "Affected-row results differ from the expected synthetic update set.")
      require(stats.targetRowsUpdated == expectedUpdates)
      sameRows(spark.read.format("delta").load(target.getCanonicalPath), expected)
      require(DeltaLog.forTable(spark, base).update().version == 0,
        "The pinned source snapshot was modified.")
      Map(
        "arm" -> (if (bounded) "literal-bound-control" else "baseline"),
        "durationNs" -> duration,
        "targetInstrumentedFileReadBytes" -> targetBytes,
        "targetFilesWithReadEvents" -> bytesByPath.keys.count(inputs.contains),
        "otherInstrumentedFileReadBytes" ->
          bytesByPath.iterator.filterNot(e => inputs.contains(e._1)).map(_._2).sum,
        "heapBeforeBytes" -> heapBefore,
        "heapAfterBytes" -> heapAfter,
        "candidateFiles" -> stats.targetAfterSkipping.files,
        "candidateFileSizes" -> stats.targetAfterSkipping.bytes,
        "touchedFilesRemoved" -> stats.targetFilesRemoved,
        "materializeSourceTimeMs" -> stats.materializeSourceTimeMs,
        "sourceMaterializationReason" -> stats.materializeSourceReason,
        "sourceRows" -> stats.source.rows,
        "sourceRowsInSecondScan" -> stats.sourceRowsInSecondScan,
        "scanTimeMs" -> stats.scanTimeMs,
        "rewriteTimeMs" -> stats.rewriteTimeMs,
        "commitVersion" -> stats.commitVersion,
        "updatedRows" -> stats.targetRowsUpdated,
        "work" -> work.result,
        "plans" -> plans.result)
    } finally {
      spark.listenerManager.unregister(plans)
      spark.sparkContext.removeSparkListener(work)
      recorder.close()
      Utils.deleteRecursively(root)
    }
  }

  private def median(values: Seq[Double]): Double = {
    val sorted = values.sorted
    sorted(sorted.size / 2)
  }

  def measureControls(spark: SparkSession, directory: File, output: Path): Unit = {
    require(spark.sparkContext.master == "local[2]", "Use the planned local[2] resource bound.")
    require(Runtime.getRuntime.maxMemory() <= 1100L * 1024 * 1024, "Test heap exceeds 1 GiB.")
    Files.createDirectories(output)
    val destination = output.resolve("literal-controls.json")
    require(!Files.exists(destination), "Refusing to overwrite previous benchmark results.")
    val runs = mutable.ArrayBuffer.empty[Map[String, Any]]
    val summaries = mutable.ArrayBuffer.empty[Map[String, Any]]
    val settings = Seq(
      "spark.sql.adaptive.enabled", "spark.sql.optimizer.dynamicPartitionPruning.enabled",
      "spark.sql.optimizer.runtime.bloomFilter.enabled", "spark.sql.autoBroadcastJoinThreshold",
      "spark.sql.shuffle.partitions", "spark.databricks.delta.stats.skipping",
      "spark.databricks.delta.merge.optimizeMatchedOnlyMerge.enabled",
      "spark.databricks.delta.merge.materializeSource")
      .map(key => key -> spark.conf.getOption(key)).toMap
    def save(status: String): Unit = writeResult(destination, Map(
      "status" -> status, "kind" -> "optimistic literal-bound diagnostic",
      "summaryPreparationIncluded" -> false, "additionalTransactionReadIncluded" -> false,
      "productionOptimizationImplemented" -> false, "numRows" -> numRows, "numFiles" -> numFiles,
      "repetitions" -> repetitions, "settings" -> settings,
      "cacheState" -> "warmed JVM; OS cache uncontrolled, not cold disk",
      "byteSemantics" -> "JDK FileRead bytes for pinned target paths, not physical/network bytes",
      "memorySemantics" -> "heap endpoints and max per-task execution memory, not peak total RSS",
      "runs" -> runs.toVector, "summaries" -> summaries.toVector))
    save("in_progress")
    try {
      Seq("clustered", "random").foreach { layout =>
        val base = new File(directory, s"base-$layout")
        val generated = spark.range(0, numRows, 1, numFiles).toDF("key")
          .withColumn("value", lit(0L))
          .withColumn("payload", sha2(concat(col("key"), lit("-fixed-seed-42")), 256))
        val targetData = if (layout == "clustered") generated else generated.repartition(numFiles)
        targetData.write.format("delta").option("compression", "snappy")
          .option("delta.enableDeletionVectors", "false").save(base.getCanonicalPath)
        val snapshot = DeltaLog.forTable(spark, base).update()
        val inputs = manifest(base, snapshot)
        require(inputs.size <= 256 && inputs.values.sum <= 512L * 1024 * 1024)
        val contiguous = shapes.head
        spark.sql(contiguous.sourceSql).createOrReplaceTempView("pruning_benchmark_source")
        Seq(false, true).foreach { bounded =>
          runOne(spark, base, inputs, contiguous, bounded, retainPlans = false)
        }
        val noise = (0 until 2).map { _ =>
          val first = runOne(spark, base, inputs, contiguous, false, retainPlans = false)
          val second = runOne(spark, base, inputs, contiguous, false, retainPlans = false)
          val a = first("durationNs").toString.toDouble
          val b = second("durationNs").toString.toDouble
          runs += first ++ Map("layout" -> layout, "shape" -> "baseline-calibration")
          runs += second ++ Map("layout" -> layout, "shape" -> "baseline-calibration")
          math.abs(a - b) / math.max(a, b)
        }.max
        shapes.foreach { shape =>
          spark.sql(shape.sourceSql).createOrReplaceTempView("pruning_benchmark_source")
          require(spark.table("pruning_benchmark_source").count() * 16 <= 64L * 1024 * 1024)
          val pairs = (0 until repetitions).map { iteration =>
            val arms = if (iteration % 2 == 0) Seq(false, true) else Seq(true, false)
            val results = arms.map { bounded =>
              val result = runOne(spark, base, inputs, shape, bounded, iteration == 0)
              runs += result ++ Map("layout" -> layout, "shape" -> shape.name,
                "iteration" -> iteration)
              bounded -> result
            }.toMap
            save("in_progress")
            results
          }
          val durationReduction = median(pairs.map { pair =>
            1.0 - pair(true)("durationNs").toString.toDouble /
              pair(false)("durationNs").toString.toDouble
          })
          val baselineBytes = median(pairs.map(_(false)(
            "targetInstrumentedFileReadBytes").toString.toDouble))
          val controlBytes = median(pairs.map(_(true)(
            "targetInstrumentedFileReadBytes").toString.toDouble))
          if (shape.name == "contiguous") {
            require(baselineBytes > 0 && controlBytes > 0,
              "JFR did not observe real target reads; file sizes cannot substitute.")
          }
          summaries += Map("layout" -> layout, "shape" -> shape.name,
            "baselineNoiseRatio" -> noise, "medianPairedDurationReduction" -> durationReduction,
            "exceedsBaselineNoise" -> (durationReduction > noise),
            "medianBaselineTargetReadBytes" -> baselineBytes,
            "medianControlTargetReadBytes" -> controlBytes)
          save("in_progress")
        }
      }
      save("complete")
    } finally {
      spark.catalog.dropTempView("pruning_benchmark_source")
    }
  }
}
