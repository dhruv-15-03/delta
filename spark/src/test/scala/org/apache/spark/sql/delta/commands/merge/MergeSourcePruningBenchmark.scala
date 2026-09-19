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

import java.io.{File, FileDescriptor, FileInputStream, InputStream, RandomAccessFile}
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileVisitResult, Files, NoSuchFileException, Path, SimpleFileVisitor, StandardCopyOption}
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.time.{Duration, Instant}
import java.util.concurrent.{CompletableFuture, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.function.{Consumer, IntFunction}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.databricks.spark.util.Log4jUsageLogger
import jdk.jfr.{AnnotationElement, EventFactory, FlightRecorder, Name, Recording, RecordingState, ValueDescriptor}
import jdk.jfr.consumer.{RecordedEvent, RecordingFile}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.{FileRange, FSDataInputStream, Path => HadoopPath, PathHandle, RawLocalFileSystem}
import org.apache.hadoop.fs.statistics.IOStatistics

import org.apache.spark.DebugFilesystem
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
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
      val before = DeltaLog.forTable(spark, target.getCanonicalPath).update()
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
      require(DeltaLog.forTable(spark, base.getCanonicalPath).update().version == 0,
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
    require(sys.props.get("delta.mergePruning.profile").contains("full"),
      "The timing matrix requires its own explicitly selected profile.")
    val master = spark.sparkContext.master
    // TestSparkSession spells the equivalent two-worker, one-failure master as local[2,1].
    require(Set("local[2]", "local[2,1]").contains(master) &&
      spark.sparkContext.defaultParallelism == 2,
      s"Use exactly two local workers and one task-failure attempt; found $master.")
    require(Runtime.getRuntime.maxMemory() <= 1024L * 1024 * 1024, "Test heap exceeds 1 GiB.")
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
      "repetitions" -> repetitions, "settings" -> settings, "master" -> master,
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
        val snapshot = DeltaLog.forTable(spark, base.getCanonicalPath).update()
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

/** Test-only instrumentation; the normal checksum, buffering and Parquet readers are retained. */
class MergeSmokeFileSystem extends DebugFilesystem {
  fs = new MergeSmokeRawFileSystem
}

class MergeSmokeRawFileSystem extends RawLocalFileSystem {
  override def open(handle: PathHandle, bufferSize: Int): FSDataInputStream = {
    MergeSmokeReads.uncovered("path-handle open")
    super.open(handle, bufferSize)
  }

  override def open(path: HadoopPath, bufferSize: Int): FSDataInputStream = {
    val original = super.open(path, bufferSize)
    val id = try {
      MergeSmokeReads.opened(pathToFile(path).getCanonicalPath, original)
    } catch {
      case NonFatal(error) =>
        Utils.tryWithSafeFinally { throw error } { original.close() }
    }
    new FSDataInputStream(original.getWrappedStream) {
      private val closed = new AtomicBoolean()

      override def getWrappedStream: InputStream = {
        MergeSmokeReads.uncovered(id, "wrapped-stream exposure")
        super.getWrappedStream
      }

      override def getFileDescriptor: FileDescriptor = {
        MergeSmokeReads.uncovered(id, "file descriptor exposure")
        super.getFileDescriptor
      }

      override def readVectored(
          ranges: java.util.List[_ <: FileRange],
          allocate: IntFunction[ByteBuffer]): Unit = {
        super.readVectored(MergeSmokeReads.observeVectors(id, ranges), allocate)
      }

      override def readVectored(
          ranges: java.util.List[_ <: FileRange],
          allocate: IntFunction[ByteBuffer],
          release: Consumer[ByteBuffer]): Unit = {
        super.readVectored(MergeSmokeReads.observeVectors(id, ranges), allocate, release)
      }

      override def close(): Unit = {
        if (closed.compareAndSet(false, true)) {
          Utils.tryWithSafeFinally { original.close() } { MergeSmokeReads.closed(id) }
        }
      }
    }
  }
}

private[merge] object MergeSmokeReads {
  private case class Witness(
      path: String,
      statistics: IOStatistics,
      implementation: String,
      initialBytes: Long)
  private case class VectorRead(
      streamId: Long,
      path: String,
      length: Int,
      synchronousBytesBefore: Long,
      var completedBytes: Option[Long] = None)
  private var nextId = 0L
  private var nextVectorId = 0L
  private val open = mutable.Set.empty[Long]
  private val witnesses = mutable.Map.empty[Long, Witness]
  private val vectors = mutable.Map.empty[Long, VectorRead]
  private val uncoveredApis = mutable.Set.empty[String]
  private var watched = Set.empty[String]

  def opened(path: String, stream: FSDataInputStream): Long = synchronized {
    val statistics = if (watched.contains(path)) {
      val result = stream.getIOStatistics
      require(result != null && result.counters().containsKey("stream_read_bytes"),
        "The actual raw reader does not provide independent byte counters.")
      require(stream.getWrappedStream.getClass.getName ==
        "org.apache.hadoop.fs.BufferedFSInputStream", "Unexpected raw reader implementation.")
      require(result.counters().get("stream_read_bytes").longValue() == 0L,
        "A target stream was read before its witness was installed.")
      require(witnesses.size < 128, "Too many streams for the tiny reader-coverage smoke.")
      Some(result)
    } else None
    nextId += 1
    open += nextId
    statistics.foreach { value =>
      witnesses(nextId) = Witness(path, value, stream.getWrappedStream.getClass.getName,
        value.counters().get("stream_read_bytes").longValue())
    }
    nextId
  }

  def closed(id: Long): Unit = synchronized { open -= id }

  def uncovered(id: Long, api: String): Unit = synchronized {
    if (witnesses.contains(id)) uncoveredApis += api
  }

  def uncovered(api: String): Unit = synchronized {
    if (watched.nonEmpty) uncoveredApis += api
  }

  def observeVectors(
      streamId: Long,
      ranges: java.util.List[_ <: FileRange]): java.util.List[_ <: FileRange] = synchronized {
    witnesses.get(streamId) match {
      case None => ranges
      case Some(witness) =>
        require(ranges.size() <= 1024 - vectors.size, "Vectored-read witness budget exceeded.")
        ranges.asScala.map { range =>
          require(range.getOffset >= 0 && range.getLength >= 0, "Invalid raw read range.")
          nextVectorId += 1
          val vectorId = nextVectorId
          vectors(vectorId) = VectorRead(streamId, witness.path, range.getLength,
            witness.statistics.counters().get("stream_read_bytes").longValue())
          new FileRange {
            private var nativeFuture: CompletableFuture[ByteBuffer] = null

            override def getOffset: Long = range.getOffset
            override def getLength: Int = range.getLength
            override def getReference: AnyRef = range.getReference
            override def getData: CompletableFuture[ByteBuffer] = nativeFuture

            override def setData(data: CompletableFuture[ByteBuffer]): Unit = {
              require(data != null && nativeFuture == null, "Read future assigned more than once.")
              nativeFuture = data
              // The backend completes its own future. Its caller sees the same buffer only
              // after accounting, before consumer code can advance the buffer's position.
              range.setData(data.thenApply[ByteBuffer] { (buffer: ByteBuffer) =>
                completeVector(vectorId, buffer)
                buffer
              })
            }
          }: FileRange
        }.asJava
    }
  }

  private def completeVector(id: Long, buffer: ByteBuffer): Unit = synchronized {
    val read = vectors.getOrElse(id,
      throw new IllegalStateException("A vector read completed outside its capture."))
    require(buffer != null && buffer.position() == 0 && buffer.remaining() == read.length,
      "The raw vectored read did not complete with the full requested range.")
    require(read.completedBytes.isEmpty, "A vector range completed more than once.")
    val overlapping = witnesses(read.streamId).statistics.counters().get("stream_read_bytes")
      .longValue() != read.synchronousBytesBefore
    if (overlapping) uncoveredApis += "overlapping-synchronous-accounting"
    require(!overlapping,
      "Vectored reads overlapped synchronous accounting; adding both would double-count.")
    val actualBytes = buffer.remaining().toLong
    MergeSmokeAttribution.emitVector(read.path, id, actualBytes)
    read.completedBytes = Some(actualBytes)
  }

  def begin(paths: Set[String]): Unit = synchronized {
    require(watched.isEmpty && open.isEmpty, "Reader capture starts with live streams.")
    require(paths.nonEmpty && paths.size <= 4, "Unexpected target file count.")
    watched = paths
    witnesses.clear()
    vectors.clear()
    uncoveredApis.clear()
  }

  def snapshot(): MergeSmokeAttribution.Independent = synchronized {
    val problems = mutable.ArrayBuffer.empty[String]
    if (open.nonEmpty) problems += "unclosed-raw-streams"
    problems ++= uncoveredApis.toSeq.sorted
    val streams = witnesses.toVector.sortBy(_._1).map { case (id, witness) =>
      val count = witness.statistics.counters().get("stream_read_bytes").longValue()
      if (count < 0) problems += "negative-raw-reader-counter"
      MergeSmokeAttribution.Stream(id, witness.path, count, !open.contains(id),
        witness.implementation, witness.initialBytes)
    }
    val pending = vectors.collect { case (id, read) if read.completedBytes.isEmpty => id }.toSet
    if (pending.nonEmpty) problems += "pending-failed-or-cancelled-vector"
    val completed = vectors.toVector.collect {
      case (id, read) if read.completedBytes.isDefined =>
        id -> (read.path, read.completedBytes.get)
    }.toMap
    def total(reads: Seq[(String, Long)]): Map[String, Long] = {
      reads.groupMapReduce(_._1)(_._2) { (left, right) =>
        Math.addExact(left, right)
      }
        .filter(_._2 != 0L)
    }
    MergeSmokeAttribution.Independent(
      total(streams.map(stream => stream.path -> stream.bytes)),
      total(completed.values.toVector), completed, pending, streams, open.size,
      problems.distinct.toVector)
  }

  def resetCapture(): Unit = synchronized {
    watched = Set.empty
    witnesses.clear()
    vectors.clear()
    uncoveredApis.clear()
  }

  def openCount: Int = synchronized { open.size }
}

/** Bounded JFR decoding and fail-closed attribution, shared by the real and synthetic checks. */
private[merge] object MergeSmokeAttribution {
  val vectorEventName: String = "delta.smoke.CompletedVectorRead"
  private val descriptorEventName = "delta.smoke.DescriptorReadControl"
  private val maxEvents = 20000
  private val maxWitnesses = 8
  private val maxFrames = 12
  private val descriptorBytes = 64L
  private lazy val vectorEvents = EventFactory.create(
    java.util.Arrays.asList(new AnnotationElement(classOf[Name], vectorEventName)),
    java.util.Arrays.asList(new ValueDescriptor(classOf[String], "path"),
      new ValueDescriptor(java.lang.Long.TYPE, "rangeId"),
      new ValueDescriptor(java.lang.Long.TYPE, "bytesRead")))
  private lazy val descriptorEvents = EventFactory.create(
    java.util.Arrays.asList(new AnnotationElement(classOf[Name], descriptorEventName)),
    java.util.Arrays.asList(new ValueDescriptor(java.lang.Long.TYPE, "bytesRead")))

  case class Stream(
      id: Long,
      path: String,
      bytes: Long,
      closed: Boolean,
      implementation: String,
      initialBytes: Long)
  case class Independent(
      synchronous: Map[String, Long],
      vectored: Map[String, Long],
      completed: Map[Long, (String, Long)],
      pending: Set[Long],
      streams: Vector[Stream],
      openStreams: Int,
      problems: Vector[String])
  case class ThreadInfo(id: Long, name: Option[String], nameRedacted: Boolean)
  case class ReadWitness(
      pathState: String,
      bytes: Long,
      start: Instant,
      end: Instant,
      thread: Option[ThreadInfo],
      frames: Vector[String],
      stackAvailable: Boolean,
      stackTruncated: Boolean,
      framesOmitted: Int,
      endOfFile: Boolean) {
    def json: Map[String, Any] = Map(
      "eventType" -> "jdk.FileRead", "pathState" -> pathState, "bytesRead" -> bytes,
      "startTime" -> start.toString, "endTime" -> end.toString,
      "threadId" -> thread.map(_.id), "threadName" -> thread.flatMap(_.name),
      "threadNameRedacted" -> thread.exists(_.nameRedacted),
      "stackAvailable" -> stackAvailable, "stackTruncated" -> stackTruncated,
      "stackFrames" -> frames, "stackFramesOmitted" -> framesOmitted,
      "endOfFile" -> endOfFile, "eventSizeAvailable" -> false)
  }
  case class DescriptorSpan(start: Instant, end: Instant, threadId: Option[Long], bytes: Long)
  case class Capture(
      synchronous: Map[String, Long],
      vectored: Map[String, Long],
      vectorEvents: Map[Long, (String, Long)],
      firstTargetEvent: Option[(String, Long)],
      witnesses: Vector[ReadWitness],
      pathlessEvents: Long,
      pathlessBytes: Long,
      nullPaths: Long,
      emptyPaths: Long,
      descriptorSpans: Vector[DescriptorSpan],
      markerBytes: Long,
      otherNamedEvents: Long,
      otherNamedBytes: Long,
      dataLossEvents: Int,
      eventsRead: Int,
      complete: Boolean,
      problems: Vector[String]) {
    def descriptorWitnesses: Vector[ReadWitness] = witnesses.filter { read =>
      read.pathState == "null" && read.bytes == descriptorBytes &&
        read.frames.exists(_.startsWith(
          "org.apache.spark.sql.delta.commands.merge.MergeSmokeAttribution$" +
            "#readDescriptorControl:")) &&
        descriptorSpans.exists { span =>
          span.bytes == descriptorBytes && span.threadId.isDefined &&
            span.threadId == read.thread.map(_.id) &&
            !read.start.isBefore(span.start) && !read.end.isAfter(span.end)
        }
    }
    def unattributedEvents: Long = pathlessEvents - descriptorWitnesses.size
  }

  def enable(recording: Recording): Unit = {
    recording.enable("jdk.FileRead").withThreshold(Duration.ZERO).withStackTrace()
    recording.enable("jdk.DataLoss")
    recording.enable(vectorEvents.getEventType.getName)
      .withThreshold(Duration.ZERO).withoutStackTrace()
    recording.enable(descriptorEvents.getEventType.getName)
      .withThreshold(Duration.ZERO).withoutStackTrace()
  }

  def emitVector(path: String, id: Long, bytes: Long): Unit = {
    val event = vectorEvents.newEvent()
    require(event.shouldCommit(), "The completed-vector JFR event is not being recorded.")
    event.set(0, path)
    event.set(1, java.lang.Long.valueOf(id))
    event.set(2, java.lang.Long.valueOf(bytes))
    event.commit()
  }

  def readDescriptorControl(marker: Path): Unit = {
    val owner = new FileInputStream(marker.toFile)
    try {
      val descriptor = new FileInputStream(owner.getFD)
      try {
        val event = descriptorEvents.newEvent()
        require(event.shouldCommit(), "Descriptor control recording is unavailable.")
        event.begin()
        val count = descriptor.read(new Array[Byte](descriptorBytes.toInt))
        event.end()
        event.set(0, java.lang.Long.valueOf(count.toLong))
        event.commit()
        require(count == descriptorBytes, "Descriptor control read did not complete.")
      } finally {
        descriptor.close()
      }
    } finally {
      owner.close()
    }
  }

  private def witness(event: RecordedEvent, pathState: String, bytes: Long): ReadWitness = {
    val thread = Option(event.getThread).map { value =>
      val name = Option(value.getJavaName)
      val safe = name.filter(_.matches("[A-Za-z0-9 _.,()#\\[\\]-]{1,160}"))
      ThreadInfo(value.getJavaThreadId, safe, name.isDefined && safe.isEmpty)
    }
    val stack = Option(event.getStackTrace)
    val frames = mutable.ArrayBuffer.empty[String]
    stack.foreach { value =>
      val iterator = value.getFrames.iterator()
      while (iterator.hasNext && frames.size < maxFrames) {
        val frame = iterator.next()
        val method = frame.getMethod
        val className = method.getType.getName
        val methodName = method.getName
        val safe = className.matches("[A-Za-z0-9_.$]+") &&
          methodName.matches("[A-Za-z0-9_$<>]+")
        frames += (if (safe) {
          s"${className.take(128)}#${methodName.take(48)}:${frame.getLineNumber}"
        } else {
          "[redacted-frame]"
        })
      }
    }
    ReadWitness(pathState, bytes, event.getStartTime, event.getEndTime, thread,
      frames.toVector, stack.isDefined, stack.exists(_.isTruncated),
      stack.map(_.getFrames.size() - frames.size).getOrElse(0), event.getBoolean("endOfFile"))
  }

  def read(recording: Path, paths: Set[String], marker: Path): Capture = {
    val synchronous = mutable.Map.empty[String, Long].withDefaultValue(0L)
    val vectored = mutable.Map.empty[String, Long].withDefaultValue(0L)
    val vectors = mutable.Map.empty[Long, (String, Long)]
    val witnesses = mutable.ArrayBuffer.empty[ReadWitness]
    val spans = mutable.ArrayBuffer.empty[DescriptorSpan]
    val problems = mutable.Set.empty[String]
    var firstTargetEvent: Option[(String, Long)] = None
    var pathlessEvents = 0L
    var pathlessBytes = 0L
    var nullPaths = 0L
    var emptyPaths = 0L
    var markerBytes = 0L
    var otherNamedEvents = 0L
    var otherNamedBytes = 0L
    var dataLossEvents = 0
    var eventsRead = 0
    var complete = false
    def add(left: Long, right: Long): Long = {
      try {
        Math.addExact(left, right)
      } catch {
        case _: ArithmeticException =>
          problems += "counter-overflow"
          left
      }
    }
    val input = new RecordingFile(recording)
    try {
      while (input.hasMoreEvents && eventsRead < maxEvents) {
        val event = input.readEvent()
        eventsRead += 1
        event.getEventType.getName match {
          case "jdk.DataLoss" => dataLossEvents += 1
          case "jdk.FileRead" =>
            val bytes = event.getLong("bytesRead")
            val rawPath = event.getString("path")
            if (bytes < 0L) problems += "negative-native-read-bytes"
            if (rawPath == null || rawPath.isEmpty) {
              pathlessEvents += 1
              pathlessBytes = add(pathlessBytes, bytes)
              val state = if (rawPath == null) "null" else "empty"
              if (rawPath == null) nullPaths += 1 else emptyPaths += 1
              if (witnesses.size < maxWitnesses) {
                witnesses += witness(event, state, bytes)
              } else {
                problems += "unattributed-witness-limit"
              }
            } else {
              val path = new File(rawPath).getCanonicalPath
              if (paths.contains(path)) {
                synchronous(path) = add(synchronous(path), bytes)
                if (bytes > 0 && firstTargetEvent.isEmpty) firstTargetEvent = Some(path -> bytes)
              } else if (path == marker.toFile.getCanonicalPath) {
                markerBytes = add(markerBytes, bytes)
              } else {
                otherNamedEvents += 1
                otherNamedBytes = add(otherNamedBytes, bytes)
              }
            }
          case name if name == vectorEventName =>
            val bytes = event.getLong("bytesRead")
            val rawPath = event.getString("path")
            val id = event.getLong("rangeId")
            if (rawPath == null || rawPath.isEmpty || bytes < 0L) {
              problems += "invalid-vector-event"
            } else {
              val path = new File(rawPath).getCanonicalPath
              if (!paths.contains(path)) problems += "vector-outside-target-domain"
              if (vectors.contains(id)) problems += "duplicate-vector-event"
              if (vectors.size >= 1024) {
                problems += "vector-event-limit"
              } else {
                vectors(id) = path -> bytes
                vectored(path) = add(vectored(path), bytes)
              }
            }
          case name if name == descriptorEventName =>
            if (spans.size < 2) {
              spans += DescriptorSpan(event.getStartTime, event.getEndTime,
                Option(event.getThread).map(_.getJavaThreadId), event.getLong("bytesRead"))
            } else {
              problems += "descriptor-control-limit"
            }
          case _ => problems += "unexpected-recorded-event-type"
        }
      }
      complete = !input.hasMoreEvents
      if (!complete) problems += "event-count-limit"
    } catch {
      case NonFatal(error) => problems += s"recording-decode-${error.getClass.getSimpleName}"
    } finally {
      input.close()
    }
    Capture(synchronous.toMap.filter(_._2 != 0L), vectored.toMap.filter(_._2 != 0L),
      vectors.toMap, firstTargetEvent, witnesses.toVector, pathlessEvents, pathlessBytes,
      nullPaths, emptyPaths, spans.toVector, markerBytes, otherNamedEvents, otherNamedBytes,
      dataLossEvents, eventsRead, complete, problems.toVector.sorted)
  }

  def problems(capture: Capture, independent: Independent, paths: Set[String]): Vector[String] = {
    val issues = mutable.ArrayBuffer.empty[String]
    issues ++= capture.problems
    issues ++= independent.problems
    if (!capture.complete) issues += "incomplete-recording-scan"
    if (capture.dataLossEvents != 0) issues += "jfr-data-loss"
    if (capture.markerBytes != 128L) issues += "named-negative-control-mismatch"
    if (capture.descriptorSpans.size != 1 || capture.descriptorWitnesses.size != 1) {
      issues += "descriptor-control-not-proven"
    }
    if (capture.unattributedEvents != 0L) issues += "unattributed-global-file-read"
    if (capture.synchronous != independent.synchronous) issues += "per-file-synchronous-mismatch"
    if (capture.vectored != independent.vectored) issues += "per-file-vector-mismatch"
    if (capture.vectorEvents != independent.completed) issues += "vector-id-or-buffer-mismatch"
    if (independent.openStreams != 0 || independent.streams.exists(!_.closed)) {
      issues += "unclosed-stream-witness"
    }
    if (independent.pending.nonEmpty) issues += "pending-failed-or-cancelled-vector"
    if (independent.streams.isEmpty || independent.streams.exists { stream =>
        !paths.contains(stream.path) || stream.bytes < 0 || stream.initialBytes != 0 ||
          stream.implementation != "org.apache.hadoop.fs.BufferedFSInputStream"
      } || (capture.synchronous.keySet ++ capture.vectored.keySet).exists { path =>
        !independent.streams.exists(_.path == path)
      }) {
      issues += "incomplete-target-stream-domain"
    }
    if (independent.synchronous.isEmpty && independent.vectored.isEmpty) {
      issues += "missing-positive-target-counters"
    }
    issues.distinct.toVector
  }

  def evidence(
      label: String,
      capture: Capture,
      independent: Independent,
      paths: Set[String],
      recordingBytes: Long,
      operationReturned: Boolean,
      failures: Vector[String]): Map[String, Any] = {
    val issues = (problems(capture, independent, paths) ++ failures).distinct
    val indices = paths.toVector.sorted.zipWithIndex.toMap
    val perFile = indices.toVector.sortBy(_._2).map { case (path, index) =>
      Map("fileIndex" -> index, "jdkFileReadBytes" -> capture.synchronous.getOrElse(path, 0L),
        "hadoopSynchronousBytes" -> independent.synchronous.getOrElse(path, 0L),
        "vectorCompletionEventBytes" -> capture.vectored.getOrElse(path, 0L),
        "completedRawVectorBytes" -> independent.vectored.getOrElse(path, 0L),
        "streamWitnesses" -> independent.streams.count(_.path == path))
    }
    def total(values: Iterable[Long]): Long =
      values.foldLeft(0L)((left, right) => Math.addExact(left, right))
    val nativeBytes = total(capture.synchronous.values)
    val vectorBytes = total(capture.vectored.values)
    def vectorRows(values: Map[Long, (String, Long)]): Vector[Map[String, Any]] = {
      values.toVector.sortBy(_._1).map { case (id, (path, bytes)) =>
        Map("rangeId" -> id, "fileIndex" -> indices.get(path), "bytesRead" -> bytes)
      }
    }
    Map(
      "schemaVersion" -> 1, "phase" -> label,
      "evidenceFile" -> s"reader-$label.json", "operationReturned" -> operationReturned,
      "diagnosticCaptureComplete" -> capture.complete,
      "targetCoverageStatus" -> (if (issues.isEmpty) "verified" else "inconclusive"),
      "mergeCorrectnessStatus" -> "not-evaluated-by-reader-capture",
      "covered" -> issues.isEmpty, "problems" -> issues,
      "dataLossEvents" -> capture.dataLossEvents, "recordedEventsScanned" -> capture.eventsRead,
      "rollingRetention" -> false, "openRawStreams" -> independent.openStreams,
      "targetFilesRead" -> paths.count { path =>
        capture.synchronous.getOrElse(path, 0L) > 0 || capture.vectored.getOrElse(path, 0L) > 0
      },
      "targetReaderBytes" -> Math.addExact(nativeBytes, vectorBytes),
      "independentRawReaderBytes" ->
        Math.addExact(total(independent.synchronous.values), total(independent.vectored.values)),
      "jdkFileReadBytes" -> nativeBytes, "vectorCompletionEventBytes" -> vectorBytes,
      "completedVectorRanges" -> capture.vectorEvents.size,
      "vectorCompletionIdsVerified" -> (capture.vectorEvents == independent.completed),
      "pendingVectorRanges" -> independent.pending.size,
      "pendingFailedOrCancelledVectorIds" -> independent.pending.toVector.sorted,
      "overlappedSynchronousAccounting" ->
        independent.problems.contains("overlapping-synchronous-accounting"),
      "vectorAccounting" -> "Successful raw FileRange buffers, not submitted/logical ranges",
      "vectorEventType" -> vectorEventName, "perFileBytes" -> perFile,
      "targetStreamWitnesses" -> independent.streams.map { stream =>
        Map("streamId" -> stream.id, "fileIndex" -> indices.get(stream.path),
          "implementation" -> stream.implementation, "initialBytes" -> stream.initialBytes,
          "finalSynchronousBytes" -> stream.bytes, "closed" -> stream.closed)
      },
      "completedRawVectors" -> vectorRows(independent.completed),
      "recordedVectorEvents" -> vectorRows(capture.vectorEvents),
      "independentProblems" -> independent.problems,
      "negativeControlBytes" -> capture.markerBytes,
      "negativeControlExcludedFromTarget" -> (capture.markerBytes == 128L),
      "pathlessReadEvents" -> capture.pathlessEvents, "pathlessReadBytes" -> capture.pathlessBytes,
      "nullPathEvents" -> capture.nullPaths, "emptyPathEvents" -> capture.emptyPaths,
      "knownDescriptorControlEvents" -> capture.descriptorWitnesses.size,
      "knownDescriptorControlBytes" -> total(capture.descriptorWitnesses.map(_.bytes)),
      "descriptorControlSpans" -> capture.descriptorSpans.map { span =>
        Map("startTime" -> span.start.toString, "endTime" -> span.end.toString,
          "threadId" -> span.threadId, "bytesRead" -> span.bytes)
      },
      "unattributedReadEvents" -> capture.unattributedEvents,
      "unattributedReadBytes" ->
        Math.subtractExact(capture.pathlessBytes, total(capture.descriptorWitnesses.map(_.bytes))),
      "pathlessReadWitnesses" -> capture.witnesses.map { read =>
        read.json + ("attribution" -> (if (capture.descriptorWitnesses.contains(read)) {
          "known-non-target-descriptor-control"
        } else {
          "unattributed"
        }))
      },
      "witnessesOmitted" -> (capture.pathlessEvents - capture.witnesses.size),
      "witnessLimit" -> maxWitnesses, "stackFrameLimit" -> maxFrames,
      "otherNamedReadEvents" -> capture.otherNamedEvents,
      "otherNamedReadBytes" -> capture.otherNamedBytes,
      "recordingBytes" -> recordingBytes, "rawRecordingRetained" -> false,
      "rawRecordingOmissionReason" ->
        ("Global paths and constant-pool/thread metadata are not export-approved; " +
          "sanitized selected events and counter maps are persisted before dump cleanup."),
      "reproduction" -> "New recording; the lost run6 event cannot be identified.",
      "scope" -> "Actual local Parquet reads; not physical-disk or network bytes")
  }

  def negativeChecks(
      capture: Capture,
      independent: Independent,
      paths: Set[String]): Map[String, Any] = {
    require(problems(capture, independent, paths).isEmpty,
      "Negative controls need a fully reconciled positive fixture.")
    val (path, bytes) = capture.firstTargetEvent.getOrElse {
      throw new IllegalStateException("No native target event to remove.")
    }
    val missingSync = capture.copy(synchronous =
      capture.synchronous.updated(path, capture.synchronous(path) - bytes).filter(_._2 != 0L))
    val (id, (vectorPath, vectorBytes)) = capture.vectorEvents.toVector.sortBy(_._1).headOption
      .getOrElse(throw new IllegalStateException("No vector event to remove."))
    val missingVector = capture.copy(vectored = capture.vectored
      .updated(vectorPath, capture.vectored(vectorPath) - vectorBytes).filter(_._2 != 0L),
      vectorEvents = capture.vectorEvents - id)
    val syncRejected = problems(missingSync, independent, paths)
      .contains("per-file-synchronous-mismatch")
    val vectorRejected = problems(missingVector, independent, paths)
      .contains("vector-id-or-buffer-mismatch")
    val emptyPath = capture.copy(witnesses =
      capture.witnesses.map(_.copy(pathState = "empty")))
    val emptyRejected = problems(emptyPath, independent, paths)
      .contains("unattributed-global-file-read")
    val unclassified = capture.copy(
      witnesses = capture.witnesses.map(_.copy(frames = Vector.empty)))
    val unknownRejected = problems(unclassified, independent, paths)
      .contains("unattributed-global-file-read")
    val lossRejected = problems(capture.copy(dataLossEvents = 1), independent, paths)
      .contains("jfr-data-loss")
    val escapeRejected = problems(capture,
      independent.copy(problems = Vector("wrapped-stream exposure")), paths)
      .contains("wrapped-stream exposure")
    require(syncRejected && vectorRejected && capture.pathlessEvents > 0 &&
      emptyRejected && unknownRejected && lossRejected && escapeRejected,
      "Invalid coverage must fail even with descriptor events present.")
    Map("synthetic" -> true, "syntheticNegativeChecks" -> true,
      "hadoopCountersSynthetic" -> false, "mutationOfObservedEvents" -> true,
      "missingTargetSyncEventRejected" -> syncRejected,
      "missingVectorEventRejected" -> vectorRejected,
      "nativeDescriptorControlObserved" -> (capture.descriptorWitnesses.size == 1),
      "emptyPathRejected" -> emptyRejected, "unattributedNullPathRejected" -> unknownRejected,
      "dataLossRejected" -> lossRejected, "streamEscapeRejected" -> escapeRejected,
      "pathlessEventsPreservedInBothMutations" -> capture.pathlessEvents)
  }

  def verifyControls(directory: Path, observe: Recording => Unit): Map[String, Any] = {
    Files.createDirectory(directory)
    val target = directory.resolve("target.bin")
    val marker = directory.resolve("marker.bin")
    val destination = directory.resolve("controls.jfr")
    Files.write(target, new Array[Byte](128))
    Files.write(marker, new Array[Byte](128))
    val path = target.toFile.getCanonicalPath
    val recorder = new Recording()
    observe(recorder)
    try {
      enable(recorder)
      recorder.setMaxSize(0L)
      recorder.start()
      val stream = new FileInputStream(target.toFile)
      try {
        require(stream.read(new Array[Byte](64)) == 64, "Synthetic parser read failed.")
      } finally {
        stream.close()
      }
      // Only this parser fixture supplies synthetic vector/counter values.
      emitVector(path, 1L, 16L)
      val unrelated = new FileInputStream(marker.toFile)
      try {
        require(unrelated.read(new Array[Byte](128)) == 128)
      } finally {
        unrelated.close()
      }
      readDescriptorControl(marker)
      recorder.stop()
      recorder.dump(destination)
      require(Files.size(destination) <= 2L * 1024 * 1024, "Parser fixture recording too large.")
      val capture = read(destination, Set(path), marker)
      val independent = Independent(Map(path -> 64L), Map(path -> 16L),
        Map(1L -> (path -> 16L)), Set.empty,
        Vector(Stream(1L, path, 64L, true, "org.apache.hadoop.fs.BufferedFSInputStream", 0L)),
        0, Vector.empty)
      val negatives = negativeChecks(capture, independent, Set(path))
      negatives ++ Map("synthetic" -> true, "hadoopCountersSynthetic" -> true,
        "controlWitness" -> capture.descriptorWitnesses.head.json)
    } finally {
      recorder.close()
      observe(null)
      Files.deleteIfExists(destination)
      Files.deleteIfExists(target)
      Files.deleteIfExists(marker)
      Files.deleteIfExists(directory)
    }
  }
}

/** A resource/serialization/reader smoke, never a timing or automatic-pruning experiment. */
private[merge] object MergeSourcePruningSmoke {
  private val maxTemporaryBytes = 16L * 1024 * 1024
  private val maxRecordingBytes = 2L * 1024 * 1024
  private val targetRows = 256L
  private val sourceRows = 16L

  private def treeBytes(root: Path): Long = {
    var bytes = 0L
    var count = 0
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def visitFile(path: Path, attrs: BasicFileAttributes): FileVisitResult = {
        require(!attrs.isSymbolicLink, "Symlinks are not allowed in smoke scratch data.")
        count += 1
        require(count <= 1024, "Smoke scratch file count exceeded.")
        bytes = Math.addExact(bytes, attrs.size())
        FileVisitResult.CONTINUE
      }

      override def visitFileFailed(path: Path, error: java.io.IOException): FileVisitResult = {
        error match {
          case _: NoSuchFileException => FileVisitResult.CONTINUE // Concurrent task cleanup.
          case other => throw other
        }
      }

      override def postVisitDirectory(
          path: Path, error: java.io.IOException): FileVisitResult = {
        if (error == null) FileVisitResult.CONTINUE else visitFileFailed(path, error)
      }
    })
    bytes
  }

  private class Budget(spark: SparkSession, root: Path, output: Path) extends AutoCloseable {
    private val started = System.nanoTime()
    private val failure = new AtomicReference[Throwable]()
    val activeRecording = new AtomicReference[Recording]()
    private var peak = 0L
    private val monitor = Executors.newSingleThreadScheduledExecutor((task: Runnable) => {
      val thread = new Thread(task, "delta-merge-smoke-budget")
      thread.setDaemon(true)
      thread
    })
    private val watchdog = monitor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        try check() catch {
          case NonFatal(error) =>
            failure.compareAndSet(null, error)
            try spark.sparkContext.cancelJobGroup("delta-merge-pruning-smoke") catch {
              case NonFatal(cancelError) => error.addSuppressed(cancelError)
            }
        }
      }
    }, 0L, 100L, TimeUnit.MILLISECONDS)

    def check(): Unit = synchronized {
      Option(failure.get()).foreach(error => throw error)
      require(System.nanoTime() - started <= TimeUnit.MINUTES.toNanos(3),
        "The three-minute smoke budget was exhausted.")
      val bytes = Math.addExact(treeBytes(root), treeBytes(output))
      peak = math.max(peak, bytes)
      require(bytes <= maxTemporaryBytes, "Smoke scratch data exceeded 16 MiB.")
      require(root.toFile.getUsableSpace >= 2L * 1024 * 1024 * 1024,
        "Insufficient remaining disk space.")
      Option(activeRecording.get()).foreach { recording =>
        require(recording.getSize <= maxRecordingBytes, "JFR resource budget exceeded.")
      }
    }

    def peakBytes: Long = synchronized { peak }

    override def close(): Unit = {
      watchdog.cancel(false)
      monitor.shutdownNow()
      require(monitor.awaitTermination(5, TimeUnit.SECONDS), "Budget monitor did not stop.")
      check()
    }
  }

  private def atomicJson(path: Path, value: Map[String, Any]): Unit = {
    val bytes = (JsonUtils.toJson(value) + "\n").getBytes(UTF_8)
    require(bytes.length <= 128 * 1024, "Smoke JSON exceeded 128 KiB.")
    val temporary = Files.createTempFile(path.getParent, ".smoke-", ".tmp")
    Utils.tryWithSafeFinally {
      Files.write(temporary, bytes)
      Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING)
    } {
      Files.deleteIfExists(temporary)
    }
  }

  private def readJson(path: Path): com.fasterxml.jackson.databind.JsonNode = {
    require(Files.size(path) <= 128 * 1024, "Smoke JSON is unexpectedly large.")
    JsonUtils.mapper.readTree(Files.readAllBytes(path))
  }

  private def inputs(directory: File, snapshot: Snapshot): Map[String, Long] = {
    val files = snapshot.allFiles.take(5)
    require(files.nonEmpty && files.length <= 4, "Smoke target exceeds four files.")
    files.map { file =>
      val uri = new java.net.URI(file.path)
      require(uri.getScheme == null || uri.getScheme == "file", "Non-local smoke file.")
      val local = if (uri.getScheme == "file") new File(uri) else {
        val path = new File(uri.getPath)
        if (path.isAbsolute) path else new File(directory, uri.getPath)
      }
      local.getCanonicalPath -> file.size
    }.toMap
  }

  private def coverage[T](
      label: String,
      paths: Set[String],
      marker: Path,
      root: Path,
      output: Path,
      budget: Budget,
      observed: Map[String, Any] => Unit,
      retainDump: Path => Unit)(operation: => T): (T, Map[String, Any]) = {
    require(FlightRecorder.getFlightRecorder.getRecordings.isEmpty,
      "An unrelated recording would make resource accounting ambiguous.")
    require(FlightRecorder.getFlightRecorder.getEventTypes.asScala
      .exists(_.getName == "jdk.DataLoss"), "JFR data-loss detection is unavailable.")
    val recorder = new Recording()
    val destination = root.resolve(s"$label.jfr")
    val evidenceFile = output.resolve(s"reader-$label.json")
    val failures = mutable.ArrayBuffer.empty[Throwable]
    var result: Option[T] = None
    var independent: Option[MergeSmokeAttribution.Independent] = None
    var capture: Option[MergeSmokeAttribution.Capture] = None
    var recordingBytes: Option[Long] = None
    var negativeChecks: Option[Map[String, Any]] = None
    var persisted = false
    def attempt(operation: => Unit): Unit = {
      try operation catch { case NonFatal(error) => failures += error }
    }
    budget.activeRecording.set(recorder)
    Utils.tryWithSafeFinally {
      attempt {
        MergeSmokeAttribution.enable(recorder)
        recorder.setMaxSize(0L) // Rolling retention would silently discard earlier reads.
        require(recorder.getMaxAge == null, "JFR must not discard events by age.")
        MergeSmokeReads.begin(paths)
        recorder.start()
        result = Some(operation)
        val negative = new FileInputStream(marker.toFile)
        Utils.tryWithSafeFinally {
          require(negative.read(new Array[Byte](128)) == 128, "Negative control read failed.")
        } {
          negative.close()
        }
        MergeSmokeAttribution.readDescriptorControl(marker)
        budget.check()
        require(recorder.getState == RecordingState.RUNNING, "Recording stopped prematurely.")
      }
      if (recorder.getState == RecordingState.RUNNING) attempt { recorder.stop() }
      attempt { independent = Some(MergeSmokeReads.snapshot()) }
      attempt {
        require(recorder.getState == RecordingState.STOPPED, "Recording was not completed.")
        recorder.dump(destination)
        recordingBytes = Some(Files.size(destination))
        require(recordingBytes.get <= maxRecordingBytes, "JFR dump budget exceeded.")
        capture = Some(MergeSmokeAttribution.read(destination, paths, marker))
        budget.check()
      }
      if (label == "actual-parquet-scan" && failures.isEmpty) {
        for (events <- capture; counts <- independent
            if MergeSmokeAttribution.problems(events, counts, paths).isEmpty) {
          attempt {
            negativeChecks = Some(MergeSmokeAttribution.negativeChecks(events, counts, paths))
          }
        }
      }
      val errors = failures.map(error => s"capture-stage-${error.getClass.getSimpleName}").toVector
      val evidence = (capture, independent, recordingBytes) match {
        case (Some(events), Some(counts), Some(bytes)) =>
          MergeSmokeAttribution.evidence(label, events, counts, paths, bytes,
            result.isDefined, errors)
        case _ =>
          Map[String, Any]("schemaVersion" -> 1, "phase" -> label,
            "evidenceFile" -> evidenceFile.getFileName.toString,
            "operationReturned" -> result.isDefined, "diagnosticCaptureComplete" -> false,
            "targetCoverageStatus" -> "inconclusive", "covered" -> false,
            "problems" -> (errors :+ "missing-recording-or-counter-capture"),
            "independentCaptured" -> independent.isDefined,
            "independentPerFile" -> independent.map { counts =>
              paths.toVector.sorted.zipWithIndex.map { case (path, index) =>
                Map("fileIndex" -> index,
                  "hadoopSynchronousBytes" -> counts.synchronous.getOrElse(path, 0L),
                  "completedRawVectorBytes" -> counts.vectored.getOrElse(path, 0L))
              }
            },
            "independentProblems" -> independent.map(_.problems),
            "completedRawVectorIds" -> independent.map(_.completed.keys.toVector.sorted),
            "pendingFailedOrCancelledVectorIds" -> independent.map(_.pending.toVector.sorted),
            "rawRecordingRetained" -> false, "recordingBytes" -> recordingBytes,
            "rawRecordingOmissionReason" -> "Capture failed; no raw JVM metadata is exported.")
      }
      val packet = evidence + ("negativeControlChecks" -> negativeChecks)
      atomicJson(evidenceFile, packet)
      require(readJson(evidenceFile).path("phase").asText() == label &&
        readJson(evidenceFile).path("operationReturned").asBoolean() == result.isDefined,
        "The diagnostic evidence packet did not round-trip.")
      persisted = true
      val summary = packet -- Seq("pathlessReadWitnesses", "targetStreamWitnesses",
        "completedRawVectors", "recordedVectorEvents")
      observed(summary)
      failures.headOption.foreach { error =>
        failures.drop(1).foreach(error.addSuppressed)
        throw error
      }
      require(evidence("covered") == true,
        s"Actual target-reader attribution is inconclusive; inspect ${evidenceFile.getFileName}.")
      result.getOrElse {
        throw new IllegalStateException("The measured operation did not return.")
      } -> summary
    } {
      Utils.tryWithSafeFinally {
        recorder.close()
      } {
        budget.activeRecording.set(null)
        MergeSmokeReads.resetCapture()
        if (persisted) {
          Files.deleteIfExists(destination)
        } else if (Files.exists(destination)) {
          retainDump(destination)
        }
      }
    }
  }

  private def sameRows(actual: Array[Row], expected: Seq[Row]): Unit = {
    require(actual.length <= 4096 && expected.length <= 4096, "Smoke row limit exceeded.")
    require(actual.toSeq.groupMapReduce(identity)(_ => 1)(_ + _) ==
      expected.groupMapReduce(identity)(_ => 1)(_ + _), "Smoke result multiset differs.")
  }

  def run(spark: SparkSession, root: Path, output: Path): Unit = {
    require(sys.props.get("delta.mergePruning.profile").contains("smoke"),
      "Only the explicitly selected smoke profile may run.")
    Files.createDirectories(output)
    val destination = output.resolve("smoke.json")
    require(!Files.exists(destination), "Refusing to overwrite a previous smoke result.")
    val phases = mutable.ArrayBuffer.empty[Map[String, Any]]
    val diagnostics = mutable.ArrayBuffer.empty[Map[String, Any]]
    val retainedDumps = mutable.Set.empty[Path]
    val report = mutable.Map[String, Any](
      "schemaVersion" -> 1, "profile" -> "smoke", "status" -> "running",
      "benchmarkExecuted" -> false, "performanceClaim" -> false, "mergesStarted" -> 0,
      "mergesCompleted" -> 0, "diagnosticCaptureComplete" -> false,
      "targetCoverageStatus" -> "not-started", "mergeCorrectnessStatus" -> "not-verified")
    def save(): Unit = atomicJson(destination, report.toMap ++
      Map("phases" -> phases.toVector, "readerDiagnostics" -> diagnostics.toVector))
    def recordObservation(value: Map[String, Any]): Unit = {
      diagnostics += value
      report("diagnosticCaptureComplete") =
        diagnostics.forall(_("diagnosticCaptureComplete") == true)
      report("targetCoverageStatus") = if (diagnostics.forall(_("covered") == true)) {
        "verified-for-captured-phases"
      } else {
        "inconclusive"
      }
      save()
    }
    def retainDump(path: Path): Unit = { retainedDumps += path }
    save()
    val started = System.nanoTime()
    val data = root.resolve("data")
    var budget: Option[Budget] = None
    var ownsData = false
    var failure: Option[Throwable] = None
    def recordFailure(error: Throwable): Unit = {
      failure match {
        case Some(original) if original ne error => original.addSuppressed(error)
        case Some(_) =>
        case None => failure = Some(error)
      }
      report("status") = "failed"
      report("error") = Map("class" -> failure.get.getClass.getName,
        "message" -> Option(failure.get.getMessage).getOrElse("").take(2048))
    }
    def cleanup(operation: => Unit): Unit = {
      try operation catch { case NonFatal(error) => recordFailure(error) }
    }
    try {
      val master = spark.sparkContext.master
      val parallelism = spark.sparkContext.defaultParallelism
      val taskCpus = spark.sparkContext.getConf.getInt("spark.task.cpus", 1)
      val heap = Runtime.getRuntime.maxMemory()
      report("resources") = Map(
        "master" -> master, "defaultParallelism" -> parallelism, "taskCpus" -> taskCpus,
        "maximumHeapBytes" -> heap, "javaVersion" -> sys.props("java.runtime.version"),
        "sparkVersion" -> spark.version,
        "scalaVersion" -> scala.util.Properties.versionNumberString,
        "hadoopVersion" -> org.apache.hadoop.util.VersionInfo.getVersion,
        "heapArguments" -> ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
          .filter(_.startsWith("-Xmx")).toVector,
        "sqlSettings" -> Seq("spark.sql.adaptive.enabled",
          "spark.sql.optimizer.dynamicPartitionPruning.enabled",
          "spark.sql.optimizer.runtime.bloomFilter.enabled",
          "spark.sql.parquet.enableVectorizedReader", "spark.sql.shuffle.partitions",
          "spark.sql.optimizer.excludedRules", "spark.databricks.delta.merge.materializeSource")
          .map(key => key -> spark.conf.getOption(key)).toMap)
      save()
      require(Set("local[2]", "local[2,1]").contains(master) && parallelism == 2 && taskCpus == 1,
        s"Expected two local cores and one task CPU; found $master/$parallelism/$taskCpus.")
      require(heap <= 1024L * 1024 * 1024, "The smoke heap exceeds 1 GiB.")
      require(sys.props("java.specification.version") == "17" && spark.version == "4.2.0" &&
        scala.util.Properties.versionNumberString == "2.13.17" &&
        org.apache.hadoop.util.VersionInfo.getVersion == "3.5.0",
        "Resolved runtime versions changed.")
      require(root.isAbsolute && !Files.exists(data), "Smoke scratch directory is not fresh.")
      require(new File(sys.props("java.io.tmpdir")).getCanonicalFile.toPath.startsWith(root) &&
        new File(spark.sparkContext.getConf.get("spark.local.dir"))
          .getCanonicalFile.toPath.startsWith(root),
        "JVM temporary data and Spark spill must stay inside the monitored smoke directory.")
      Files.createDirectories(data)
      ownsData = true
      val activeBudget = new Budget(spark, root, output)
      budget = Some(activeBudget)
      spark.sparkContext.setJobGroup("delta-merge-pruning-smoke", "Tiny MERGE harness smoke", true)

      val sample = output.resolve("serialization.json")
      atomicJson(sample, Map("value" -> Some(7L), "absent" -> Option.empty[Long],
        "nested" -> Seq(Map("valid" -> true))))
      require(readJson(sample).path("value").asLong() == 7L &&
        readJson(sample).path("nested").get(0).path("valid").asBoolean(),
        "Nested/optional serialization round trip failed.")
      atomicJson(sample, Map("replaced" -> true))
      require(readJson(sample).path("replaced").asBoolean() &&
        !readJson(sample).has("value"), "Atomic replacement did not replace the whole result.")
      atomicJson(output.resolve("synthetic-failure.json"),
        Map("status" -> "failed", "syntheticNegativeCheck" -> true,
          "message" -> "Deliberate failure record; no real operation failed."))
      require(readJson(output.resolve("synthetic-failure.json"))
        .path("status").asText() == "failed")
      report("outputChecks") = Map("roundTrip" -> true, "atomicReplacement" -> true,
        "syntheticFailureRecord" -> true)
      save()
      val base = data.resolve("base").toFile
      spark.range(0, targetRows, 1, 4).toDF("key").withColumn("value", lit(0L))
        .withColumn("payload", sha2(col("key").cast("string"), 256))
        .write.format("delta").option("compression", "snappy")
        .option("delta.enableDeletionVectors", "false").save(base.getCanonicalPath)
      val deltaLog = DeltaLog.forTable(spark, base.getCanonicalPath)
      val snapshot = deltaLog.update()
      val manifest = inputs(base, snapshot)
      require(manifest.size == 4 && manifest.values.sum <= 1024 * 1024,
        "Smoke target exceeds its file/byte budget.")
      val fs = new HadoopPath(base.getCanonicalPath).getFileSystem(deltaLog.newDeltaHadoopConf())
      require(fs.getClass == classOf[MergeSmokeFileSystem],
        "The actual reader bypassed the independent raw-stream accounting filesystem.")
      require(manifest.keys.forall { path =>
        new HadoopPath(path).getFileSystem(deltaLog.newDeltaHadoopConf()).getClass ==
          classOf[MergeSmokeFileSystem]
      }, "An original-snapshot target path bypasses the instrumented filesystem.")
      report("fixture") = Map("targetRows" -> targetRows, "targetFiles" -> manifest.size,
        "targetBytes" -> manifest.values.sum, "sourceRows" -> sourceRows,
        "manifestSha256" -> MessageDigest.getInstance("SHA-256")
          .digest(manifest.toSeq.sorted.mkString("\n").getBytes(UTF_8))
          .map(value => f"${value & 0xff}%02x").mkString)
      val marker = data.resolve("unrelated.bin")
      Files.write(marker, Array.fill[Byte](128)(3))
      val scan = spark.read.format("delta").load(base.getCanonicalPath)
      val formats = scan.queryExecution.executedPlan.collect {
        case fileScan: FileSourceScanExec => fileScan.relation.fileFormat.getClass.getName
      }
      require(formats.exists(_.endsWith("DeltaParquetFileFormat")),
        s"The smoke did not exercise a Delta Parquet file reader: $formats")
      val (beforeRows, scanCoverage) =
        coverage("actual-parquet-scan", manifest.keySet, marker, data, output,
          activeBudget, recordObservation, retainDump) {
          scan.collect()
        }
      require(beforeRows.length == targetRows &&
        scanCoverage("targetFilesRead") == manifest.size, "The full target scan was not covered.")
      report("attributionControlChecks") = scanCoverage("negativeControlChecks")
      phases += scanCoverage + ("readerFormats" -> formats)
      save()

      spark.range(64L, 64L + sourceRows, 1L, 2).selectExpr("id AS key", "1L AS value")
        .createOrReplaceTempView("pruning_smoke_source")
      val expected = beforeRows.toSeq.map { row =>
        val value = if (row.getLong(0) >= 64L && row.getLong(0) < 64L + sourceRows) {
          1L
        } else {
          0L
        }
        Row(row.getLong(0), value, row.getString(2))
      }
      Seq(false, true).foreach { bounded =>
        activeBudget.check()
        val target = data.resolve(s"clone-$bounded").toFile
        spark.sql(s"CREATE TABLE delta.`${target.getCanonicalPath}` SHALLOW CLONE " +
          s"delta.`${base.getCanonicalPath}` VERSION AS OF 0").collect()
        require(inputs(target, DeltaLog.forTable(spark, target.getCanonicalPath).update()) ==
          manifest, "The smoke clone changed the pinned input manifest.")
        val label = if (bounded) "literal-control-merge" else "baseline-merge"
        val condition = if (bounded) " AND t.key >= 64L AND t.key <= 79L" else ""
        val merges = report("mergesStarted").toString.toInt + 1
        require(merges <= 2, "The smoke cannot execute a third MERGE.")
        report("mergesStarted") = merges
        save()
        val ((rows, stats), observed) =
          coverage(label, manifest.keySet, marker, data, output,
            activeBudget, recordObservation, retainDump) {
            MergeSourcePruningBenchmark.execute(spark,
              s"MERGE INTO delta.`${target.getCanonicalPath}` t USING pruning_smoke_source s " +
                s"ON t.key = s.key$condition WHEN MATCHED THEN UPDATE SET value = s.value")
          }
        require(rows.toSeq == Seq(Row(sourceRows, sourceRows, 0L, 0L)) &&
          stats.source.rows.contains(sourceRows) && stats.targetRowsUpdated == sourceRows,
          "The smoke returned incorrect source or affected-row metrics.")
        require(stats.materializeSourceReason
          .contains("materializeNonDeterministicSourceNonDelta") &&
          stats.materializeSourceAttempts.contains(1L),
          "The incoming source was not eagerly materialized once under the existing policy.")
        sameRows(spark.read.format("delta").load(target.getCanonicalPath).collect(), expected)
        require(deltaLog.update().version == 0L, "The pinned base table was modified.")
        val committedVersion = DeltaLog.forTable(spark, target.getCanonicalPath).update().version
        require(committedVersion == 1L && stats.commitVersion.contains(committedVersion),
          "The smoke did not verify the expected clone commit.")
        report("mergesCompleted") = merges
        report("mergeCorrectnessStatus") = if (merges == 2) "verified" else "partially-verified"
        phases += observed ++ Map("dataCorrect" -> true, "metricsCorrect" -> true,
          "commitCorrect" -> true, "commitVersion" -> stats.commitVersion,
          "sourceMaterializationReason" -> stats.materializeSourceReason)
        save()
      }
      activeBudget.check()
      require(MergeSmokeReads.openCount == 0, "Raw reader streams leaked.")
      DebugFilesystem.assertNoOpenStreams()
      report("readerCoverageEstablished") = true
      report("peakTemporaryBytes") = activeBudget.peakBytes
    } catch {
      case NonFatal(error) => recordFailure(error)
    } finally {
      cleanup { budget.foreach(_.close()) }
      budget.foreach(value => report("peakTemporaryBytes") = value.peakBytes)
      cleanup { spark.catalog.dropTempView("pruning_smoke_source") }
      cleanup { spark.sparkContext.clearJobGroup() }
      cleanup { MergeSmokeReads.resetCapture() }
      if (ownsData && retainedDumps.isEmpty) cleanup { Utils.deleteRecursively(data.toFile) }
      if (retainedDumps.nonEmpty) {
        recordFailure(new IllegalStateException(
          "Diagnostic persistence failed; an unpublished raw dump was kept on the runner."))
      }
      report("status") = if (failure.isEmpty) "passed" else "failed"
      report("elapsedMs") = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
      report("openRawStreamsAfterCleanup") = MergeSmokeReads.openCount
      report("scratchDataRemoved") = ownsData && !Files.exists(data)
      cleanup { save() }
    }
    failure.foreach(error => throw error)
  }
}
