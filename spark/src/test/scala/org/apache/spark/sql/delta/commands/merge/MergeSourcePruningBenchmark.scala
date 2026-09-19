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

import java.io.{File, FileDescriptor, FileInputStream, RandomAccessFile}
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{FileVisitResult, Files, NoSuchFileException, Path, SimpleFileVisitor, StandardCopyOption}
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.time.Duration
import java.util.concurrent.{CompletableFuture, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.function.{Consumer, IntFunction}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.databricks.spark.util.Log4jUsageLogger
import jdk.jfr.{AnnotationElement, EventFactory, FlightRecorder, Name, Recording, RecordingState, ValueDescriptor}
import jdk.jfr.consumer.RecordingFile
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.{FileRange, FSDataInputStream, Path => HadoopPath, RawLocalFileSystem}
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
  private case class Witness(path: String, statistics: IOStatistics)
  private case class VectorRead(
      streamId: Long,
      path: String,
      length: Int,
      synchronousBytesBefore: Long,
      var completedBytes: Option[Long] = None)
  case class Counts(
      synchronous: Map[String, Long],
      vectored: Map[String, Long],
      vectorIds: Set[Long])

  val vectorEventName: String = "delta.smoke.CompletedVectorRead"
  // Hadoop's asynchronous channel path has neither jdk.FileRead events nor stream_read_bytes.
  // Record only successful raw-range completions; keep the two components separate.
  private lazy val vectorEvents = EventFactory.create(
    Seq(new AnnotationElement(classOf[Name], vectorEventName)).asJava,
    Seq(new ValueDescriptor(classOf[String], "path"),
      new ValueDescriptor(java.lang.Long.TYPE, "rangeId"),
      new ValueDescriptor(java.lang.Long.TYPE, "bytesRead")).asJava)
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
      require(witnesses.size < 128, "Too many streams for the tiny reader-coverage smoke.")
      Some(result)
    } else None
    nextId += 1
    open += nextId
    statistics.foreach(value => witnesses(nextId) = Witness(path, value))
    nextId
  }

  def closed(id: Long): Unit = synchronized { open -= id }

  def uncovered(id: Long, api: String): Unit = synchronized {
    if (witnesses.contains(id)) uncoveredApis += api
  }

  def enableVectorEvents(recording: Recording): Unit = {
    recording.enable(vectorEvents.getEventType.getName)
      .withThreshold(Duration.ZERO).withoutStackTrace()
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
    require(witnesses(read.streamId).statistics.counters().get("stream_read_bytes").longValue() ==
      read.synchronousBytesBefore,
      "Vectored reads overlapped synchronous accounting; adding both would double-count.")
    val actualBytes = buffer.remaining().toLong
    val event = vectorEvents.newEvent()
    require(event.shouldCommit(), "The completed-vector JFR event is not being recorded.")
    event.set(0, read.path)
    event.set(1, java.lang.Long.valueOf(id))
    event.set(2, java.lang.Long.valueOf(actualBytes))
    event.commit()
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

  def finish(): Counts = synchronized {
    require(open.isEmpty, "Reader capture ended with unclosed raw streams.")
    require(uncoveredApis.isEmpty, s"Unaccounted reader APIs: $uncoveredApis")
    val values = witnesses.values.toSeq.map { witness =>
      val count = witness.statistics.counters().get("stream_read_bytes").longValue()
      require(count >= 0, "Negative raw-reader byte count.")
      witness.path -> count
    }
    val completed = vectors.values.toSeq.map { read =>
      read.path -> read.completedBytes.getOrElse {
        throw new IllegalStateException("A raw vector read is pending, failed or cancelled.")
      }
    }
    def total(reads: Seq[(String, Long)]): Map[String, Long] =
      reads.groupMapReduce(_._1)(_._2)((left, right) => Math.addExact(left, right))
        .filter(_._2 != 0L)
    Counts(total(values), total(completed), vectors.keySet.toSet)
  }

  def resetCapture(): Unit = synchronized {
    watched = Set.empty
    witnesses.clear()
    vectors.clear()
    uncoveredApis.clear()
  }

  def openCount: Int = synchronized { open.size }
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
      budget: Budget)(operation: => T): (T, Map[String, Any]) = {
    require(FlightRecorder.getFlightRecorder.getRecordings.isEmpty,
      "An unrelated recording would make resource accounting ambiguous.")
    require(FlightRecorder.getFlightRecorder.getEventTypes.asScala
      .exists(_.getName == "jdk.DataLoss"), "JFR data-loss detection is unavailable.")
    val recorder = new Recording()
    val destination = root.resolve(s"$label.jfr")
    budget.activeRecording.set(recorder)
    Utils.tryWithSafeFinally {
      recorder.enable("jdk.FileRead").withThreshold(Duration.ZERO).withoutStackTrace()
      recorder.enable("jdk.DataLoss")
      MergeSmokeReads.enableVectorEvents(recorder)
      recorder.setMaxSize(0L) // A rolling limit could silently discard the first target reads.
      require(recorder.getMaxAge == null, "JFR must not discard events by age.")
      MergeSmokeReads.begin(paths)
      recorder.start()
      val result = operation
      val negative = new FileInputStream(marker.toFile)
      Utils.tryWithSafeFinally {
        require(negative.read(new Array[Byte](128)) == 128, "Negative control read failed.")
      } {
        negative.close()
      }
      budget.check()
      require(recorder.getState == RecordingState.RUNNING, "Recording stopped prematurely.")
      recorder.stop()
      val independent = MergeSmokeReads.finish()
      require(independent.synchronous.nonEmpty || independent.vectored.nonEmpty,
        "The actual Parquet reader did not provide positive target-byte counters.")
      recorder.dump(destination)
      require(Files.size(destination) <= maxRecordingBytes, "JFR dump budget exceeded.")
      budget.check()
      val synchronous = mutable.Map.empty[String, Long].withDefaultValue(0L)
      val vectored = mutable.Map.empty[String, Long].withDefaultValue(0L)
      val vectorIds = mutable.Set.empty[Long]
      var markerBytes = 0L
      var eventCount = 0
      val input = new RecordingFile(destination)
      Utils.tryWithSafeFinally {
        while (input.hasMoreEvents) {
          val event = input.readEvent()
          eventCount += 1
          require(eventCount <= 20000, "JFR event-count budget exceeded.")
          require(event.getEventType.getName != "jdk.DataLoss",
            "JFR reported lost data; byte measurements are invalid.")
          if (event.getEventType.getName == "jdk.FileRead") {
            val rawPath = event.getString("path")
            require(rawPath != null, "A JFR file event is missing its path.")
            val path = new File(rawPath).getCanonicalPath
            val count = event.getLong("bytesRead")
            require(count >= 0, "Negative JFR byte count.")
            if (paths.contains(path)) {
              synchronous(path) = Math.addExact(synchronous(path), count)
            } else if (path == marker.toFile.getCanonicalPath) {
              markerBytes = Math.addExact(markerBytes, count)
            }
          } else if (event.getEventType.getName == MergeSmokeReads.vectorEventName) {
            val path = new File(event.getString("path")).getCanonicalPath
            val count = event.getLong("bytesRead")
            require(paths.contains(path) && count >= 0, "Unexpected vectored-read event.")
            require(vectorIds.add(event.getLong("rangeId")), "Duplicate vector completion event.")
            vectored(path) = Math.addExact(vectored(path), count)
          }
        }
      } {
        input.close()
      }
      require(markerBytes == 128L, "The unrelated-file JFR negative control was not observed.")
      val perFile = paths.toSeq.sorted.zipWithIndex.map { case (path, index) =>
        Map("fileIndex" -> index, "jdkFileReadBytes" -> synchronous(path),
          "hadoopSynchronousBytes" -> independent.synchronous.getOrElse(path, 0L),
          "vectorCompletionEventBytes" -> vectored(path),
          "completedRawVectorBytes" -> independent.vectored.getOrElse(path, 0L))
      }
      require(synchronous.toMap.filter(_._2 != 0L) == independent.synchronous &&
        vectored.toMap.filter(_._2 != 0L) == independent.vectored &&
        vectorIds.toSet == independent.vectorIds,
        s"JFR/raw-reader counters disagree; coverage is not established: $perFile")
      val totals = paths.map { path =>
        path -> Math.addExact(synchronous(path), vectored(path))
      }.toMap.filter(_._2 > 0L)
      result -> Map(
        "phase" -> label, "covered" -> true, "dataLossEvents" -> 0,
        "rollingRetention" -> false, "openRawStreams" -> MergeSmokeReads.openCount,
        "targetFilesRead" -> totals.size, "targetReaderBytes" -> totals.values.sum,
        "independentRawReaderBytes" ->
          Math.addExact(independent.synchronous.values.sum, independent.vectored.values.sum),
        "jdkFileReadBytes" -> synchronous.values.sum,
        "vectorCompletionEventBytes" -> vectored.values.sum,
        "completedVectorRanges" -> vectorIds.size,
        "vectorCompletionIdsVerified" -> true, "pendingVectorRanges" -> 0,
        "overlappedSynchronousAccounting" -> false,
        "vectorAccounting" -> "Successful raw FileRange buffers, not submitted/logical ranges",
        "vectorEventType" -> MergeSmokeReads.vectorEventName,
        "perFileBytes" -> perFile,
        "negativeControlBytes" -> markerBytes,
        "negativeControlExcludedFromTarget" -> true, "recordingBytes" -> Files.size(destination),
        "scope" -> "Actual local Parquet reads; not physical-disk or network bytes")
    } {
      Utils.tryWithSafeFinally {
        recorder.close()
      } {
        budget.activeRecording.set(null)
        MergeSmokeReads.resetCapture()
        Files.deleteIfExists(destination)
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
    val report = mutable.Map[String, Any](
      "schemaVersion" -> 1, "profile" -> "smoke", "status" -> "running",
      "benchmarkExecuted" -> false, "performanceClaim" -> false, "mergesStarted" -> 0,
      "mergesCompleted" -> 0)
    def save(): Unit = atomicJson(destination, report.toMap + ("phases" -> phases.toVector))
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
        scala.util.Properties.versionNumberString == "2.13.17", "Runtime pins changed.")
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
        coverage("actual-parquet-scan", manifest.keySet, marker, data, activeBudget) {
          scan.collect()
        }
      require(beforeRows.length == targetRows &&
        scanCoverage("targetFilesRead") == manifest.size, "The full target scan was not covered.")
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
          coverage(label, manifest.keySet, marker, data, activeBudget) {
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
        report("mergesCompleted") = merges
        phases += observed ++ Map("dataCorrect" -> true, "metricsCorrect" -> true,
          "commitVersion" -> stats.commitVersion,
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
      if (ownsData) cleanup { Utils.deleteRecursively(data.toFile) }
      report("status") = if (failure.isEmpty) "passed" else "failed"
      report("elapsedMs") = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
      report("openRawStreamsAfterCleanup") = MergeSmokeReads.openCount
      report("scratchDataRemoved") = ownsData && !Files.exists(data)
      cleanup { save() }
    }
    failure.foreach(error => throw error)
  }
}
