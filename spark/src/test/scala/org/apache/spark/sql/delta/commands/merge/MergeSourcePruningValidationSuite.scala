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

import java.io.File
import java.nio.file.Paths

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Diagnostic controls only: every MERGE uses the unmodified production command.
 * Literal bounds are known from synthetic input, not an implemented source-summary optimization.
 */
class MergeSourcePruningValidationSuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest {
  import testImplicits._

  private val smokeOnly = sys.props.get("delta.mergePruning.profile").contains("smoke")

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf.setMaster("local[2]")
    if (smokeOnly) {
      conf.set("spark.hadoop.fs.file.impl", classOf[MergeSmokeFileSystem].getName)
        .set("spark.default.parallelism", "2")
        .set("spark.task.cpus", "1")
    } else conf
  }

  private val update = "WHEN MATCHED THEN UPDATE SET value = s.value"

  private def compare(
      targetData: DataFrame,
      sourceData: DataFrame,
      bound: String,
      clauses: String,
      expected: Seq[Row],
      partitioned: Boolean = false): Unit = {
    withTempDir { directory =>
      withTempView("pruning_source") {
        val base = new File(directory, "base").getCanonicalPath
        val writer = targetData.coalesce(1).write.format("delta")
        if (partitioned) writer.partitionBy("key").save(base) else writer.save(base)
        sourceData.createOrReplaceTempView("pruning_source")
        val metrics = Seq(false, true).map { bounded =>
          val target = new File(directory, s"target-$bounded").getCanonicalPath
          spark.sql(s"CREATE TABLE delta.`$target` SHALLOW CLONE delta.`$base` VERSION AS OF 0")
          val predicate = if (bounded) s" AND ($bound)" else ""
          val (_, stats) = MergeSourcePruningBenchmark.execute(
            spark,
            s"MERGE INTO delta.`$target` t USING pruning_source s " +
              s"ON t.key = s.key$predicate $clauses")
          checkAnswer(spark.read.format("delta").load(target), expected)
          Seq(stats.source.rows, Some(stats.targetRowsUpdated), Some(stats.targetRowsDeleted),
            Some(stats.targetRowsInserted))
        }
        assert(metrics.head == metrics.last)
      }
    }
  }

  if (!smokeOnly) {
    registerExistingControls()
  } else {
    test("resource serialization output smoke") {
      val root = sys.props.get("delta.mergePruning.root").getOrElse {
        fail("Set delta.mergePruning.root to the isolated smoke scratch directory.")
      }
      val output = sys.props.get("delta.mergePruning.results").getOrElse {
        fail("Set delta.mergePruning.results to the isolated diagnostic output directory.")
      }
      MergeSourcePruningSmoke.run(spark, Paths.get(root), Paths.get(output))
    }
  }

  private def registerExistingControls(): Unit = {
    Seq(
      ("TINYINT", "-128", "127"),
      ("SMALLINT", "-32768", "32767"),
      ("INT", "-2147483648", "2147483647"),
      ("BIGINT", "-9223372036854775808", "9223372036854775807")
    ).foreach { case (dataType, minimum, maximum) =>
      test(s"literal control preserves $dataType inclusive extreme keys") {
        val target = spark.sql(
          s"SELECT CAST(key AS $dataType) key, value FROM " +
            s"VALUES ('$minimum', 0L), ('0', 0L), ('$maximum', 0L) AS t(key, value)")
        val source = spark.sql(
          s"SELECT CAST(key AS $dataType) key, value FROM " +
            s"VALUES ('$minimum', 1L), ('$maximum', 1L) AS s(key, value)")
        val expected = target.selectExpr(
          "key", s"IF(key = 0, 0L, 1L) AS value").collect().toSeq
        compare(target, source,
          s"t.key >= CAST('$minimum' AS $dataType) AND " +
            s"t.key <= CAST('$maximum' AS $dataType)", update, expected)
      }
    }

    test("literal control preserves duplicate target keys and untouched rows in the same file") {
      compare(
        Seq((1L, 0L), (1L, 0L), (900L, 0L)).toDF("key", "value"),
        Seq((1L, 7L)).toDF("key", "value"),
        "t.key >= 1L AND t.key <= 1L", update,
        Seq(Row(1L, 7L), Row(1L, 7L), Row(900L, 0L)))
    }

    test("literal control preserves ordered clauses and matches old assigned keys") {
      compare(
        Seq((1L, 0L), (2L, 0L), (3L, 0L)).toDF("key", "value"),
        Seq((1L, 10L), (2L, 20L)).toDF("key", "value"),
        "t.key >= 1L AND t.key <= 2L",
        "WHEN MATCHED AND s.key = 1L THEN UPDATE SET key = 101L, value = s.value " +
          "WHEN MATCHED THEN UPDATE SET key = 102L, value = s.value",
        Seq(Row(101L, 10L), Row(102L, 20L), Row(3L, 0L)),
        partitioned = true)
    }

    test("literal control preserves ordinary equality null behavior") {
      compare(
        spark.sql("SELECT * FROM VALUES (1L, 0L), (CAST(NULL AS BIGINT), 0L) t(key, value)"),
        spark.sql("SELECT * FROM VALUES (1L, 7L), (CAST(NULL AS BIGINT), 9L) s(key, value)"),
        "t.key >= 1L AND t.key <= 1L", update,
        Seq(Row(1L, 7L), Row(null, 0L)))
    }

    test("empty and all-null sources retain the original no-op path and source metrics") {
      Seq("SELECT 1L key, 1L value WHERE false",
        "SELECT CAST(NULL AS BIGINT) key, 1L value").foreach { sourceSql =>
        compare(Seq((1L, 0L)).toDF("key", "value"), spark.sql(sourceSql),
          "true", update, Seq(Row(1L, 0L)))
      }
    }

    test("literal control preserves duplicate source match errors") {
      withTempDir { directory =>
        withTempView("pruning_source") {
          Seq((1L, 7L), (1L, 8L)).toDF("key", "value")
            .createOrReplaceTempView("pruning_source")
          Seq("", " AND t.key >= 1L AND t.key <= 1L").zipWithIndex.foreach {
            case (bound, index) =>
              val target = new File(directory, s"target-$index").getCanonicalPath
              Seq((1L, 0L)).toDF("key", "value").write.format("delta").save(target)
              val version = DeltaLog.forTable(spark, target).update().version
              val error = intercept[Exception] {
                spark.sql(s"MERGE INTO delta.`$target` t USING pruning_source s " +
                  s"ON t.key = s.key$bound $update").collect()
              }
              assert(Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null)
                .exists(e => Option(e.getMessage).exists(
                  _.contains("DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"))))
              assert(DeltaLog.forTable(spark, target).update().version == version)
              checkAnswer(spark.read.format("delta").load(target), Seq(Row(1L, 0L)))
          }
        }
      }
    }

    test("source-key bounds are not valid for not matched by source actions") {
      withTempDir { directory =>
        withTempView("pruning_source") {
          Seq((1L, 7L)).toDF("key", "value").createOrReplaceTempView("pruning_source")
          val target = new File(directory, "target").getCanonicalPath
          Seq((1L, 0L), (2L, 0L)).toDF("key", "value")
            .write.format("delta").partitionBy("key").save(target)
          spark.sql(s"MERGE INTO delta.`$target` t USING pruning_source s ON t.key = s.key " +
            s"$update WHEN NOT MATCHED BY SOURCE THEN DELETE").collect()
          checkAnswer(spark.read.format("delta").load(target), Seq(Row(1L, 7L)))
        }
      }
    }

    test("null-safe equality retains a matching null target outside numeric bounds") {
      withTempDir { directory =>
        withTempView("pruning_source") {
          spark.sql("SELECT CAST(NULL AS BIGINT) key, 7L value")
            .createOrReplaceTempView("pruning_source")
          val target = new File(directory, "target").getCanonicalPath
          spark.sql("SELECT CAST(NULL AS BIGINT) key, 0L value")
            .write.format("delta").save(target)
          spark.sql(s"MERGE INTO delta.`$target` t USING pruning_source s " +
            s"ON t.key <=> s.key $update").collect()
          checkAnswer(spark.read.format("delta").load(target), Seq(Row(null, 7L)))
        }
      }
    }

    test("JFR target-byte accounting observes instrumented stream and positioned reads") {
      withTempDir { directory =>
        MergeSourcePruningBenchmark.verifyFileReadAccounting(directory)
      }
    }

    test("measure literal-bound controls against one optimized batched MERGE") {
      val destination = sys.props.get("delta.mergePruning.results").getOrElse {
        fail("Set delta.mergePruning.results to an isolated diagnostic output directory.")
      }
      withTempDir { directory =>
        MergeSourcePruningBenchmark.measureControls(spark, directory, Paths.get(destination))
      }
    }
  }
}
