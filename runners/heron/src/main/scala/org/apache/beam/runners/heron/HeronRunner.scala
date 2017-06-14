package org.apache.beam.runners.heron

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.twitter.algebird.Semigroup
import com.twitter.summingbird.Producer
import com.twitter.summingbird.memory.Memory
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsValidator}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.runners.PipelineRunner
import org.slf4j.{Logger, LoggerFactory}

object HeronRunner {

  /**
    * Creates and returns a new HeronRunner with specified options.
    *
    * @param options The PipelineOptions to use when executing the job.
    * @return A pipeline runner that will execute with specified options.
    */
  def fromOptions(options: PipelineOptions): HeronRunner = {
    val heronOptions: HeronPipelineOptions = PipelineOptionsValidator.validate(classOf[HeronPipelineOptions], options)
    new HeronRunner(heronOptions)
  }
}

/**
  * Prototype Heron runner. To build and install do:
  *
  * (cd runners/heron && mvn install)
  *
  * To run wordcount:
  *
  * cd examples/java
  * mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount -Dexec.args="--runner=HeronRunner --inputFile=pom.xml --output=counts" -Pheron-runner
  *
  * To debug, use mvnDebug instead.
  *
  */
final class HeronRunner extends PipelineRunner[HeronPipelineResult] {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[HeronRunner])
  var options: HeronPipelineOptions = null

  def this(heronOptions: HeronPipelineOptions) {
    this()
    options = heronOptions
  }

  def run(pipeline: Pipeline): HeronPipelineResult = {
    LOG.info("Running HeronRunner")

    val heronRuntimeContext: HeronRuntimeContext = new HeronRuntimeContext(pipeline)
    val batchContext = new SummingbirdBatchContext(heronRuntimeContext)
    val visitor: SummingbirdVisitor = new SummingbirdVisitor(batchContext, heronRuntimeContext)
    pipeline.traverseTopologically(visitor)
    batchContext.execute()

//    doWordCount()

    LOG.info("Running HeronRunner - DONE")
    new HeronPipelineResult
  }

  private def doWordCount() = {
    val fileSource: TraversableOnce[String] = scala.io.Source.fromFile("pom.xml").getLines
    val fileSink: (String => Unit) = { (value: String) =>
      Files.write(Paths.get("output.txt"), s"$value\n".getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    }

    val platform = new Memory
    val sinkBuffer = collection.mutable.Buffer[(String, Long)]()
    val store: Memory#Store[String, Long] = collection.mutable.Map.empty[String, Long]

    val source: Producer[Memory, String] = Memory.toSource(fileSource)
    val sink: Memory#Sink[String] = fileSink

    val summed: Producer[Memory, String] = source
      .flatMap { sentance =>
        sentance.trim().split(" ").map(_ -> 1L)
      }
      .sumByKey(store)
      .map {
        case (key, (existingEventOpt, currentEvent)) =>
          existingEventOpt.map { existingEvent =>
            s"$key\t${Semigroup.plus(existingEvent, currentEvent)}"
          }.getOrElse(s"$key\t$currentEvent")
      }
    val write1 = summed.write(sink)
    val write2 = summed.write(sink)
    val job = write1.also(write2)

    platform.run(platform.plan(job))
  }

  private def doIntCount() = {
    val fileSource: TraversableOnce[Int] = scala.io.Source.fromFile("input.txt").getLines.map { x: String => x.toInt }
    val fileSink: (Int => Unit) = { value: Int =>
      Files.write(Paths.get("output.txt"), s"$value\n".getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    }

    val platform = new Memory
    val sinkBuffer = collection.mutable.Buffer[Int]()
    val store: Memory#Store[Int, Int] = collection.mutable.Map.empty[Int, Int]

    val source: Producer[Memory, Int] = Memory.toSource(List(1, 2))
//    val source = Memory.toSource(fileSource)
//    val sink: Memory#Sink[Int] = fileSink
    val sink: Memory#Sink[Int] = { x: Int =>
      sinkBuffer += x
      LOG.info(s"MemorySink x=$x buffer=$sinkBuffer")
    }

    val summed = source
      .map { v => (v, v) }
      .sumByKey(store)
      .map {
        case (_, (existingEventOpt, currentEvent)) =>
          LOG.info(s"currentEvent=$currentEvent")
          existingEventOpt.map { existingEvent =>
            LOG.info(s"existingEvent=$existingEvent")
            Semigroup.plus(existingEvent, currentEvent)
          }.getOrElse(currentEvent)
      }

    val write1 = summed.write(sink)
    val write2 = summed.write(sink)
    val job = write1.also(write2)

    platform.run(platform.plan(job))
//    assert(1 == store(1))
//    assert(2 == store(2))
//    assert(List(1, 2) == sinkBuffer.toList)
    assert(2 == store(1))
    assert(4 == store(2))
    assert(List(1, 2, 2, 4) == sinkBuffer.toList)
  }
}
