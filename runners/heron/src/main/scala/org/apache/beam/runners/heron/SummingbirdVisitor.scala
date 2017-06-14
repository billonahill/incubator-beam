package org.apache.beam.runners.heron

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.google.common.base.Preconditions._
import com.google.common.collect.Iterables
import com.twitter.summingbird.memory.Memory
import com.twitter.summingbird.{Platform, Producer, TailProducer}
import org.apache.beam.sdk.Pipeline.PipelineVisitor
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.{BoundedSource, Read}
import org.apache.beam.sdk.io.Read.Bounded
import org.apache.beam.sdk.runners.TransformHierarchy
import org.apache.beam.sdk.transforms.View.CreatePCollectionView
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, Window, WindowFn}
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.reflect.{DoFnSignature, DoFnSignatures}
import org.apache.beam.sdk.util.{WindowedValue, WindowingStrategy}
import org.apache.beam.sdk.values._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

object LoggingVisitor {
  /**
    * Utility formatting method.
    *
    * @param n number of spaces to generate
    * @return String with "|" followed by n spaces
    */
  protected def genSpaces(n: Int): String = {
    val builder: StringBuilder = new StringBuilder
    for (_ <- 0 to n) {
      builder.append("| ")
    }
    builder.toString
  }

  private def formatNodeName(node: TransformHierarchy#Node): String = {
    var className = ""
    if (node.getTransform != null) {
      className = node.getTransform.getClass.getName.split("\\.").last
    }
    s"${node.toString.split("@"){1}}:${node.getTransform}:$className"
  }
}

class LoggingVisitor extends PipelineVisitor {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[LoggingVisitor])
  private var depth = 0

  @Override
  def enterCompositeTransform(node: TransformHierarchy#Node): CompositeBehavior = {
    LOG.info(s"${LoggingVisitor.genSpaces(this.depth)} enterComposite ${LoggingVisitor.formatNodeName(node)}")
    this.depth += 1

    CompositeBehavior.ENTER_TRANSFORM
  }

  @Override
  def leaveCompositeTransform(node: TransformHierarchy#Node): Unit = {
    this.depth -= 1
    LOG.info(s"${LoggingVisitor.genSpaces(this.depth)} leaveComposite ${LoggingVisitor.formatNodeName(node)}")
  }

  @Override
  def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit = {
    LOG.info(s"${LoggingVisitor.genSpaces(this.depth)} visitPrimitive ${LoggingVisitor.formatNodeName(node)}")
    //LOG.info(s"${LoggingVisitor.genSpaces(this.depth)} \\--  ${node.getInputs} ${node.getOutputs}")
  }


  @Override
  def visitValue(value: PValue, producer: TransformHierarchy#Node): Unit =
    LOG.info(
      s"${LoggingVisitor.genSpaces(this.depth)} visitValue in: ${LoggingVisitor.formatNodeName(producer)} out: $value")
}

trait BatchTransformTranslator[TransformT <: PTransform[_, _]] {
  def translateNode(transform: TransformT, batchContext: SummingbirdBatchContext)
}

object SummingbirdVisitor {
  @SuppressWarnings(Array("rawtypes"))
  val TRANSLATORS: Map[Class[_ <: PTransform[_, _]], BatchTransformTranslator[_]] = Map(
    (classOf[Create.Values[_]], new CreateTranslatorBatch()),
    (classOf[Read.Bounded[_]], new ReadSourceTranslatorBatch()),
    (classOf[ParDo.Bound[_, _]], new ParDoTranslatorBatch()),
    (classOf[View.CreatePCollectionView[_, _]], new CreatePCollectionViewTranslatorBatch()),
    (classOf[Window.Bound[_]], new WindowBoundTranslatorBatch())
  )
//      TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslatorBatch())
//      TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslatorBatch())
//      TRANSLATORS.put(Reshuffle.class, new ReshuffleTranslatorBatch())
//      TRANSLATORS.put(Flatten.FlattenPCollectionList.class, new FlattenPCollectionTranslatorBatch())
//      TRANSLATORS.put(ParDo.BoundMulti.class, new ParDoBoundMultiTranslatorBatch())

    private def getTranslator(transform: PTransform[_, _]): Option[BatchTransformTranslator[_]]
      = TRANSLATORS.get(transform.getClass)
  }

/**
  *
  */
class SummingbirdVisitor(batchContext: SummingbirdBatchContext,
                         heronRuntimeContext: HeronRuntimeContext) extends LoggingVisitor {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SummingbirdVisitor])

  @Override
  override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit = {
    super.visitPrimitiveTransform(node)
    // get the transformation corresponding to the node we are
    // currently visiting and translate it into its Heron alternative.
    val transform: PTransform[_, _] = node.getTransform
    SummingbirdVisitor.getTranslator(transform) match {
      case Some(translator: BatchTransformTranslator[_]) => applyBatchTransform(transform, node, translator)
      case _ =>
        LOG.warn(s"The transform $transform is currently not supported.")
//        throw new UnsupportedOperationException(s"The transform $transform is currently not supported.")
    }
  }

  def applyBatchTransform[T <: PTransform[_, _]](transform: PTransform[_, _],
                                                 node: TransformHierarchy#Node,
                                                 translator: BatchTransformTranslator[_]) : Unit = {
    @SuppressWarnings(Array("unchecked"))
    val typedTransform: T = transform.asInstanceOf[T]

    @SuppressWarnings(Array("unchecked"))
    val typedTranslator: BatchTransformTranslator[T] = translator.asInstanceOf[BatchTransformTranslator[T]]

    batchContext.setCurrentTransform(node.toAppliedPTransform)

    // create the applied PTransform on the batchContext
    typedTranslator.translateNode(typedTransform, batchContext)
  }
}

class SummingbirdBatchContext(heronRuntimeContext: HeronRuntimeContext) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SummingbirdBatchContext])
  private val producers: mutable.Map[PValue, Producer[_ <: Platform[_], _]] = mutable.Map()
  private val leaves: mutable.Map[PValue, Producer[_ <: Platform[_], _]] = mutable.Map()
  private var currentTransform: AppliedPTransform[_, _, _] = _

  private val printSink: (Object => Unit) = { (value: Object) =>
    LOG.info(s"sink value: ${value.toString}")
  }

  private val fileSink: (Object => Unit) = { (value: Object) =>
    Files.write(Paths.get("outputWindows.txt"), s"${value.toString}\n".getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }

  def getHeronRuntimeContext: HeronRuntimeContext = heronRuntimeContext

  def execute() = {
    // TODO: this is all jank town. Trying to see if I can manually chain on of the producers to a sink here but we
    // would never do this. Instead we'd properly assemble the graph in a way that we can chain a source/producer chain
    // to a TailProducer and execute that
    val sourceKVP: (PValue, Producer[_ <: Platform[_], _]) = producers.seq.iterator.next()
    val source: Producer[Memory, String] = sourceKVP._2.asInstanceOf[Producer[Memory, String]]
    val sink: Memory#Sink[Object] = printSink

    val job: TailProducer[Memory, String] = source.write(sink)

    val platform = new Memory
    platform.run(platform.plan(job))
  }

  def setCurrentTransform(currentTransform: AppliedPTransform[_, _, _]) =
    this.currentTransform = currentTransform

  def getCurrentTransform: AppliedPTransform[_, _, _] = currentTransform

  def getRuntimeContext: HeronRuntimeContext = heronRuntimeContext

  def getInput[T <: PInput](transform: PTransform[T, _]): T = {
    checkArgument(currentTransform != null && currentTransform.getTransform.equals(transform),
      "can only be called with current transform %s not with %s", currentTransform, transform)
    @SuppressWarnings(Array("unchecked"))
    val input: T = currentTransform.getInput.asInstanceOf[T]
    input
  }

  def getOutput[T <: POutput](transform: PTransform[_, T]): T = {
    checkArgument(currentTransform != null && currentTransform.getTransform.equals(transform),
      "can only be called with current transform %s not with %s", currentTransform, transform)
    @SuppressWarnings(Array("unchecked"))
    val output: T = currentTransform.getOutput.asInstanceOf[T]
    output
  }

  def putProducer[T](transform: PTransform[_, _], producer: Producer[_ <: Platform[_], WindowedValue[T]]) {
//    @SuppressWarnings(Array("unchecked"))
//    val casted: PTransform[_, T] = transform.asInstanceOf[PTransform[_, T]]
    @SuppressWarnings(Array("unchecked"))
    val output: T = currentTransform.getOutput.asInstanceOf[T]
    val pvalue: PValue = output.asInstanceOf[PValue]
    putProducer(pvalue, producer)
  }

  def putProducer[T](pvalue: PValue, producer: Producer[_ <: Platform[_], WindowedValue[T]]) {
    producer.name(pvalue.getName)
    producers(pvalue) = producer
    leaves(pvalue) = producer
  }

  @SuppressWarnings(Array("unchecked"))
  def getInputProducer[T](pvalue: PValue): Producer[_ <: Platform[_], WindowedValue[T]] = {
    // assume that the DataSet is used as an input if retrieved here
    leaves.remove(pvalue)
    producers.get(pvalue) match {
      case Some(x) => x.asInstanceOf[Producer[_ <: Platform[_], WindowedValue[T]]]
      case None => throw new RuntimeException(s"Could not find producer for $pvalue")
    }
  }
}

class ReadSourceTranslatorBatch[T] extends BatchTransformTranslator[Read.Bounded[T]] {
  def translateNode(transform: Bounded[T], context: SummingbirdBatchContext): Unit = {
    val source: BoundedSource[T] = transform.getSource
    val bounded: Producer[Memory, WindowedValue[T]] = HeronBounded.bounded[T](source, context.getHeronRuntimeContext)
    context.putProducer(transform, bounded)
  }
}

class CreateTranslatorBatch[T] extends BatchTransformTranslator[Create.Values[T]] {
  def translateNode(transform: Create.Values[T], context: SummingbirdBatchContext): Unit = {
    val elems: java.lang.Iterable[T] = transform.getElements
    // Use a coder to convert the objects in the PCollection to byte arrays, so they
    // can be transferred over the network.
    val coder: Coder[T] = context.getOutput(transform).getCoder
    val create: Producer[Memory, WindowedValue[T]] = HeronCreate[T](elems, coder)
    context.putProducer(transform, create)
  }
}

class CreatePCollectionViewTranslatorBatch[ElemT, ViewT] extends BatchTransformTranslator[View.CreatePCollectionView[ElemT, ViewT]] {
  def translateNode(transform: CreatePCollectionView[ElemT, ViewT], context: SummingbirdBatchContext): Unit = {
    val inputDataSet: Producer[_ <: Platform[_], WindowedValue[ElemT]] = context.getInputProducer(context.getInput(transform))
    val input: PCollectionView[ViewT] = transform.getView

    // TODO: implement for real
//    context.setSideInputDataSet(input, inputDataSet)
  }
}

class WindowBoundTranslatorBatch[T] extends BatchTransformTranslator[Window.Bound[T]] {
  def translateNode(transform: Window.Bound[T], context: SummingbirdBatchContext): Unit = {
    val inputDataSet: Producer[_ <: Platform[_], WindowedValue[T]] = context.getInputProducer(context.getInput(transform))

    @SuppressWarnings(Array("unchecked"))
    val windowingStrategy: WindowingStrategy[T, _ <: BoundedWindow] =
      context.getOutput(transform).getWindowingStrategy.asInstanceOf[WindowingStrategy[T, _ <: BoundedWindow]]
    val windowFn: WindowFn[T, _ <: BoundedWindow] = windowingStrategy.getWindowFn

    // TODO: implement for real
    val resultDataSet: Producer[_ <: Platform[_], WindowedValue[T]] = inputDataSet.map(x => x)

    context.putProducer(context.getOutput(transform), resultDataSet)
  }
}

class ParDoTranslatorBatch[InputT, OutputT] extends BatchTransformTranslator[ParDo.Bound[InputT, OutputT]] {
  def translateNode(transform: ParDo.Bound[InputT, OutputT], context: SummingbirdBatchContext): Unit = {
    val doFn: DoFn[InputT, OutputT] = transform.getNewFn
    rejectStateAndTimers(doFn)

    val inputProducer: Producer[_ <: Platform[_], WindowedValue[InputT]] = context.getInputProducer(context.getInput(transform))
    val sideInputs: java.util.List[PCollectionView[_]] = transform.getSideInputs

    @SuppressWarnings(Array("unchecked"))
    val windowFn: WindowFn[AnyRef, _] = context.getInput(transform).getWindowingStrategy.getWindowFn.asInstanceOf[WindowFn[AnyRef, _]]

    val doFnFunction = new DoFnFunction[InputT, OutputT](transform.getFn, context.getRuntimeContext, sideInputs, windowFn)
    val outputProducer: Producer[_ <: Platform[_], WindowedValue[OutputT]] = inputProducer.flatMap { x =>
      doFnFunction.call(List(x).asJava.iterator()).iterator().asScala
    }

    context.putProducer(context.getOutput(transform), outputProducer)
  }

  private def rejectStateAndTimers(doFn: DoFn[_, _]) {
    val signature: DoFnSignature = DoFnSignatures.getSignature(doFn.getClass)
    if (signature.stateDeclarations.size > 0) {
      throw new UnsupportedOperationException(String.format(
        "Found %s annotations on %s, but %s cannot yet be used with state in the %s.",
        classOf[DoFn.StateId].getSimpleName, doFn.getClass.getName, classOf[DoFn[_, _]].getSimpleName, classOf[HeronRunner].getSimpleName))
    }
    if (signature.timerDeclarations.size > 0) {
      throw new UnsupportedOperationException(String.format(
        "Found %s annotations on %s, but %s cannot yet be used with timers in the %s.",
        classOf[DoFn.TimerId].getSimpleName, doFn.getClass.getName, classOf[DoFn[_, _]].getSimpleName, classOf[HeronRunner].getSimpleName))
    }
    }
}

object WindowingHelpers {
  def windowValueFunction[T]: com.google.common.base.Function[T, WindowedValue[T]] =
    new com.google.common.base.Function[T, WindowedValue[T]]() {
    def apply(t: T): WindowedValue[T] = WindowedValue.valueInGlobalWindow(t)
  }
}

object HeronCreate {
  def apply[T](values: java.lang.Iterable[T], coder: Coder[T]): Producer[Memory, WindowedValue[T]] = {
    val x: java.lang.Iterable[WindowedValue[T]] = Iterables.transform(values, WindowingHelpers.windowValueFunction[T])
    val y: java.util.Iterator[WindowedValue[T]] = x.iterator()
    Memory.toSource(y.asScala)(null)
  }
}

object HeronBounded {
  def bounded[T](source: BoundedSource[T], runtimeContext: HeronRuntimeContext): Producer[Memory, WindowedValue[T]] = {
    Memory.toSource(new HeronBounded(source, runtimeContext))(null)
  }
}

case class HeronBounded[T](source: BoundedSource[T], runtimeContext: HeronRuntimeContext) extends TraversableOnce[WindowedValue[T]] {
  // TODO: this is a toy implementation since we don't handle partitioning. see SourceRDD
  val iter: Iterator[WindowedValue[T]] = new Iterator[WindowedValue[T]]() {
    val reader: BoundedSource.BoundedReader[T] = source.createReader(runtimeContext.getPipelineOptions)
    var finished: Boolean = false
    var started: Boolean = false
    var closed: Boolean = false

    def hasNext: Boolean =
      try {

        if (!started) {
          started = true
          finished = !reader.start
        }
        else finished = !reader.advance
        if (finished) {
          // safely close the reader if there are no more elements left to read.
          closeIfNotClosed()
        }
        !finished

      } catch {
        case e: IOException => {
          closeIfNotClosed()
          throw new RuntimeException("Failed to read from reader.", e)
        }
      }

    def next: WindowedValue[T] =
      WindowedValue.timestampedValueInGlobalWindow(reader.getCurrent, reader.getCurrentTimestamp)

    def remove() =
      throw new UnsupportedOperationException("Remove from partition iterator is not allowed.")

    def closeIfNotClosed() {
      if (!closed) {
        closed = true
        try {
          reader.close()
        } catch {
          case e: IOException => {
            throw new RuntimeException("Failed to close Reader.", e)
          }
        }
      }
    }
  }

  def foreach[U](f: (WindowedValue[T]) => U): Unit = iter.foreach(f)
  def isEmpty: Boolean = iter.isEmpty
  def hasDefiniteSize: Boolean = iter.hasDefiniteSize
  def seq: TraversableOnce[WindowedValue[T]] = iter.seq
  def forall(p: (WindowedValue[T]) => Boolean): Boolean = iter.forall(p)
  def exists(p: (WindowedValue[T]) => Boolean): Boolean = iter.exists(p)
  def find(p: (WindowedValue[T]) => Boolean): Option[WindowedValue[T]] = iter.find(p)
  def copyToArray[B >: WindowedValue[T]](xs: Array[B], start: Int, len: Int): Unit = iter.copyToArray(xs, start, len)
  def toTraversable: Traversable[WindowedValue[T]] = iter.toTraversable
  def isTraversableAgain: Boolean = iter.isTraversableAgain
  def toStream: Stream[WindowedValue[T]] = iter.toStream
  def toIterator: Iterator[WindowedValue[T]] = iter.toIterator
}