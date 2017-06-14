package org.apache.beam.runners.heron;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class HeronRuntimeContext implements Serializable {
  private final String serializedPipelineOptions;

  /**
   * Map fo names to Beam aggregators.
   */
  private final Map<String, Aggregator<?, ?>> aggregators = new HashMap<>();
  private transient CoderRegistry coderRegistry;

  HeronRuntimeContext(Pipeline pipeline) {
    this.serializedPipelineOptions = serializePipelineOptions(pipeline.getOptions());
  }

  private String serializePipelineOptions(PipelineOptions pipelineOptions) {
    try {
      return new ObjectMapper().writeValueAsString(pipelineOptions);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize the pipeline options.", e);
    }
  }

  private static PipelineOptions deserializePipelineOptions(String serializedPipelineOptions) {
    try {
      return new ObjectMapper().readValue(serializedPipelineOptions, PipelineOptions.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize the pipeline options.", e);
    }
  }

  public synchronized PipelineOptions getPipelineOptions() {
    return deserializePipelineOptions(serializedPipelineOptions);
  }

//  /**
//   * Creates and aggregator and associates it with the specified name.
//   *
//   * @param accum     Spark Accumulator.
//   * @param named     Name of aggregator.
//   * @param combineFn Combine function used in aggregation.
//   * @param <InputT>  Type of inputs to aggregator.
//   * @param <InterT>  Intermediate data type
//   * @param <OutputT> Type of aggregator outputs.
//   * @return Specified aggregator
//   */
//  public synchronized <InputT, InterT, OutputT> Aggregator<InputT, OutputT> createAggregator(
//      Accumulator<NamedAggregators> accum,
//      String named,
//      Combine.CombineFn<? super InputT, InterT, OutputT> combineFn) {
//    @SuppressWarnings("unchecked")
//    Aggregator<InputT, OutputT> aggregator = (Aggregator<InputT, OutputT>) aggregators.get(named);
//    if (aggregator == null) {
//      @SuppressWarnings("unchecked")
//      NamedAggregators.CombineFunctionState<InputT, InterT, OutputT> state =
//          new NamedAggregators.CombineFunctionState<>(
//              (Combine.CombineFn<InputT, InterT, OutputT>) combineFn,
//              (Coder<InputT>) getCoder(combineFn),
//              this);
//      accum.add(new NamedAggregators(named, state));
//      aggregator = new SparkAggregator<>(named, state);
//      aggregators.put(named, aggregator);
//    }
//    return aggregator;
//  }

  public CoderRegistry getCoderRegistry() {
    if (coderRegistry == null) {
      coderRegistry = new CoderRegistry();
      coderRegistry.registerStandardCoders();
    }
    return coderRegistry;
  }

  private Coder<?> getCoder(Combine.CombineFn<?, ?, ?> combiner) {
    try {
      if (combiner.getClass() == Sum.SumIntegerFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Integer.class));
      } else if (combiner.getClass() == Sum.SumLongFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Long.class));
      } else if (combiner.getClass() == Sum.SumDoubleFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Double.class));
      } else if (combiner.getClass() == Min.MinIntegerFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Integer.class));
      } else if (combiner.getClass() == Min.MinLongFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Long.class));
      } else if (combiner.getClass() == Min.MinDoubleFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Double.class));
      } else if (combiner.getClass() == Max.MaxIntegerFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Integer.class));
      } else if (combiner.getClass() == Max.MaxLongFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Long.class));
      } else if (combiner.getClass() == Max.MaxDoubleFn.class) {
        return getCoderRegistry().getDefaultCoder(TypeDescriptor.of(Double.class));
      } else {
        throw new IllegalArgumentException("unsupported combiner in Aggregator: "
            + combiner.getClass().getName());
      }
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException("Could not determine default coder for combiner", e);
    }
  }

//  /**
//   * Initialize spark aggregators exactly once.
//   *
//   * @param <InputT> Type of element fed in to aggregator.
//   */
//  private static class SparkAggregator<InputT, OutputT>
//      implements Aggregator<InputT, OutputT>, Serializable {
//    private final String name;
//    private final NamedAggregators.State<InputT, ?, OutputT> state;
//
//    SparkAggregator(String name, NamedAggregators.State<InputT, ?, OutputT> state) {
//      this.name = name;
//      this.state = state;
//    }
//
//    @Override
//    public String getName() {
//      return name;
//    }
//
//    @Override
//    public void addValue(InputT elem) {
//      state.update(elem);
//    }
//
//    @Override
//    public Combine.CombineFn<InputT, ?, OutputT> getCombineFn() {
//      return state.getCombineFn();
//    }
//  }
}