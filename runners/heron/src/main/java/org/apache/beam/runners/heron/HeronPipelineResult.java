package org.apache.beam.runners.heron;

import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;
import org.joda.time.Duration;

import java.io.IOException;

/**
 *
 */
public class HeronPipelineResult implements PipelineResult {

  protected PipelineResult.State state;

  HeronPipelineResult() {
    state = State.RUNNING;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public State cancel() throws IOException {
    if (state != null && !state.isTerminal()) {

      // TODO: cancel

      state = PipelineResult.State.CANCELLED;
    }

    return state;
  }

  @Override
  public State waitUntilFinish(Duration duration) {

    // TODO: block until execution is finished

    state = PipelineResult.State.DONE;
    return state;
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(Duration.millis(Long.MAX_VALUE));
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator) throws AggregatorRetrievalException {
    return null;
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("The HeronRunner does not currently support metrics.");
  }
}
