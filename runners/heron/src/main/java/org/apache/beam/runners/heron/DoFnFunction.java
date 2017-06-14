/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.heron;

import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Beam's Do functions correspond to Summingbird's FlatMap functions.
 *
 * @param <InputT> Input element type.
 * @param <OutputT> Output element type.
 */
public class DoFnFunction<InputT, OutputT>  {
  //private final Accumulator<NamedAggregators> accum;
  private final OldDoFn<InputT, OutputT> mFunction;
  private final HeronRuntimeContext mRuntimeContext;
  private final List<PCollectionView<?>> mSideInputs;
  private final WindowFn<Object, ?> windowFn;

  /**
   * @param fn                DoFunction to be wrapped.
   * @param runtime           Runtime to apply function in.
   * @param sideInputs        Side inputs used in DoFunction.
   * @param windowFn          Input {@link WindowFn}.
   */
  public DoFnFunction(//Accumulator<NamedAggregators> accum,
                      OldDoFn<InputT, OutputT> fn,
                      HeronRuntimeContext runtime,
                      List<PCollectionView<?>> sideInputs,
                      WindowFn<Object, ?> windowFn) {
    //this.accum = accum;
    this.mFunction = fn;
    this.mRuntimeContext = runtime;
    this.mSideInputs = sideInputs;
    this.windowFn = windowFn;
  }


  public Iterable<WindowedValue<OutputT>> call(Iterator<WindowedValue<InputT>> iter) throws
      Exception {
    return new ProcCtxt(mFunction, mRuntimeContext, mSideInputs, windowFn).callWithCtxt(iter);
  }

  private class ProcCtxt extends HeronProcessContext<InputT, OutputT, WindowedValue<OutputT>> {

    private final List<WindowedValue<OutputT>> outputs = new LinkedList<>();

    ProcCtxt(OldDoFn<InputT, OutputT> fn,
             HeronRuntimeContext runtimeContext,
             List<PCollectionView<?>> sideInputs,
             WindowFn<Object, ?> windowFn) {
      super(fn, runtimeContext, sideInputs, windowFn);
    }

    @Override
    protected synchronized void outputWindowedValue(WindowedValue<OutputT> o) {
      outputs.add(o);
    }

    @Override
    protected <T> void sideOutputWindowedValue(TupleTag<T> tag, WindowedValue<T> output) {
      throw new UnsupportedOperationException(
          "sideOutput is an unsupported operation for doFunctions, use a "
              + "MultiDoFunction instead.");
    }

//    @Override
//    public Accumulator<NamedAggregators> getAccumulator() {
//      return accum;
//    }

    @Override
    protected void clearOutput() {
      outputs.clear();
    }

    @Override
    protected Iterator<WindowedValue<OutputT>> getOutputIterator() {
      return outputs.iterator();
    }
  }

}
