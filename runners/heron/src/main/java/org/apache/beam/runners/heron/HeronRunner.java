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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.PipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prototype Heron runner. To build and install do:
 *
 *   (cd runners/heron && mvn install)
 *
 * To run wordcount:
 *
 *   cd examples/java
 *   mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
 *     -Dexec.args="--runner=HeronRunner --inputFile=pom.xml --output=counts" -Pheron-runner
 *
 * To debug, use mvnDebug instead.
 *
 */
public final class HeronRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(HeronRunner.class);

  private HeronPipelineOptions options;

  private HeronRunner(HeronPipelineOptions heronOptions) {
    options = heronOptions;
  }

  /**
   * Creates and returns a new HeronRunner with specified options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static HeronRunner fromOptions(PipelineOptions options) {
    HeronPipelineOptions heronOptions =
        PipelineOptionsValidator.validate(HeronPipelineOptions.class, options);
    return new HeronRunner(heronOptions);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    LOG.info("Running HeronRunner");
    return new HeronPipelineResult();
  }
}