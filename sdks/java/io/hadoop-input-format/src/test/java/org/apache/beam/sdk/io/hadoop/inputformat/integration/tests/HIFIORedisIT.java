/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.integration.tests;

import java.io.Serializable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.custom.options.HIFTestOptions;
import org.apache.beam.sdk.io.hadoop.inputformat.redis.inputformat.RedisInputDriver;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

/**
 * Runs integration test to validate HadoopInputFromatIO for a redis instance.
 * You need to pass redis server IP in beamTestPipelineOptions.
 *
 * <p>
 * You can run just this test by doing the following: mvn test-compile compile
 * failsafe:integration-test -D beamTestPipelineOptions='[ "--serverIp=1.2.3.4" ]'
 * -Dit.test=HIFIORedisIT -DskipITs=false
 */

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIORedisIT implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final String REDIS_HASH_KEY_CONF = "scientists";
  private static HIFTestOptions options;

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(HIFTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HIFTestOptions.class);
  }

  /**
   * This test reads data from the redis instance and verifies whether data is read successfully.
   */
  @Test
  public void testReadData() {
    Pipeline p = TestPipeline.create(options);
    Configuration conf = getConfiguration(options);

    PCollection<KV<Text, Text>> redisData =
        p.apply(HadoopInputFormatIO.<Text, Text>read().withConfiguration(conf));
    PAssert.thatSingleton(redisData.apply("Count", Count.<KV<Text, Text>>globally()))
        .isEqualTo(10L);

    p.run();
  }

  /**
   * Returns configuration of RedisHashInputFormat. Mandatory parameters required apart from
   * inputformat class name, key class, value class are hash key, host ip address.
   */
  private Configuration getConfiguration(HIFTestOptions options) {
    Configuration conf = new Configuration();
    conf.set("mapred.redishashinputformat.hosts", options.getServerIp());
    conf.set("mapred.redishashinputformat.key", REDIS_HASH_KEY_CONF);

    conf.setClass("mapreduce.job.inputformat.class", RedisInputDriver.RedisHashInputFormat.class,
        InputFormat.class);
    conf.setClass("key.class", org.apache.hadoop.io.Text.class, Object.class);
    conf.setClass("value.class", org.apache.hadoop.io.Text.class, Object.class);
    return conf;
  }

}
