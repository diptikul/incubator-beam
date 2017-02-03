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
package org.apache.beam.sdk.io.hadoop.inputformat;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.inputformat.testing.HIFIOTextMatcher;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Tests to validate HadoopInputFormatIO for embedded Elasticsearch instance.
 *
 * {@link EsInputFormat} can be used to read data from Elasticsearch. EsInputFormat by default
 * returns key class as Text and value class as LinkedMapWritable. If you want to get MapWritable as
 * value type then you must set property "mapred.mapoutput.value.class" to MapWritable.class.
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOWithElasticTest implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(HIFIOWithElasticTest.class);
  private static final String ELASTIC_IN_MEM_HOSTNAME = "127.0.0.1";
  private static final String ELASTIC_IN_MEM_PORT = "9200";
  private static final String ELASTIC_INTERNAL_VERSION = "5.x";
  private static final String TRUE = "true";
  private static final String ELASTIC_INDEX_NAME = "beamdb";
  private static final String ELASTIC_TYPE_NAME = "scientists";
  private static final String ELASTIC_RESOURCE = "/" + ELASTIC_INDEX_NAME + "/" + ELASTIC_TYPE_NAME;
  private static final int TEST_DATA_ROW_COUNT = 10;
  private static final String ELASTIC_TYPE_ID_PREFIX = "s";
  private static List<String> expectedList = new ArrayList<>();
  private static final String OUTPUT_WRITE_FILE_PATH = "D:\\op";

  @ClassRule
  public static TemporaryFolder elasticTempFolder = new TemporaryFolder();

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void startServer() throws NodeValidationException, InterruptedException,
      IOException {
    ElasticEmbeddedServer.startElasticEmbeddedServer();
  }

  /**
   * Test to read data from embedded Elasticsearch instance and verify whether data is read
   * successfully.
   */
  @Test
  public void testHifIOWithElastic() {
    Configuration conf = getConfiguration();

    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, LinkedMapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) TEST_DATA_ROW_COUNT);
    PCollection<LinkedMapWritable> values = esData.apply(Values.<LinkedMapWritable>create());
    MapElements<LinkedMapWritable, String> transformFunc =
        MapElements.<LinkedMapWritable, String>via(new SimpleFunction<LinkedMapWritable, String>() {
          @Override
          public String apply(LinkedMapWritable mapw) {
            return mapw.get(new Text("id")) + "|" + mapw.get(new Text("scientist"));
          }
        });

    PCollection<String> textValues = values.apply(transformFunc);

    // Write Pcollection of Strings to a file using TextIO Write transform.
    textValues.apply(TextIO.Write.to(OUTPUT_WRITE_FILE_PATH).withNumShards(1).withSuffix("txt"));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // Verify the output values using checksum comparison
    HIFIOTextMatcher matcher =
        new HIFIOTextMatcher(OUTPUT_WRITE_FILE_PATH + "-00000-of-00001.txt", expectedList);
    assertThat(result, matcher);
  }

  /**
   * Test to read data from embedded Elasticsearch instance based on query and verify whether data
   * is read successfully.
   */
  @Test
  public void testHifIOWithElasticQuery() {
    Configuration conf = getConfiguration();
    String fieldValue = ELASTIC_TYPE_ID_PREFIX + "2";
    String query =
        "{\n"
            + "  \"query\": {\n"
            + "  \"match\" : {\n"
            + "    \"id\" : {\n"
            + "      \"query\" : \"" + fieldValue + "" + "\",\n"
            + "      \"type\" : \"boolean\"\n"
            + "    }\n"
            + "  }\n"
            + "  }\n"
            + "}";
    conf.set(ConfigurationOptions.ES_QUERY, query);
    PCollection<KV<Text, LinkedMapWritable>> esData =
        pipeline.apply(HadoopInputFormatIO.<Text, LinkedMapWritable>read().withConfiguration(conf));
    PCollection<Long> count = esData.apply(Count.<KV<Text, LinkedMapWritable>>globally());
    PAssert.thatSingleton(count).isEqualTo((long) 1);

    pipeline.run().waitUntilFinish();
  }

  public static Map<String, String> populateElasticData(String id, String name) {
    Map<String, String> data = new HashMap<String, String>();
    data.put("id", id);
    data.put("scientist", name);
    return data;
  }

  @AfterClass
  public static void shutdownServer() throws IOException {
    ElasticEmbeddedServer.shutdown();
  }

  /**
   * Class for in memory Elasticsearch server.
   */
  static class ElasticEmbeddedServer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static Node node;

    public static void startElasticEmbeddedServer() throws UnknownHostException,
        NodeValidationException, InterruptedException {

      Settings settings =
          Settings.builder().put("node.data", TRUE).put("network.host", ELASTIC_IN_MEM_HOSTNAME)
              .put("http.port", ELASTIC_IN_MEM_PORT)
              .put("path.data", elasticTempFolder.getRoot().getPath())
              .put("path.home", elasticTempFolder.getRoot().getPath())
              .put("transport.type", "local").put("http.enabled", TRUE).put("node.ingest", TRUE)
              .build();
      node = new PluginNode(settings);
      node.start();
      LOGGER.info("Elastic im memory server started..");
      prepareElasticIndex();
      LOGGER.info("Prepared index " + ELASTIC_INDEX_NAME
          + "and populated data on elastic in memory server..");
    }

    private static void prepareElasticIndex() throws InterruptedException {
      CreateIndexRequest indexRequest = new CreateIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().create(indexRequest).actionGet();
      for (int i = 0; i < TEST_DATA_ROW_COUNT; i++) {
        node.client().prepareIndex(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, String.valueOf(i))
            .setSource(populateElasticData(ELASTIC_TYPE_ID_PREFIX + i, "Faraday")).execute();
        expectedList.add(ELASTIC_TYPE_ID_PREFIX + i + "|" + "Faraday");
        Thread.sleep(100);
      }
      GetResponse response =
          node.client().prepareGet(ELASTIC_INDEX_NAME, ELASTIC_TYPE_NAME, "1").execute()
              .actionGet();

    }

    public Client getClient() throws UnknownHostException {
      return node.client();
    }

    public static void shutdown() throws IOException {
      DeleteIndexRequest indexRequest = new DeleteIndexRequest(ELASTIC_INDEX_NAME);
      node.client().admin().indices().delete(indexRequest).actionGet();
      LOGGER.info("Deleted index " + ELASTIC_INDEX_NAME + " from elastic in memory server");
      node.close();
      LOGGER.info("Closed elastic in memory server node.");
      deleteElasticDataDirectory();
    }

    private static void deleteElasticDataDirectory() {
      try {
        FileUtils.deleteDirectory(new File(elasticTempFolder.getRoot().getPath()));
      } catch (IOException e) {
        throw new RuntimeException("Exception: Could not delete elastic data directory", e);
      }
    }

  }

  /**
   * Class created for handling "http.enabled" property as "true" for Elasticsearch node.
   */
  static class PluginNode extends Node implements Serializable {

    private static final long serialVersionUID = 1L;
    static Collection<Class<? extends Plugin>> list = new ArrayList<Class<? extends Plugin>>();
    static {
      list.add(Netty4Plugin.class);
    }

    public PluginNode(final Settings settings) {
      super(InternalSettingsPreparer.prepareEnvironment(settings, null), list);

    }
  }

  /**
   * Set the Elasticsearch configuration parameters in the Hadoop configuration object.
   * Configuration object should have InputFormat class, key class and value class to be set.
   * Mandatory fields for ESInputFormat to be set are es.resource, es.nodes, es.port,
   * es.internal.es.version. Please refer <a
   * href="https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html"
   * >Elasticsearch Configuration</a> for more details.
   */
  public Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(ConfigurationOptions.ES_NODES, ELASTIC_IN_MEM_HOSTNAME);
    conf.set(ConfigurationOptions.ES_PORT, String.format("%s", ELASTIC_IN_MEM_PORT));
    conf.set(ConfigurationOptions.ES_RESOURCE, ELASTIC_RESOURCE);
    conf.set("es.internal.es.version", ELASTIC_INTERNAL_VERSION);
    conf.set(ConfigurationOptions.ES_NODES_DISCOVERY, TRUE);
    conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, TRUE);
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        org.elasticsearch.hadoop.mr.EsInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, Text.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, LinkedMapWritable.class, Object.class);
    return conf;
  }
}