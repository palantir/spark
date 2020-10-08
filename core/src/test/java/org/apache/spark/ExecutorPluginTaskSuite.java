/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark;

import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExecutorPluginTaskSuite {
  private static final String EXECUTOR_PLUGIN_CONF_NAME = "spark.executor.plugins";
  private static final String taskWellBehavedPluginName = TestWellBehavedPlugin.class.getName();
  private static final String taskBadlyBehavedPluginName = TestBadlyBehavedPlugin.class.getName();

  // Static value modified by testing plugins to ensure plugins are called correctly.
  public static int numOnTaskStart = 0;
  public static int numOnTaskSucceeded = 0;
  public static int numOnTaskFailed = 0;

  private JavaSparkContext sc;

  @Before
  public void setUp() {
    sc = null;
    numOnTaskStart = 0;
    numOnTaskSucceeded = 0;
    numOnTaskFailed = 0;
  }

  @After
  public void tearDown() {
    if (sc != null) {
      sc.stop();
      sc = null;
    }
  }

  private SparkConf initializeSparkConf(String pluginNames) {
    return new SparkConf()
        .setMaster("local")
        .setAppName("test")
        .set(EXECUTOR_PLUGIN_CONF_NAME, pluginNames);
  }

  @Test
  public void testWellBehavedPlugin() {
    SparkConf conf = initializeSparkConf(taskWellBehavedPluginName);

    sc = new JavaSparkContext(conf);
    JavaRDD<Integer> rdd = sc.parallelize(ImmutableList.of(1, 2));
    rdd.filter(value -> value.equals(1)).collect();

    assertEquals(numOnTaskStart, 1);
    assertEquals(numOnTaskSucceeded, 1);
    assertEquals(numOnTaskFailed, 0);
  }

  @Test
  public void testBadlyBehavedPluginDoesNotAffectWellBehavedPlugin() {
    SparkConf conf = initializeSparkConf(
            taskWellBehavedPluginName + "," + taskBadlyBehavedPluginName);

    sc = new JavaSparkContext(conf);
    JavaRDD<Integer> rdd = sc.parallelize(ImmutableList.of(1, 2));
    rdd.filter(value -> value.equals(1)).collect();

    assertEquals(numOnTaskStart, 1);
    assertEquals(numOnTaskSucceeded, 2);
    assertEquals(numOnTaskFailed, 0);
  }

  @Test
  public void testTaskWhichFails() {
    SparkConf conf = initializeSparkConf(taskWellBehavedPluginName);

    sc = new JavaSparkContext(conf);
    JavaRDD<Integer> rdd = sc.parallelize(ImmutableList.of(1, 2));
    try {
      rdd.foreach(integer -> {
        throw new RuntimeException();
      });
    } catch (Exception e) {
      // ignore exception
    }

    assertEquals(numOnTaskStart, 1);
    assertEquals(numOnTaskSucceeded, 0);
    assertEquals(numOnTaskFailed, 1);
  }

  public static class TestWellBehavedPlugin implements ExecutorPlugin {
    @Override
    public void onTaskStart() {
      numOnTaskStart++;
    }

    @Override
    public void onTaskSucceeded() {
      numOnTaskSucceeded++;
    }

    @Override
    public void onTaskFailed(Throwable throwable) {
      numOnTaskFailed++;
    }
  }

  public static class TestBadlyBehavedPlugin implements ExecutorPlugin {
    @Override
    public void onTaskStart() {
      throw new RuntimeException();
    }

    @Override
    public void onTaskSucceeded() {
      numOnTaskSucceeded++;
    }

    @Override
    public void onTaskFailed(Throwable throwable) {
      numOnTaskFailed++;
    }
  }
}
