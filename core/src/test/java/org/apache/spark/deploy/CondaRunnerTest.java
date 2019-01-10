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
package org.apache.spark.deploy;


import org.apache.spark.SparkConf;
import org.junit.Test;
import scala.collection.immutable.Map;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CondaRunnerTest {

    @Test
    public void testReadingEnvVariables() throws IOException {
        SparkConf conf = new SparkConf();
        conf.set("spark.conda.envVars", "key1=value1,key2=value2");
        Map.Map2 expected = new Map.Map2<>("key1", "value1", "key2", "value2");
        assertThat(CondaRunner.extractEnvVariables(conf), is(expected));
    }
}
