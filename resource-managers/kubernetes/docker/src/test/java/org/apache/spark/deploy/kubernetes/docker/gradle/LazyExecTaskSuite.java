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
package org.apache.spark.deploy.kubernetes.docker.gradle;

import org.gradle.api.Project;
import org.gradle.api.provider.ListProperty;
import org.gradle.process.ExecSpec;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

public final class LazyExecTaskSuite {

  @Mock
  private LazyExecTask taskUnderTest;

  @Mock
  private Project project;

  @Mock
  private ExecSpec execSpec;

  @Mock
  private ListProperty<String> commandLine;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    ProjectExecUtils.invokeExecSpecAction(project, execSpec);
    Mockito.when(taskUnderTest.getCommandLine()).thenReturn(commandLine);
    Mockito.when(taskUnderTest.getProject()).thenReturn(project);
    Mockito.doCallRealMethod().when(taskUnderTest).runCommand();
  }

  @Test
  public void testRunCOmmand_setsCommandLineOnExecSpec() {
    List<String> command = new ArrayList<>();
    command.add("ls");
    command.add("-lahrt");
    command.add("git");
    Mockito.when(commandLine.get()).thenReturn(command);
    taskUnderTest.runCommand();
    Mockito.verify(execSpec).commandLine(command);
  }

}