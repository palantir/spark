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

package org.apache.spark.palantir.shuffle.async.util.keys;

import com.google.common.base.Throwables;

import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public final class KeyPairs {
  private KeyPairs() {
  }

  public static KeyPair generateKeyPair(String algorithm, int keysize) {
    try {
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(algorithm);
      keyPairGenerator.initialize(keysize);
      return keyPairGenerator.generateKeyPair();
    } catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }
  }

  public static String serializeKey(Key key) {
    return Base64.getEncoder().encodeToString(key.getEncoded());
  }
}
