/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.cdc.postgres.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.jcustenborder.kafka.connect.cdc.Change;
import com.github.jcustenborder.kafka.connect.cdc.NamedTest;
import com.github.jcustenborder.kafka.connect.cdc.ObjectMapperFactory;
import com.github.jcustenborder.kafka.connect.cdc.TableMetadataProvider;
import org.apache.kafka.common.utils.Time;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PostgreSQLChangeTestCase implements NamedTest {
  public String location;
  public long xid;
  public String data;
  public Time time;
  public TableMetadataProvider.TableMetadata tableMetadata;
  public Change expected;
  @JsonIgnore
  private String name;

  public static void write(File file, PostgreSQLChangeTestCase change) throws IOException {
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ObjectMapperFactory.INSTANCE.writeValue(outputStream, change);
    }
  }

  public static void write(OutputStream outputStream, PostgreSQLChangeTestCase change) throws IOException {
    ObjectMapperFactory.INSTANCE.writeValue(outputStream, change);
  }

  public static PostgreSQLChangeTestCase read(InputStream inputStream) throws IOException {
    return ObjectMapperFactory.INSTANCE.readValue(inputStream, PostgreSQLChangeTestCase.class);
  }

  public static PostgreSQLChangeTestCase read(File inputFile) throws IOException {
    try (FileInputStream inputStream = new FileInputStream(inputFile)) {
      return ObjectMapperFactory.INSTANCE.readValue(inputStream, PostgreSQLChangeTestCase.class);
    }
  }

  @Override
  public void name(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return this.name;
  }
}
