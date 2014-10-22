package com.zhoushuai.mySpark;

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


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

public class MySparkSQLParquetLoad {
  public static class Person implements Serializable {
	private static final long serialVersionUID = 1L;
	private String name;
    private int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaSQLContext sqlCtx = new JavaSQLContext(ctx);
    JavaSchemaRDD parquetFile = sqlCtx.parquetFile("people.parquet");
    parquetFile.registerAsTable("parquetFile");
    JavaSchemaRDD teenagers2 = sqlCtx.sql("SELECT name FROM parquetFile left join (select parquetFile.name name2 from parquetFile group by parquetFile.name) p2 on parquetFile.name = p2.name2");
    System.out.println("=== Data source: JSON Dataset ===");
    List<String> teenagerNames = teenagers2.map(new Function<Row, String>() {
		private static final long serialVersionUID = 1L;

	public String call(Row row) {
          return "Name: " + row.getString(0);
      }
    }).collect();
    for (String name: teenagerNames) {
      System.out.println(name);
    }
  }
}
