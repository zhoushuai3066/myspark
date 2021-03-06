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

public class MySparkSQLParquetSave {
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
    System.out.println("=== Data source: RDD ===");
    JavaRDD<Person> people = ctx.textFile("examples/src/main/resources/people.txt").map(
      new Function<String, Person>() {
		private static final long serialVersionUID = 1L;
		public Person call(String line) throws Exception {
          String[] parts = line.split(",");

          Person person = new Person();
          person.setName(parts[0]);
          person.setAge(Integer.parseInt(parts[1].trim()));

          return person;
        }
      });
    JavaSQLContext sqlCtx = new JavaSQLContext(ctx);
    JavaSchemaRDD schemaPeople = sqlCtx.applySchema(people, Person.class);
    schemaPeople.registerAsTable("people");
    System.out.println("=== Data source: Parquet File ===");
    schemaPeople.saveAsParquetFile("people.parquet");
  }
}
