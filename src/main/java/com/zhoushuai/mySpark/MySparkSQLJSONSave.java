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

public class MySparkSQLJSONSave {
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
    // Load a text file and convert each line to a Java Bean.
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
    // Apply a schema to an RDD of Java Beans and register it as a table.
    JavaSchemaRDD schemaPeople = sqlCtx.applySchema(people, Person.class);
    schemaPeople.registerAsTable("people");
    // SQL can be run over RDDs that have been registered as tables.
    JavaSchemaRDD teenagers = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    List<String> teenagerNames = teenagers.map(new Function<Row, String>() {
		private static final long serialVersionUID = 1L;

	public String call(Row row) {
        return "Name: " + row.getString(0);
      }
    }).collect();
    for (String name: teenagerNames) {
      System.out.println(name);
    }

    System.out.println("=== Data source: Parquet File ===");
    // JavaSchemaRDDs can be saved as parquet files, maintaining the schema information.
    schemaPeople.saveAsParquetFile("people.parquet");

    // Read in the parquet file created above.
    // Parquet files are self-describing so the schema is preserved.
    // The result of loading a parquet file is also a JavaSchemaRDD.
    JavaSchemaRDD parquetFile = sqlCtx.parquetFile("people.parquet");
    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerAsTable("parquetFile");
    JavaSchemaRDD teenagers2 =
      sqlCtx.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
    teenagerNames = teenagers2.map(new Function<Row, String>() {
		private static final long serialVersionUID = 1L;

	public String call(Row row) {
          return "Name: " + row.getString(0);
      }
    }).collect();
    for (String name: teenagerNames) {
      System.out.println(name);
    }

    System.out.println("=== Data source: JSON Dataset ===");
    // A JSON dataset is pointed by path.
    // The path can be either a single text file or a directory storing text files.
    String path = "examples/src/main/resources/people.json";
    // Create a JavaSchemaRDD from the file(s) pointed by path
    JavaSchemaRDD peopleFromJsonFile = sqlCtx.jsonFile(path);

    // Because the schema of a JSON dataset is automatically inferred, to write queries,
    // it is better to take a look at what is the schema.
    peopleFromJsonFile.printSchema();
    // The schema of people is ...
    // root
    //  |-- age: IntegerType
    //  |-- name: StringType

    // Register this JavaSchemaRDD as a table.
    peopleFromJsonFile.registerAsTable("people");

    // SQL statements can be run by using the sql methods provided by sqlCtx.
    JavaSchemaRDD teenagers3 = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

    // The results of SQL queries are JavaSchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    teenagerNames = teenagers3.map(new Function<Row, String>() {
		private static final long serialVersionUID = 1L;

	public String call(Row row) { return "Name: " + row.getString(0); }
    }).collect();
    for (String name: teenagerNames) {
      System.out.println(name);
    }

    // Alternatively, a JavaSchemaRDD can be created for a JSON dataset represented by
    // a RDD[String] storing one JSON object per string.
    List<String> jsonData = Arrays.asList(
          "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
    JavaRDD<String> anotherPeopleRDD = ctx.parallelize(jsonData);
    JavaSchemaRDD peopleFromJsonRDD = sqlCtx.jsonRDD(anotherPeopleRDD);

    // Take a look at the schema of this new JavaSchemaRDD.
    peopleFromJsonRDD.printSchema();
    // The schema of anotherPeople is ...
    // root
    //  |-- address: StructType
    //  |    |-- city: StringType
    //  |    |-- state: StringType
    //  |-- name: StringType

    peopleFromJsonRDD.registerAsTable("people2");

    JavaSchemaRDD peopleWithCity = sqlCtx.sql("SELECT name, address.city FROM people2");
    List<String> nameAndCity = peopleWithCity.map(new Function<Row, String>() {
      public String call(Row row) {
        return "Name: " + row.getString(0) + ", City: " + row.getString(1);
      }
    }).collect();
    for (String name: nameAndCity) {
      System.out.println(name);
    }
  }
}
