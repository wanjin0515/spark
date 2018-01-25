import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class SparkSql {

    public static void main(String[] args){
        //创建一个基本的SparkSession,它是使用所有Spark SQL功能的入口点
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[*]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        //读取json文件，显示DataFrame内容
        Dataset<Row> df = spark.read().json("E:/workspace/bigdata/spark/spark/examples/src/main/resources/people.json");
        df.show();
        // 树形格式打印schema
        df.printSchema();
        // 选择“name”列
        df.select("name").show();
        // 选择所有数据, 但对“age”列执行+1操作
        df.select(col("name"), col("age").plus(1)).show();
        // 选择年龄“age”大于21的people
        df.filter(col("age").gt(21)).show();
        // 根据年龄“age”分组并计数
        df.groupBy("age").count().show();
        // 注册DataFrame为一个SQL的临时视图
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
        // 通过一个文本文件创建Person对象的RDD
        JavaRDD<People> peopleRDD = spark.read()
                .textFile("E:/workspace/bigdata/spark/spark/examples/src/main/resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    People person = new People();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });
        // 对JavaBeans的RDD指定schema得到DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, People.class);
        // 注册该DataFrame为临时视图
        peopleDF.createOrReplaceTempView("people");

        // 执行SQL语句
        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        // 结果中的列可以通过属性的下标获取
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                row -> "Name: " + row.getString(0),
                stringEncoder);
        teenagerNamesByIndexDF.show();
        // 或者通过属性的名字获取
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();
    }
}
