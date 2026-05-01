package ru.bigdata.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import ru.bigdata.lab2.model.StackOverflowPost;

import java.nio.file.Files;
import java.nio.file.Path;

public final class StackOverflowXmlReader {

    private StackOverflowXmlReader() {
    }

    public static Dataset<StackOverflowPost> readPosts(SparkSession spark, Path xmlPath) {
        Path normalized = xmlPath.toAbsolutePath().normalize();
        if (!Files.exists(normalized)) {
            throw new IllegalArgumentException("Input file was not found: " + normalized);
        }

        JavaRDD<String> rows = spark.read()
                .textFile(normalized.toUri().toString())
                .javaRDD()
                .map(String::trim)
                .filter(line -> line.startsWith("<row "));

        JavaRDD<StackOverflowPost> posts = rows.map(StackOverflowXmlParser::parse)
                .filter(post -> post != null);

        return spark.createDataset(posts.rdd(), Encoders.bean(StackOverflowPost.class));
    }
}
