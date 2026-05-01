package ru.bigdata.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.bigdata.lab2.model.LanguageAlias;
import ru.bigdata.lab2.model.StackOverflowPost;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Lab2ParquetIntegrationTest {

    private static SparkSession spark;

    @BeforeAll
    static void setUpSpark() {
        spark = Lab2Application.createSparkSession("Lab2ParquetIntegrationTest", "local[2]");
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void writesParquetReportToLocalFilesystem(@TempDir Path tempDir) {
        Dataset<StackOverflowPost> posts = spark.createDataset(List.of(
                post(1L, 1, 2010, "<java><spring>"),
                post(2L, 1, 2010, "<java>"),
                post(3L, 1, 2011, "<python>")
        ), Encoders.bean(StackOverflowPost.class));

        Dataset<LanguageAlias> aliases = spark.createDataset(List.of(
                new LanguageAlias("Java", "java"),
                new LanguageAlias("Python", "python")
        ), Encoders.bean(LanguageAlias.class));

        Dataset<Row> report = new PopularityReportService().buildTopLanguagesReport(posts, aliases);
        Path outputPath = tempDir.resolve("report-parquet");

        report.write().mode("overwrite").parquet(outputPath.toString());

        List<Row> savedRows = spark.read().parquet(outputPath.toString())
                .orderBy("year", "rank")
                .collectAsList();

        assertEquals(2, savedRows.size());
        assertEquals("Java", savedRows.get(0).getAs("language"));
        assertEquals(Long.valueOf(2L), savedRows.get(0).getAs("questionsCount"));
        assertEquals("Python", savedRows.get(1).getAs("language"));
    }

    private static StackOverflowPost post(long id, int postTypeId, int year, String tags) {
        StackOverflowPost post = new StackOverflowPost();
        post.setId(id);
        post.setPostTypeId(postTypeId);
        post.setCreationDate(LocalDateTime.of(year, 1, 1, 0, 0));
        post.setYear(year);
        post.setTags(tags);
        post.setTitle("Post " + id);
        return post;
    }
}
