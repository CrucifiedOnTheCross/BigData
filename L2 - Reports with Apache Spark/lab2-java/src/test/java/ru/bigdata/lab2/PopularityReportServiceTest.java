package ru.bigdata.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.bigdata.lab2.model.LanguageAlias;
import ru.bigdata.lab2.model.StackOverflowPost;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PopularityReportServiceTest {

    private static SparkSession spark;

    @BeforeAll
    static void setUpSpark() {
        spark = Lab2Application.createSparkSession("PopularityReportServiceTest", "local[2]");
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void buildsYearlyTop10UsingOnlyQuestionsAndDistinctPostLanguagePairs() {
        Dataset<StackOverflowPost> posts = spark.createDataset(List.of(
                post(1L, 1, 2010, "<java><spring>"),
                post(2L, 1, 2010, "<java><c#>"),
                post(3L, 1, 2010, "<c#>"),
                post(4L, 2, 2010, "<java>"),
                post(5L, 1, 2011, "<python><go>"),
                post(6L, 1, 2011, "<python>")
        ), Encoders.bean(StackOverflowPost.class));

        Dataset<LanguageAlias> aliases = spark.createDataset(List.of(
                new LanguageAlias("Java", "java"),
                new LanguageAlias("C#", "c#"),
                new LanguageAlias("Python", "python"),
                new LanguageAlias("Go", "go")
        ), Encoders.bean(LanguageAlias.class));

        PopularityReportService service = new PopularityReportService();
        List<Row> report = service.buildTopLanguagesReport(posts, aliases).collectAsList();

        assertEquals(4, report.size());

        Row first2010 = report.get(0);
        assertEquals(Integer.valueOf(2010), first2010.getAs("year"));
        assertEquals(Integer.valueOf(1), first2010.getAs("rank"));
        assertEquals("C#", first2010.getAs("language"));
        assertEquals(Long.valueOf(2L), first2010.getAs("questionsCount"));

        Row second2010 = report.get(1);
        assertEquals(Integer.valueOf(2010), second2010.getAs("year"));
        assertEquals(Integer.valueOf(2), second2010.getAs("rank"));
        assertEquals("Java", second2010.getAs("language"));
        assertEquals(Long.valueOf(2L), second2010.getAs("questionsCount"));

        Row first2011 = report.get(2);
        assertEquals(Integer.valueOf(2011), first2011.getAs("year"));
        assertEquals(Integer.valueOf(1), first2011.getAs("rank"));
        assertEquals("Python", first2011.getAs("language"));
        assertEquals(Long.valueOf(2L), first2011.getAs("questionsCount"));
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
