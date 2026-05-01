package ru.bigdata.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import ru.bigdata.lab2.model.LanguageAlias;
import ru.bigdata.lab2.model.StackOverflowPost;

import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.split;

public final class PopularityReportService {

    public Dataset<Row> buildTopLanguagesReport(
            Dataset<StackOverflowPost> posts,
            Dataset<LanguageAlias> languageAliases
    ) {
        Dataset<Row> questionTags = posts
                .filter(col("postTypeId").equalTo(1)
                        .and(col("year").geq(2010))
                        .and(col("year").leq(2020))
                        .and(col("tags").isNotNull())
                        .and(col("tags").notEqual("")))
                .withColumn("tag", explode(
                        split(
                                regexp_replace(regexp_replace(col("tags"), "&lt;|<", ""), "&gt;|>", ","),
                                ","
                        )))
                .filter(col("tag").notEqual(""));

        Dataset<Row> matchedLanguages = questionTags
                .join(languageAliases.toDF(),
                        questionTags.col("tag").equalTo(languageAliases.col("alias")),
                        "inner")
                .select(
                        questionTags.col("id"),
                        questionTags.col("year"),
                        languageAliases.col("canonicalName").alias("language")
                )
                .distinct();

        Dataset<Row> yearlyCounts = matchedLanguages
                .groupBy(col("year"), col("language"))
                .agg(countDistinct("id").alias("questionsCount"));

        WindowSpec rankingWindow = Window.partitionBy("year")
                .orderBy(col("questionsCount").desc(), asc("language"));

        return yearlyCounts
                .withColumn("rank", row_number().over(rankingWindow))
                .filter(col("rank").leq(10))
                .select("year", "rank", "language", "questionsCount")
                .orderBy("year", "rank");
    }
}
