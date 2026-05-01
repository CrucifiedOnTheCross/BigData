package ru.bigdata.lab2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.bigdata.lab2.model.LanguageAlias;
import ru.bigdata.lab2.model.StackOverflowPost;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class Lab2Application {

    private Lab2Application() {
    }

    public static void main(String[] args) {
        AppConfig config = AppConfig.fromArgs(args);
        SparkSession spark = createSparkSession("Lab2ProgrammingLanguagesReport", config.master());

        spark.sparkContext().setLogLevel("WARN");

        try {
            Dataset<StackOverflowPost> posts = StackOverflowXmlReader.readPosts(spark, config.postsPath());
            Dataset<LanguageAlias> languageAliases = LanguageDictionary.readAliases(spark, config.languagesPath());

            PopularityReportService reportService = new PopularityReportService();
            Dataset<Row> report = reportService.buildTopLanguagesReport(posts, languageAliases);

            report.write().mode("overwrite").parquet(config.outputPath().toAbsolutePath().normalize().toString());

            System.out.println("Saved parquet report to: " + config.outputPath().toAbsolutePath().normalize());
            report.orderBy("year", "rank").show(200, false);
        } finally {
            spark.stop();
        }
    }

    static SparkSession createSparkSession(String appName, String master) {
        System.setProperty("io.netty.tryReflectionSetAccessible", "true");

        return SparkSession.builder()
                .appName(appName)
                .master(master)
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.hadoop.fs.file.impl", NoOpPermissionLocalFileSystem.class.getName())
                .getOrCreate();
    }

    record AppConfig(String master, Path postsPath, Path languagesPath, Path outputPath) {
        private static final String DEFAULT_MASTER = "local[*]";

        static AppConfig fromArgs(String[] args) {
            if (args.length == 0) {
                return new AppConfig(
                        DEFAULT_MASTER,
                        Paths.get("..", "..", "data", "posts_sample.xml"),
                        Paths.get("..", "..", "data", "programming-languages.csv"),
                        Paths.get("target", "language-report-parquet")
                );
            }

            if (args.length == 3) {
                return new AppConfig(
                        DEFAULT_MASTER,
                        Paths.get(args[0]),
                        Paths.get(args[1]),
                        Paths.get(args[2])
                );
            }

            if (args.length == 4) {
                return new AppConfig(
                        args[0],
                        Paths.get(args[1]),
                        Paths.get(args[2]),
                        Paths.get(args[3])
                );
            }

            throw new IllegalArgumentException("""
                    Usage:
                      1. no args -> run locally with repository sample data
                      2. <postsXmlPath> <languagesCsvPath> <outputParquetPath>
                      3. <master> <postsXmlPath> <languagesCsvPath> <outputParquetPath>
                    """.stripIndent());
        }
    }
}
