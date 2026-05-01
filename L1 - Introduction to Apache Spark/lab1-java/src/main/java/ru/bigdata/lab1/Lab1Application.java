package ru.bigdata.lab1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.bigdata.lab1.model.AnalysisResult;
import ru.bigdata.lab1.model.Station;
import ru.bigdata.lab1.model.Trip;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class Lab1Application {

    private Lab1Application() {
    }

    public static void main(String[] args) {
        AppConfig config = AppConfig.fromArgs(args);

        SparkConf sparkConf = new SparkConf()
                .setAppName("Lab1BikeShareJava")
                .setMaster(config.master())
                .set("spark.driver.host", "127.0.0.1");

        System.setProperty("io.netty.tryReflectionSetAccessible", "true");

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            sparkContext.setLogLevel("WARN");

            JavaRDD<String> tripRows = readDataRows(sparkContext, config.tripsPath()).cache();
            long totalTripRows = tripRows.count();
            JavaRDD<Trip> trips = tripRows.map(CsvParsers::parseTrip)
                    .filter(trip -> trip != null)
                    .cache();
            long validTrips = trips.count();

            JavaRDD<String> stationRows = readDataRows(sparkContext, config.stationsPath()).cache();
            long totalStationRows = stationRows.count();
            JavaRDD<Station> stations = stationRows.map(CsvParsers::parseStation)
                    .filter(station -> station != null)
                    .cache();
            long validStations = stations.count();

            BikeShareSparkAnalytics analytics = new BikeShareSparkAnalytics();
            AnalysisResult result = analytics.analyze(
                    trips,
                    stations,
                    validTrips,
                    totalTripRows - validTrips,
                    validStations,
                    totalStationRows - validStations
            );

            System.out.println(ReportFormatter.format(result));
        }
    }

    private static JavaRDD<String> readDataRows(JavaSparkContext sparkContext, Path filePath) {
        Path normalized = filePath.toAbsolutePath().normalize();
        if (!Files.exists(normalized)) {
            throw new IllegalArgumentException("Input file was not found: " + normalized);
        }

        return sparkContext.textFile(normalized.toUri().toString())
                .zipWithIndex()
                .filter(tuple -> tuple._2 > 0)
                .keys();
    }

    record AppConfig(String master, Path tripsPath, Path stationsPath) {
        private static final String DEFAULT_MASTER = "local[*]";

        static AppConfig fromArgs(String[] args) {
            if (args.length == 0) {
                return new AppConfig(
                        DEFAULT_MASTER,
                        Paths.get("..", "..", "data", "trips.csv"),
                        Paths.get("..", "..", "data", "stations.csv")
                );
            }

            if (args.length == 2) {
                return new AppConfig(DEFAULT_MASTER, Paths.get(args[0]), Paths.get(args[1]));
            }

            if (args.length == 3) {
                return new AppConfig(args[0], Paths.get(args[1]), Paths.get(args[2]));
            }

            throw new IllegalArgumentException("""
                    Usage:
                      1. no args -> run locally with repository data
                      2. <tripsPath> <stationsPath>
                      3. <master> <tripsPath> <stationsPath>
                    """.stripIndent());
        }
    }
}
