package ru.bigdata.lab1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.bigdata.lab1.model.AnalysisResult;
import ru.bigdata.lab1.model.Station;
import ru.bigdata.lab1.model.Trip;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BikeShareSparkAnalyticsTest {

    private static JavaSparkContext sparkContext;

    @BeforeAll
    static void setUpSpark() {
        System.setProperty("io.netty.tryReflectionSetAccessible", "true");

        SparkConf sparkConf = new SparkConf()
                .setAppName("BikeShareSparkAnalyticsTest")
                .setMaster("local[2]")
                .set("spark.ui.enabled", "false")
                .set("spark.driver.host", "127.0.0.1");

        sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");
    }

    @AfterAll
    static void tearDownSpark() {
        if (sparkContext != null) {
            sparkContext.close();
        }
    }

    @Test
    void computesAllLabTasksOnDeterministicDataset() {
        JavaRDD<Station> stations = sparkContext.parallelize(List.of(
                new Station(1, "Station A", 0.0, 0.0, 10, "City", LocalDate.of(2024, 1, 1)),
                new Station(2, "Station B", 0.0, 1.0, 10, "City", LocalDate.of(2024, 1, 1)),
                new Station(3, "Station C", 1.0, 1.0, 10, "City", LocalDate.of(2024, 1, 1))
        ));

        JavaRDD<Trip> trips = sparkContext.parallelize(List.of(
                new Trip(1, 4_000, LocalDateTime.of(2024, 1, 1, 8, 0), "Station A", 1,
                        LocalDateTime.of(2024, 1, 1, 9, 0), "Station B", 2, 10, "Subscriber", "94000"),
                new Trip(2, 3_000, LocalDateTime.of(2024, 1, 1, 10, 0), "Station B", 2,
                        LocalDateTime.of(2024, 1, 1, 11, 0), "Station C", 3, 10, "Subscriber", "94001"),
                new Trip(3, 5_000, LocalDateTime.of(2024, 1, 1, 7, 0), "Station C", 3,
                        LocalDateTime.of(2024, 1, 1, 8, 0), "Station A", 1, 20, "Subscriber", "94002"),
                new Trip(4, 7_000, LocalDateTime.of(2024, 1, 1, 9, 30), "Station A", 1,
                        LocalDateTime.of(2024, 1, 1, 10, 30), "Station C", 3, 20, "Subscriber", "94000")
        ));

        BikeShareSparkAnalytics analytics = new BikeShareSparkAnalytics();
        AnalysisResult result = analytics.analyze(trips, stations, 4, 0, 3, 0);

        assertEquals(20, result.longestRunningBike().bikeId());
        assertEquals(12_000, result.longestRunningBike().totalDurationSeconds());
        assertEquals(2, result.bikeCount());
        assertIterableEquals(List.of("Station C", "Station A", "Station C"), result.longestRunningBikePath().stationPath());
        assertEquals(1, result.usersOverThreeHours().size());
        assertEquals("94000", result.usersOverThreeHours().get(0).userKey().zipCode());
        assertEquals(11_000, result.usersOverThreeHours().get(0).totalDurationSeconds());
        assertTrue(result.farthestStations().distanceKm() > 150.0);
        assertTrue(result.farthestStations().distanceKm() < 160.0);
    }
}
