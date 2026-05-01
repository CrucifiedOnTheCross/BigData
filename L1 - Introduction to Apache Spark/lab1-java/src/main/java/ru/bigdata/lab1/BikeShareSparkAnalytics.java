package ru.bigdata.lab1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import ru.bigdata.lab1.model.AnalysisResult;
import ru.bigdata.lab1.model.BikePath;
import ru.bigdata.lab1.model.BikeRideDuration;
import ru.bigdata.lab1.model.Station;
import ru.bigdata.lab1.model.StationDistance;
import ru.bigdata.lab1.model.Trip;
import ru.bigdata.lab1.model.UserDuration;
import ru.bigdata.lab1.model.UserKey;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public final class BikeShareSparkAnalytics {

    public AnalysisResult analyze(
            JavaRDD<Trip> trips,
            JavaRDD<Station> stations,
            long validTrips,
            long skippedTrips,
            long validStations,
            long skippedStations
    ) {
        BikeRideDuration longestRunningBike = findBikeWithMaxRideTime(trips);
        StationDistance farthestStations = findMaxGeodesicDistance(stations);
        BikePath bikePath = findBikePath(trips, longestRunningBike);
        long bikeCount = trips.map(Trip::bikeId).distinct().count();
        List<UserDuration> usersOverThreeHours = findUsersOverThreeHours(trips);

        return new AnalysisResult(
                longestRunningBike,
                farthestStations,
                bikePath,
                bikeCount,
                usersOverThreeHours,
                validTrips,
                skippedTrips,
                validStations,
                skippedStations
        );
    }

    BikeRideDuration findBikeWithMaxRideTime(JavaRDD<Trip> trips) {
        return trips
                .mapToPair(trip -> new Tuple2<>(trip.bikeId(), (long) trip.duration()))
                .reduceByKey(Long::sum)
                .map(tuple -> new BikeRideDuration(tuple._1, tuple._2))
                .reduce((left, right) ->
                        left.totalDurationSeconds() >= right.totalDurationSeconds() ? left : right);
    }

    StationDistance findMaxGeodesicDistance(JavaRDD<Station> stations) {
        return stations
                .cartesian(stations)
                .filter(tuple -> tuple._1.stationId() < tuple._2.stationId())
                .map(tuple -> {
                    Station first = tuple._1;
                    Station second = tuple._2;
                    return new StationDistance(
                            first.stationId(),
                            first.name(),
                            second.stationId(),
                            second.name(),
                            GeoUtils.haversineKm(first.latitude(), first.longitude(), second.latitude(), second.longitude())
                    );
                })
                .reduce((left, right) -> left.distanceKm() >= right.distanceKm() ? left : right);
    }

    BikePath findBikePath(JavaRDD<Trip> trips, BikeRideDuration longestRunningBike) {
        List<Trip> bikeTrips = trips
                .filter(trip -> trip.bikeId() == longestRunningBike.bikeId())
                .sortBy(Trip::startDate, true, 1)
                .collect();

        return new BikePath(
                longestRunningBike.bikeId(),
                longestRunningBike.totalDurationSeconds(),
                bikeTrips,
                buildStationPath(bikeTrips)
        );
    }

    List<UserDuration> findUsersOverThreeHours(JavaRDD<Trip> trips) {
        JavaPairRDD<UserKey, Long> totalDurationByUser = trips
                .filter(trip -> trip.zipCode() != null && !trip.zipCode().isBlank())
                .mapToPair(trip -> new Tuple2<>(
                        new UserKey(trip.subscriptionType(), trip.zipCode()),
                        (long) trip.duration()))
                .reduceByKey(Long::sum);

        return totalDurationByUser
                .filter(tuple -> tuple._2 > 3L * 60L * 60L)
                .map(tuple -> new UserDuration(tuple._1, tuple._2))
                .sortBy(UserDuration::totalDurationSeconds, false, 1)
                .collect();
    }

    private List<String> buildStationPath(List<Trip> orderedTrips) {
        List<String> stationPath = new ArrayList<>();
        for (Trip trip : orderedTrips) {
            if (stationPath.isEmpty()) {
                stationPath.add(trip.startStationName());
                stationPath.add(trip.endStationName());
                continue;
            }

            String lastStation = stationPath.get(stationPath.size() - 1);
            if (!lastStation.equals(trip.startStationName())) {
                stationPath.add(trip.startStationName());
            }
            stationPath.add(trip.endStationName());
        }
        return stationPath;
    }
}
