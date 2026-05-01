package ru.bigdata.lab1.model;

import java.io.Serializable;
import java.util.List;

public record AnalysisResult(
        BikeRideDuration longestRunningBike,
        StationDistance farthestStations,
        BikePath longestRunningBikePath,
        long bikeCount,
        List<UserDuration> usersOverThreeHours,
        long validTrips,
        long skippedTrips,
        long validStations,
        long skippedStations
) implements Serializable {
}
