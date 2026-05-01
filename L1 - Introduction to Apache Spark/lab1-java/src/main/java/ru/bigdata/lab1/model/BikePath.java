package ru.bigdata.lab1.model;

import java.io.Serializable;
import java.util.List;

public record BikePath(
        int bikeId,
        long totalDurationSeconds,
        List<Trip> trips,
        List<String> stationPath
) implements Serializable {
}
