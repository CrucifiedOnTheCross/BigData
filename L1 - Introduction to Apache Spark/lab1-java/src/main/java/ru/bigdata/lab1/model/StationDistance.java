package ru.bigdata.lab1.model;

import java.io.Serializable;

public record StationDistance(
        int fromStationId,
        String fromStationName,
        int toStationId,
        String toStationName,
        double distanceKm
) implements Serializable {
}
