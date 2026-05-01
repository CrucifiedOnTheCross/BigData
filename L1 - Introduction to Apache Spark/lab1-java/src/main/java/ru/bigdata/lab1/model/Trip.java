package ru.bigdata.lab1.model;

import java.io.Serializable;
import java.time.LocalDateTime;

public record Trip(
        int tripId,
        int duration,
        LocalDateTime startDate,
        String startStationName,
        int startStationId,
        LocalDateTime endDate,
        String endStationName,
        int endStationId,
        int bikeId,
        String subscriptionType,
        String zipCode
) implements Serializable {
}
