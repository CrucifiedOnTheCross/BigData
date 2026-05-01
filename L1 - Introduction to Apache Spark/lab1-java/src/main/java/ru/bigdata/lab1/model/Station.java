package ru.bigdata.lab1.model;

import java.io.Serializable;
import java.time.LocalDate;

public record Station(
        int stationId,
        String name,
        double latitude,
        double longitude,
        int dockCount,
        String city,
        LocalDate installationDate
) implements Serializable {
}
