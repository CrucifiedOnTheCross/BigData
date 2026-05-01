package ru.bigdata.lab1.model;

import java.io.Serializable;

public record BikeRideDuration(int bikeId, long totalDurationSeconds) implements Serializable {
}
