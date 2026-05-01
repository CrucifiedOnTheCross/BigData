package ru.bigdata.lab1.model;

import java.io.Serializable;

public record UserDuration(UserKey userKey, long totalDurationSeconds) implements Serializable {
}
