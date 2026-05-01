package ru.bigdata.lab1;

import ru.bigdata.lab1.model.AnalysisResult;
import ru.bigdata.lab1.model.Trip;
import ru.bigdata.lab1.model.UserDuration;

import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

public final class ReportFormatter {

    private static final int MAX_PATH_STATIONS_TO_PRINT = 30;
    private static final int MAX_TRIPS_TO_PRINT = 5;
    private static final int MAX_USERS_TO_PRINT = 20;

    private ReportFormatter() {
    }

    public static String format(AnalysisResult result) {
        StringBuilder builder = new StringBuilder();
        builder.append("Lab 1 results").append(System.lineSeparator());
        builder.append("================").append(System.lineSeparator());
        builder.append("Valid trips: ").append(result.validTrips())
                .append(", skipped trips: ").append(result.skippedTrips()).append(System.lineSeparator());
        builder.append("Valid stations: ").append(result.validStations())
                .append(", skipped stations: ").append(result.skippedStations()).append(System.lineSeparator());
        builder.append(System.lineSeparator());

        builder.append("1. Bike with maximum total ride time").append(System.lineSeparator());
        builder.append("   Bike ID: ").append(result.longestRunningBike().bikeId())
                .append(", total duration: ").append(result.longestRunningBike().totalDurationSeconds())
                .append(" sec").append(System.lineSeparator());

        builder.append("2. Maximum geodesic distance between stations").append(System.lineSeparator());
        builder.append("   ").append(result.farthestStations().fromStationName())
                .append(" (").append(result.farthestStations().fromStationId()).append(")")
                .append(" <-> ")
                .append(result.farthestStations().toStationName())
                .append(" (").append(result.farthestStations().toStationId()).append(")")
                .append(", distance: ").append(String.format(Locale.US, "%.3f km", result.farthestStations().distanceKm()))
                .append(System.lineSeparator());

        builder.append("3. Path of the bike with maximum total ride time").append(System.lineSeparator());
        builder.append("   Trips in path: ").append(result.longestRunningBikePath().trips().size()).append(System.lineSeparator());
        builder.append("   Stations in reconstructed path: ")
                .append(result.longestRunningBikePath().stationPath().size())
                .append(System.lineSeparator());
        builder.append("   Path preview: ").append(stationPathPreview(result.longestRunningBikePath().stationPath())).append(System.lineSeparator());
        builder.append("   First five segments:").append(System.lineSeparator());
        result.longestRunningBikePath().trips().stream().limit(MAX_TRIPS_TO_PRINT).forEach(trip ->
                appendTrip(builder, trip));

        builder.append("4. Number of bikes in the system").append(System.lineSeparator());
        builder.append("   ").append(result.bikeCount()).append(System.lineSeparator());

        builder.append("5. Users with more than 3 hours total ride time").append(System.lineSeparator());
        builder.append("   Assumption: a user is identified by subscription_type + zip_code because the dataset does not contain a dedicated user id.")
                .append(System.lineSeparator());
        if (result.usersOverThreeHours().isEmpty()) {
            builder.append("   No users found").append(System.lineSeparator());
        } else {
            builder.append("   Total users found: ").append(result.usersOverThreeHours().size()).append(System.lineSeparator());
            List<UserDuration> preview = result.usersOverThreeHours().stream().limit(MAX_USERS_TO_PRINT).toList();
            for (UserDuration user : preview) {
                builder.append("   ")
                        .append(user.userKey().subscriptionType()).append(" / ")
                        .append(user.userKey().zipCode()).append(" -> ")
                        .append(user.totalDurationSeconds()).append(" sec")
                        .append(System.lineSeparator());
            }
            if (result.usersOverThreeHours().size() > preview.size()) {
                builder.append("   ... truncated, showing top ")
                        .append(preview.size())
                        .append(" users by total duration")
                        .append(System.lineSeparator());
            }
        }

        return builder.toString();
    }

    private static String stationPathPreview(List<String> stationPath) {
        StringJoiner joiner = new StringJoiner(" -> ");
        int limit = Math.min(stationPath.size(), MAX_PATH_STATIONS_TO_PRINT);
        for (int index = 0; index < limit; index++) {
            joiner.add(stationPath.get(index));
        }
        if (stationPath.size() > limit) {
            joiner.add("...");
        }
        return joiner.toString();
    }

    private static void appendTrip(StringBuilder builder, Trip trip) {
        builder.append("   - ")
                .append(trip.startDate())
                .append(": ")
                .append(trip.startStationName())
                .append(" -> ")
                .append(trip.endStationName())
                .append(" (")
                .append(trip.duration())
                .append(" sec)")
                .append(System.lineSeparator());
    }
}
