package ru.bigdata.lab1;

import ru.bigdata.lab1.model.Station;
import ru.bigdata.lab1.model.Trip;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public final class CsvParsers {

    private static final DateTimeFormatter TRIP_DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy H:mm");
    private static final DateTimeFormatter STATION_DATE_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy");

    private CsvParsers() {
    }

    public static Trip parseTrip(String line) {
        String[] columns = line.split(",", -1);
        if (columns.length != 11) {
            return null;
        }

        try {
            if (isBlank(columns[0]) || isBlank(columns[1]) || isBlank(columns[2]) || isBlank(columns[4])
                    || isBlank(columns[5]) || isBlank(columns[7]) || isBlank(columns[8])) {
                return null;
            }

            return new Trip(
                    Integer.parseInt(columns[0]),
                    Integer.parseInt(columns[1]),
                    LocalDateTime.parse(columns[2], TRIP_DATE_TIME_FORMAT),
                    columns[3],
                    Integer.parseInt(columns[4]),
                    LocalDateTime.parse(columns[5], TRIP_DATE_TIME_FORMAT),
                    columns[6],
                    Integer.parseInt(columns[7]),
                    Integer.parseInt(columns[8]),
                    columns[9],
                    columns[10]
            );
        } catch (NumberFormatException | DateTimeParseException ex) {
            return null;
        }
    }

    public static Station parseStation(String line) {
        String[] columns = line.split(",", -1);
        if (columns.length != 7) {
            return null;
        }

        try {
            if (isBlank(columns[0]) || isBlank(columns[1]) || isBlank(columns[2]) || isBlank(columns[3])
                    || isBlank(columns[4]) || isBlank(columns[5]) || isBlank(columns[6])) {
                return null;
            }

            return new Station(
                    Integer.parseInt(columns[0]),
                    columns[1],
                    Double.parseDouble(columns[2]),
                    Double.parseDouble(columns[3]),
                    Integer.parseInt(columns[4]),
                    columns[5],
                    LocalDate.parse(columns[6], STATION_DATE_FORMAT)
            );
        } catch (NumberFormatException | DateTimeParseException ex) {
            return null;
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
