package ru.bigdata.lab1;

import org.junit.jupiter.api.Test;
import ru.bigdata.lab1.model.Station;
import ru.bigdata.lab1.model.Trip;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class CsvParsersTest {

    @Test
    void parsesValidTripRow() {
        String row = "4576,63,8/29/2013 14:13,South Van Ness at Market,66,8/29/2013 14:14,South Van Ness at Market,66,520,Subscriber,94127";

        Trip trip = CsvParsers.parseTrip(row);

        assertNotNull(trip);
        assertEquals(4576, trip.tripId());
        assertEquals(63, trip.duration());
        assertEquals(LocalDateTime.of(2013, 8, 29, 14, 13), trip.startDate());
        assertEquals(520, trip.bikeId());
        assertEquals("94127", trip.zipCode());
    }

    @Test
    void skipsTripWithMissingMandatoryField() {
        String row = "4607,,8/29/2013 14:42,San Jose City Hall,10,8/29/2013 14:43,San Jose City Hall,10,661,Subscriber,95138";

        Trip trip = CsvParsers.parseTrip(row);

        assertNull(trip);
    }

    @Test
    void parsesValidStationRow() {
        String row = "2,San Jose Diridon Caltrain Station,37.329732,-121.90178200000001,27,San Jose,8/6/2013";

        Station station = CsvParsers.parseStation(row);

        assertNotNull(station);
        assertEquals(2, station.stationId());
        assertEquals("San Jose Diridon Caltrain Station", station.name());
        assertEquals(LocalDate.of(2013, 8, 6), station.installationDate());
    }
}
