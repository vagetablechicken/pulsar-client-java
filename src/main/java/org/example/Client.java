package org.example;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.JSONSchema;

public class Client {
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    public static class Row {
        String id;
        int vendor_id;
        long pickup_datetime;
        long dropoff_datetime;
        int passenger_count;
        double pickup_longitude;
        double pickup_latitude;
        double dropoff_longitude;
        double dropoff_latitude;
        String store_and_fwd_flag;
        int trip_duration;
    }

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://127.0.0.1:6650")
                .build();
        Producer<Row> producer = client.newProducer(JSONSchema.of(Row.class))
                .topic("test_openmldb")
                .create();

        TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
        TimeZone.setDefault(tz);
        // You can then send messages to the broker and topic you specified:
        producer.send(new Row("id3097625", 1,
                Timestamp.valueOf("2016-01-22 16:01:00").getTime(),
                Timestamp.valueOf("2016-01-22 16:15:16").getTime(),
                2,
                -73.97746276855469, 40.7613525390625,
                -73.95573425292969, 40.772396087646484,
                "N", 856));
        producer.send(new Row("id3196697", 1,
                Timestamp.valueOf("2016-01-28 07:20:18").getTime(),
                Timestamp.valueOf("2016-01-28 07:40:16").getTime(),
                1,
                -73.98524475097656, 40.75959777832031,
                -73.99615478515625, 40.72945785522461,
                "N", 1198));
        producer.flush();
        producer.close();
        client.close();
    }
}
