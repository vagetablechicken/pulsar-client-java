import java.sql.Timestamp;

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
        int id;
        long time;
    }

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://127.0.0.1:6650")
                .build();
        Producer<Row> producer = client.newProducer(JSONSchema.of(Row.class))
                .topic("test_openmldb")
                .create();

        // You can then send messages to the broker and topic you specified:
        producer.send(new Row(1, Timestamp.valueOf("2022-04-06 00:00:00").getTime()));
        producer.send(new Row(2, Timestamp.valueOf("2022-04-06 09:00:00").getTime()));
        producer.flush();
        producer.close();
        client.close();
    }
}
