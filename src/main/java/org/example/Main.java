package org.example;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;


public class Main {
    public static void main(String[] args) {
        // Configurer l'application Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-analysis7778");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Construire le flux
        StreamsBuilder builder = new StreamsBuilder();

        // Lire les messages depuis le topic 'input-topic'
        KStream<String, String> weatherStream = builder.stream("input-topic");

        // Filtrer les températures supérieures à 30
        KStream<String, String> filteredStream = weatherStream.filter((key, value) -> {
            Double temp = Double.parseDouble(value.split(",")[1]);
            System.out.println("Filtering - Key: " + key + ", Value: " + value + ", Temp: " + temp);
            return temp > 30;
        });

        // Convertir les températures en Fahrenheit
        KStream<String, String> weatherTransformed = filteredStream.mapValues(value -> {
            String[] weather = value.split(",");
            Double temp = Double.parseDouble(weather[1]);
            temp = (temp * 1.8) + 32; // Conversion en Fahrenheit
            String transformedValue = weather[0] + "," + temp + "," + weather[2];
            System.out.println("Transforming - Original: " + value + ", Transformed: " + transformedValue);
            return transformedValue;
        });

        // Grouper par station
        KGroupedStream<String, String> groupedByStation = weatherTransformed.groupBy(
                (key, value) -> {
                    String station = value.split(",")[0];
                    System.out.println("Grouping by Station - Station: " + station + ", Value: " + value);
                    return station;
                }
        );

        // Agréger les données
        KTable<String, String> aggregatedData = groupedByStation.aggregate(
                () -> "0,0,0", // Initialisation
                (key, value, aggregate) -> {
                    String[] weather = value.split(",");
                    Double temp = Double.parseDouble(weather[1]);
                    Double humidity = Double.parseDouble(weather[2]);

                    String[] aggregateValues = aggregate.split(",");
                    Double newTempSum = Double.parseDouble(aggregateValues[0]) + temp;
                    Double newHumiditySum = Double.parseDouble(aggregateValues[1]) + humidity;
                    Integer count = Integer.parseInt(aggregateValues[2]) + 1;

                    String newAggregate = newTempSum + "," + newHumiditySum + "," + count;
                    System.out.println("Aggregating - Key: " + key + ", Current: " + aggregate + ", New: " + newAggregate);
                    return newAggregate;
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // Calculer les moyennes
        KStream<String, String> resultStream = aggregatedData.toStream().mapValues(aggregate -> {
            String[] sumsAndCount = aggregate.split(",");
            Double tempSum = Double.parseDouble(sumsAndCount[0]);
            Double humiditySum = Double.parseDouble(sumsAndCount[1]);
            Integer count = Integer.parseInt(sumsAndCount[2]);

            Double avgTemp = tempSum / count;
            Double avgHumidity = humiditySum / count;

            String result = "Température Moyenne = " + avgTemp + "°F, Humidité Moyenne = " + avgHumidity + "%";
            System.out.println("Calculating Averages - Aggregate: " + aggregate + ", Result: " + result);
            return result;
        });

        // Afficher les résultats
        resultStream.foreach((key, value) -> System.out.println("Final Record - Key: " + key + ", Value: " + value));

        // Écrire les résultats dans un topic 'output-topic'
        resultStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Créer et démarrer l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Gestion de l'arrêt propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Lancer l'application
        streams.start();
    }
}
