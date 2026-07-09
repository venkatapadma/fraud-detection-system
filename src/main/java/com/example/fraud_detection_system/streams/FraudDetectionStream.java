package com.example.fraud_detection_system.streams;

import com.example.fraud_detection_system.events.Item;
import com.example.fraud_detection_system.events.Transaction;
import com.example.fraud_detection_system.serdes.TransactionSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import java.util.List;

import java.util.ArrayList;
import java.util.Map;

@Configuration
@EnableKafkaStreams
@Slf4j
public class FraudDetectionStream {

    //create a bean
    //-> read the topic
    //-> process the filter
    //-> write to dest

    @Bean
    public KStream<String, Transaction> fraudDetectStream(StreamsBuilder builder) {

        //Serde<Transaction> transactionSerde = new JacksonJsonSerde<>(Transaction.class);

        //step 1: Read messages from the input topic.
        KStream<String, Transaction> transactionsStream = builder.stream("transactions", Consumed.with(Serdes.String(), new TransactionSerde()));

        //step 2: Process the stream to detect fraudulent transactions.
        KStream<String, Transaction> fraudTransactionsStream = transactionsStream.filter((key, tx) -> isSuspicious(tx))
                .peek((key, tx) ->
                        log.warn("FRAUD ALERT - transactionId={}, value={}", key, tx.amount()));

        //filter
        transactionsStream.filter((key, tx)->tx.amount() > 25000)
                .peek((key, tx) -> log.warn("FRAUD ALERT for {}",tx));
//
//        //filterNot()
//        transactionsStream.filterNot((key, tx) -> tx.amount() < 10000)
//        .peek((key, tx) -> log.warn("filtering unusual transactions - FRAUD ALERT for {}",tx));
//
//        //map()
//        transactionsStream.map((key,tx)-> KeyValue.pair(tx.userId(), "user spent amount: "+ tx.amount()))
//                .peek((key,tx) ->log.info("User Transaction summary: key: {}, value: {}", key, tx));
//
//        //mapValues()
//        transactionsStream.mapValues(tx->"Transaction of "+tx.amount()+" by user "+tx.userId())
//                .peek((key,tx)-> log.info("User Transaction summary value Only: key: {}, value: {}", key, tx));
//
//        //flatMap()
//        transactionsStream.flatMap((key, tx) -> {
//            List<KeyValue<String, Item >> result = new ArrayList<>();
//            for(Item item: tx.items()) {
//                result.add(KeyValue.pair(tx.transactionId(), item));
//            } return result;
//        }).peek((key, item)->log.info("flatMap --- Item purchased: Transaction Id: {}, item:{}",key, item));
//
//        //flatMapValues()
//        transactionsStream.flatMapValues(Transaction::items)
//                .peek((key,item)->log.info("flatMapValues --- Item purchased value only: Transaction Id: {}, item:{}",key, item));
//
//        //split()
//        Map<String, KStream<String, Transaction>> branch = transactionsStream.split()
//                .branch((key, tx)-> tx.type().equalsIgnoreCase("debit"),
//                        Branched.withConsumer(debitStream->debitStream
//                                .peek((key,tx)->log.info("debit transactionId={}, item:{}",key,tx.amount()))
//                                .to("debit-transactions", Produced.with(Serdes.String(), new TransactionSerde()))))
//                .branch((key, tx)-> tx.type().equalsIgnoreCase("credit"))
//                .noDefaultBranch();
//
//        //groupBy()
//        transactionsStream.groupBy((key, tx) -> tx.location()).count()
//                .toStream().peek((loc, count) -> {log.info("Location {} has {} transactions", loc, count);});
//
//        transactionsStream.groupBy((key, tx) -> tx.userId()).count(Materialized.as("user-txn-count-store"))
//                .toStream().peek((user, count) -> {log.info("userId {} has {} transactions", user, count);});
//
//        //aggregate()
//        transactionsStream.groupBy((key, tx) -> tx.type())
//                .aggregate(()->0.0,(type, tx, currentSum)->currentSum+tx.amount(),
//                        Materialized.with(Serdes.String(), Serdes.Double())
//                ).toStream().peek((type, total)->
//                        log.info("CardType: {} | Running Total Amount: {}", type, total));

        //step 3: write detected fraudulent transactions to an output topic.
        fraudTransactionsStream.to("fraud-alerts", Produced.with(Serdes.String(), new TransactionSerde()));


        return transactionsStream;
    }

    private boolean isSuspicious(Transaction tx) {
        try {
            //Transaction transaction = new ObjectMapper().readValue(value, Transaction.class); // validate JSON
            return tx.amount() > 10000; //simple fraud rule
        } catch (Exception e) {
            return false;
        }
    }
}
