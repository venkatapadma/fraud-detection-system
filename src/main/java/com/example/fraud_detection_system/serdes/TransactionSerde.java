package com.example.fraud_detection_system.serdes;

import com.example.fraud_detection_system.events.Transaction;
import org.apache.kafka.common.serialization.Serdes;

public class TransactionSerde extends Serdes.WrapperSerde<Transaction> {
    public TransactionSerde() {
        super(new TransactionSerializer(), new TransactionDeserializer());
    }
}
