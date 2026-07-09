package com.example.fraud_detection_system.serdes;

import com.example.fraud_detection_system.events.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class TransactionSerializer implements Serializer<Transaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Transaction transaction) {
        try {
            return objectMapper.writeValueAsBytes(transaction);
        } catch (Exception e) {
            throw new SerializationException("Error serializing Transaction ", e);
        }
    }
}
