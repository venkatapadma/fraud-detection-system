package com.example.fraud_detection_system.serdes;

import com.example.fraud_detection_system.events.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class TransactionDeserializer implements Deserializer<Transaction> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Transaction deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Transaction.class);
        } catch (Exception e) {
            throw new SerializationException("Error Deserializing to Transaction ", e);
        }
    }
}
