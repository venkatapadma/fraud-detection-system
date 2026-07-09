package com.example.fraud_detection_system.controller;

import com.example.fraud_detection_system.events.Transaction;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.core.io.Resource;

import java.io.InputStream;
import java.util.List;

@RestController
@RequestMapping("/api/transactions")
public class CreateTransactionsController {

    @Value("classpath:transactions.json")
    private Resource transactionResource;

    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public CreateTransactionsController(KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public String sendTransaction() throws Exception {
        try(InputStream is = transactionResource.getInputStream()) {
            List<Transaction> transactions = objectMapper.readValue(is, new TypeReference<List<Transaction>>() {});
            for(Transaction txn : transactions) {
                kafkaTemplate.send("transactions", txn.transactionId(), txn);
            }
        }
        return "All Transactions sent to Kafka!";
    }
}
