package com.example.fraud_detection_system.controller;

import com.example.fraud_detection_system.events.Item;
import com.example.fraud_detection_system.events.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/transactions")
@Slf4j
public class GetTransactionsController {

    @Value("classpath:transactions.json")
    private Resource transactionResource;

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final String TOPIC = "transactions";
    private final Random random = new Random();

    public GetTransactionsController(KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/produceTransactions")
    public String produceTransactions() throws InterruptedException {
        log.info("Starting to publish random transactions...");

        List<String> users = Arrays.asList("U1", "U2", "U3");
        List<String> locations = Arrays.asList("India", "USA", "UK", "China");
        List<String> types = Arrays.asList("debit", "credit");

        for (int i=0; i<15; i++) {
            String user = users.get(random.nextInt(users.size()));
            Transaction tx = new Transaction(UUID.randomUUID().toString(),
                    user,
                    1000+random.nextInt(9000),
                    locations.get(random.nextInt(locations.size())),
                    types.get(random.nextInt(types.size())),
                    List.of(new Item("I-"+ random.nextInt(1000),
                            "Product-"+random.nextInt(5),
                            1000+random.nextDouble(3000),
                            random.nextInt(5)))
                    );
            kafkaTemplate.send(TOPIC, tx);
            log.info("Transaction sent for {}: {}", user, tx);

            //small delay between messages so they spread windows
            TimeUnit.SECONDS.sleep(1);
        }

        log.info("Finished sending transactions!");
        return "Transactions published successfully!";
    }
}
