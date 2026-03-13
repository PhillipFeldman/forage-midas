package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {


    @KafkaListener(topics = {"${general.kafka-topic}"}, groupId = "com.jpmc")
    public void consume(String quote) {
        //System.out.println("I Received " + quote);

        String[] transactionData = quote.split(",");
        Transaction transaction = new Transaction(
                Long.parseLong(transactionData[0].split(":")[1]),
                Long.parseLong(transactionData[1].split(":")[1]),
                Float.parseFloat(transactionData[2].split(":")[1].replace("}", ""))
        );
        System.out.println("transaction: " + transaction);

        TransactionRecord transactionRecord = new TransactionRecord(transaction);
        if (validateTransaction(transactionRecord)) {
            Incentive incentive = incentiveService.getIncentive(transaction);
            float incentiveAmount = incentive.getAmount();
            transactionRecord.setIncentiveAmount(incentiveAmount);
            completeTransaction(transactionRecord);
            //post validated Transaction object to Incentives API

        }


    }

    private void completeTransaction(TransactionRecord transactionRecord) {
        float incentiveAmount = transactionRecord.getIncentiveAmount();
        UserRecord sender = getUserRecordById(transactionRecord.getSenderId());
        UserRecord recipient = getUserRecordById(transactionRecord.getRecipientId());
        float amount = transactionRecord.getAmount();
        sender.setBalance(sender.getBalance() - amount);//Put incentives here
        recipient.setBalance(recipient.getBalance() + amount+incentiveAmount);
        service.save(sender);
        service.save(recipient);
        System.out.println(sender.getName() + " sends " + recipient.getName() + " " + amount + " from balance of " + sender.getBalance());
        System.out.println("New balance for " + sender.getName() + ": " + sender.getBalance());
        System.out.println("New balance for " + recipient.getName() + ": " + recipient.getBalance());
        service.save(transactionRecord);
        /*
        for (TransactionRecord tr : getAllTransactionRecords()) {
            System.out.println(tr);
        }
        */

    }

    private boolean validateTransaction(TransactionRecord transactionRecord) {
        UserRecord sender;
        UserRecord recipient;
        try {
            sender = getUserRecordById(transactionRecord.getSenderId());
        } catch (RuntimeException e) {
            System.out.println("Invalid senderId: " + transactionRecord.getSenderId());
            return false;
        }
        try {
            recipient = getUserRecordById(transactionRecord.getRecipientId());
        } catch (RuntimeException e) {
            System.out.println("Invalid recipientId: " + transactionRecord.getRecipientId());
            return false;
        }
        float amount = transactionRecord.getAmount();
        if (sender.getBalance() < amount) {
            System.out.println("Sender's balance of " + sender.getBalance() + " is too low to send " + amount);
            return false;
        }
        return true;
    }


    @Autowired
    private IncentiveService incentiveService;


    private final DatabaseConduit service;

    public KafkaConsumer(DatabaseConduit service) {
        this.service = service;
    }

    @GetMapping("/{id}")
    public UserRecord getUserRecordById(@PathVariable Long id) {
        return service.getUserById(id);
    }


    //doesn't work for some reason:
    @GetMapping
    public Iterable<TransactionRecord> getAllTransactionRecords() {
        return service.getAllTransactionRecords();
    }


}


@Service
class IncentiveService {


    private final RestTemplate restTemplate;

    private static final String INCENTIVE_API_URL =
            "http://localhost:8080/incentive";

    public IncentiveService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public Incentive getIncentive(Transaction transaction) {

        ResponseEntity<Incentive> response =
                restTemplate.postForEntity(
                        INCENTIVE_API_URL,
                        transaction,
                        Incentive.class
                );

        return response.getBody();
    }
}

class Incentive {

    private float amount;

    public Incentive() {}

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }
}

