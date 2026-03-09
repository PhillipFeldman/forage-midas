package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

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
            completeTransaction(transactionRecord);
        }


    }

    private void completeTransaction(TransactionRecord transactionRecord) {
        UserRecord sender = getUserRecordById(transactionRecord.getSenderId());
        UserRecord recipient = getUserRecordById(transactionRecord.getRecipientId());
        float amount = transactionRecord.getAmount();
        sender.setBalance(sender.getBalance() - amount);
        recipient.setBalance(recipient.getBalance() + amount);
        service.save(sender);
        service.save(recipient);
        System.out.println(sender.getName() + " sends " + recipient.getName() + " " + amount + " from balance of " + sender.getBalance());
        System.out.println("New balance for "+ sender.getName() + ": "+sender.getBalance());
        System.out.println("New balance for "+ recipient.getName() + ": "+recipient.getBalance());
        service.save(transactionRecord);

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


    private final DatabaseConduit service;
    //private final TransactionDBConduit tdbc;

    public KafkaConsumer(DatabaseConduit service) {
        this.service = service;
        //this.tdbc = tdbc;
    }

    @GetMapping("/{id}")
    public UserRecord getUserRecordById(@PathVariable Long id) {
        return service.getUserById(id);
    }


}
