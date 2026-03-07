package com.jpmc.midascore;


import com.jpmc.midascore.foundation.Transaction;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import org.springframework.data.repository.Repository;

import java.util.Optional;

@Entity
public class TransactionRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    private long senderId;
    private long recipientId;
    private float amount;

    public TransactionRecord(Transaction transaction){
        this.senderId = transaction.getSenderId();
        this.recipientId = transaction.getRecipientId();
        this.amount = transaction.getAmount();

    }

    public long getSenderId() {
        return senderId;
    }

    public void setSenderId(long senderId) {
        this.senderId = senderId;
    }

    public long getRecipientId() {
        return recipientId;
    }

    public void setRecipientId(long recipientId) {
        this.recipientId = recipientId;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "TransactionRecord for {senderId=" + senderId + ", recipientId=" + recipientId + ", amount=" + amount + "}";
    }

}

interface TransactionRecordRepository extends Repository<TransactionRecord, Long> {

    TransactionRecord save(TransactionRecord person);

    Optional<TransactionRecord> findById(long id);
}
