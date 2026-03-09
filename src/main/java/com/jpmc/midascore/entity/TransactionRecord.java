package com.jpmc.midascore.entity;

import com.jpmc.midascore.foundation.Transaction;
import jakarta.persistence.*;

@Entity
public class TransactionRecord {


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;


    @Column(nullable = false)
    private long senderId;
    @Column(nullable = false)
    private long recipientId;
    @Column(nullable = false)
    private float amount;

    public TransactionRecord(Transaction transaction){
    this.senderId = transaction.getSenderId();
    this.recipientId = transaction.getRecipientId();
    this.amount = transaction.getAmount();
    }

    public long getSenderId(){
    return senderId;
    }
    public long getRecipientId(){
    return recipientId;
    }
    public float getAmount(){
    return amount;
    }


    @Override
    public String toString(){
        return senderId + " sends " + recipientId + " " + amount;

    }

}
