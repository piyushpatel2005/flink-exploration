package com.github.piyushpatel2005.basics.domain;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.time.LocalDate;
import java.util.Objects;

@JsonPropertyOrder({"transactionId", "date", "customerId", "gender", "age", "productCategory", "quantity", "pricePerUnit", "totalAmount"})
public class SalesOrder {
    public Integer transactionId;
    public String date;
    public String customerId;
    public String gender;
    public Integer age;
    public String productCategory;
    public Integer quantity;
    public Double pricePerUnit;
    public Double totalAmount;

    public SalesOrder() {
    }

    public SalesOrder(int transactionId, String date, String customerId, String gender, Integer age, String productCategory, Integer quantity, Double pricePerUnit, Double totalAmount) {
        this.transactionId = transactionId;
        this.date = date;
        this.customerId = customerId;
        this.gender = gender;
        this.age = age;
        this.productCategory = productCategory;
        this.quantity = quantity;
        this.pricePerUnit = pricePerUnit;
        this.totalAmount = totalAmount;
    }

    @Override
    public String toString() {
        return "SalesOrder{" +
                "transactionId=" + transactionId +
                ", date=" + date +
                ", customerId='" + customerId + '\'' +
                ", gender='" + gender + '\'' +
                ", age=" + age +
                ", productCategory='" + productCategory + '\'' +
                ", quantity=" + quantity +
                ", pricePerUnit=" + pricePerUnit +
                ", totalAmount=" + totalAmount +
                '}';
    }
}
