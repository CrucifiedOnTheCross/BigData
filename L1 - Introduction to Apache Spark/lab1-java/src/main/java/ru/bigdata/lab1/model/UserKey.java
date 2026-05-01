package ru.bigdata.lab1.model;

import java.io.Serializable;

public record UserKey(String subscriptionType, String zipCode) implements Serializable {
}
