package com.test.simple;

public class KafkaClientException extends RuntimeException {
    public KafkaClientException(String s) {
        super(s);
    }

    public KafkaClientException(Throwable throwable) {
        super(throwable);
    }
}
