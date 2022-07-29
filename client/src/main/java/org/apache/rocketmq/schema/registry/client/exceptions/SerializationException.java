package org.apache.rocketmq.schema.registry.client.exceptions;

public class SerializationException extends RuntimeException{
    public SerializationException(){}

    public SerializationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public SerializationException(String msg) {
        super(msg);
    }

    public SerializationException(Throwable cause) {
        super(cause);
    }
}
