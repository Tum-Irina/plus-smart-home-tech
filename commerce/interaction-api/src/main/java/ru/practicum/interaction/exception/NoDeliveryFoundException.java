package ru.practicum.interaction.exception;

public class NoDeliveryFoundException extends RuntimeException {
    public NoDeliveryFoundException(String message) {
        super(message);
    }
}