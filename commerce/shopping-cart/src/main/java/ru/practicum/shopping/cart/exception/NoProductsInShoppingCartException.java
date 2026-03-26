package ru.practicum.shopping.cart.exception;

public class NoProductsInShoppingCartException extends RuntimeException {
    public NoProductsInShoppingCartException(String message) {
        super(message);
    }
}