package com.notification.service.service;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.service.kafka.OrderCreatedEvent;

@Service
public class NotificationService {

	private final ObjectMapper objectMapper;

	public NotificationService(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public void sendOrderConfirmation(OrderCreatedEvent event) {
		try {
			
			//OrderCreatedEvent event = objectMapper.readValue(message, OrderCreatedEvent.class);
			System.out.println("NotificationService called for order: " + event.getOrderId());
			System.out.println("NOTIFICATION: Order " + event.getOrderId() + " has been placed successfully.");
		} catch (Exception e) {
			System.out.println("Failed to convert Kafka message: " + e.getMessage());
		}
	}
}
