package com.notification.service.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.notification.service.service.NotificationService;

@Component
public class NotificationConsumer {
	private final NotificationService notificationService;

	/*
	 * @KafkaListener(topics = "order-created", groupId = "notification-group")
	 * public void consumeOrderCreated(String message) {
	 * 
	 * System.out.println("Received Order Created Event: " + message); }
	 */

	public NotificationConsumer(NotificationService notificationService) {
		this.notificationService = notificationService;
	}

	private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

	@KafkaListener(topics = "order-created", containerFactory = "orderKafkaListenerContainerFactory")
	public void consumeOrderCreated(OrderCreatedEvent event) {
		System.out.println("=======calling notification service=========");
		notificationService.sendOrderConfirmation(event);
	}
}