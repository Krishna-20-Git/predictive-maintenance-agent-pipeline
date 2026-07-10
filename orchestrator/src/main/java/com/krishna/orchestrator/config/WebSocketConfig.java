package com.krishna.orchestrator.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * Configures Spring's STOMP-over-WebSocket message broker.
 *
 * Clients (the React frontend, Day 22) connect to /ws and subscribe to
 * topics under /topic/ to receive real-time push events. The in-memory
 * broker is fine for a single-instance portfolio project — a production
 * system would use a dedicated broker like RabbitMQ behind /topic.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // Enable a simple in-memory broker for /topic/** destinations
        registry.enableSimpleBroker("/topic");
        // Application-level destinations (for @MessageMapping methods) use /app prefix
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // The WebSocket handshake endpoint clients connect to
        registry.addEndpoint("/ws")
                // Allow the React dev server (localhost:3000) and any other origin
                // during development — tighten this to specific origins before
                // any production deployment
                .setAllowedOriginPatterns("*")
                // SockJS fallback for browsers/environments that don't support
                // native WebSocket (not strictly needed for modern browsers, but
                // keeps the option open and costs nothing)
                .withSockJS();
    }
}
