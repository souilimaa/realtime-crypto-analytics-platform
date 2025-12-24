package com.example.visualization_service.visualization;

import com.example.visualization_service.DTO.CryptoDTO;
import com.example.visualization_service.services.CryptoMetricsService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class VisualizationConsumer {

    private final CryptoMetricsService metricsService;

    public VisualizationConsumer(CryptoMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Bean
    public Consumer<CryptoDTO> visConsumer() {
        return input -> {
            System.out.println("**");
            System.out.println(input.getTime());
            metricsService.updateMetrics(input);
        };
    }

}
