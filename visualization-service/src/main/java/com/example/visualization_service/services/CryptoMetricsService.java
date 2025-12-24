package com.example.visualization_service.services;

import com.example.visualization_service.DTO.CryptoDTO;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CryptoMetricsService {

    private final MeterRegistry meterRegistry;
    private final Map<String, CryptoMetrics> metricsCache = new ConcurrentHashMap<>();
    private final Set<String> registeredPairs = ConcurrentHashMap.newKeySet(); // Track registered pairs

    public CryptoMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void updateMetrics(CryptoDTO crypto) {
        String pair = crypto.getPair();

        metricsCache.compute(pair, (key, existing) -> {
            CryptoMetrics metrics = existing != null ? existing : new CryptoMetrics();
            metrics.price = Double.parseDouble(crypto.getPrice());
            metrics.ask = Double.parseDouble(crypto.getAsk());
            metrics.bid = Double.parseDouble(crypto.getBid());
            metrics.volume = Double.parseDouble(crypto.getVolume());
            metrics.size = Double.parseDouble(crypto.getSize());
            metrics.rfqVolume = Double.parseDouble(crypto.getRfq_volume());
            metrics.spread = metrics.ask - metrics.bid; // Calculate from already parsed values

            return metrics;
        });

        // Only register gauges once per pair
        if (registeredPairs.add(pair)) {
            registerGauges(pair);
        }
    }

    private void registerGauges(String pair) {
        Iterable<Tag> tags = Arrays.asList(Tag.of("pair", pair));

        Gauge.builder("crypto_price", metricsCache, cache ->
                        cache.getOrDefault(pair, new CryptoMetrics()).price)
                .tags(tags)
                .description("Current price")
                .register(meterRegistry);

        Gauge.builder("crypto_ask", metricsCache, cache ->
                        cache.getOrDefault(pair, new CryptoMetrics()).ask)
                .tags(tags)
                .description("Current ask price")
                .register(meterRegistry);

        Gauge.builder("crypto_bid", metricsCache, cache ->
                        cache.getOrDefault(pair, new CryptoMetrics()).bid)
                .tags(tags)
                .description("Current bid price")
                .register(meterRegistry);

        Gauge.builder("crypto_volume", metricsCache, cache ->
                        cache.getOrDefault(pair, new CryptoMetrics()).volume)
                .tags(tags)
                .description("Trading volume")
                .register(meterRegistry);

        Gauge.builder("crypto_spread", metricsCache, cache ->
                        cache.getOrDefault(pair, new CryptoMetrics()).spread)
                .tags(tags)
                .description("Bid-ask spread")
                .register(meterRegistry);

        Gauge.builder("crypto_size", metricsCache, cache ->
                        cache.getOrDefault(pair, new CryptoMetrics()).size)
                .tags(tags)
                .description("Trade size")
                .register(meterRegistry);

        Gauge.builder("crypto_rfq_volume", metricsCache, cache ->
                        cache.getOrDefault(pair, new CryptoMetrics()).rfqVolume)
                .tags(tags)
                .description("RFQ volume")
                .register(meterRegistry);
    }

    private static class CryptoMetrics {
        double price = 0.0;
        double ask = 0.0;
        double bid = 0.0;
        double volume = 0.0;
        double size = 0.0;
        double rfqVolume = 0.0;
        double spread = 0.0;
    }
}