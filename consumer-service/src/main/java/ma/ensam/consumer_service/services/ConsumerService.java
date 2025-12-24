package ma.ensam.consumer_service.services;

import ma.ensam.consumer_service.domaine.DTO.CryptoDTO;
import ma.ensam.consumer_service.domaine.entities.Crypto;
import ma.ensam.consumer_service.repositories.CryptoRepository;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;

@Configuration
public class ConsumerService {

    private final CryptoRepository cryptoRepository;

    private final StreamBridge streamBridge;

    public ConsumerService(CryptoRepository cryptoRepository, StreamBridge streamBridge) {
        this.cryptoRepository = cryptoRepository;
        this.streamBridge = streamBridge;
    }

    @Bean
    public Consumer<CryptoDTO> cryptoConsumer() {
        return input -> {
            Crypto crypto = Crypto.builder()
                    .size(safeDouble(input.getSize()))
                    .ask(safeDouble(input.getAsk()))
                    .pair(input.getPair())
                    .bid(safeDouble(input.getBid()))
                    .volume(safeDouble(input.getVolume()))
                    .rfq_volume(safeDouble(input.getRfq_volume()))
                    .time(Date.from(Instant.parse(input.getTime())))
                    .price(safeDouble(input.getPrice()))
                    .trade_id(input.getTrade_id())
                    .build();

            //cryptoRepository.save(crypto);
            streamBridge.send("vis", crypto);
        };
    }

    private Double safeDouble(String value) {
        try { return Double.parseDouble(value); }
        catch (Exception e) { return null; }
    }
}
