package ma.ensam.consumer_service.services;

import ma.ensam.consumer_service.domaine.DTO.CryptoDTO;
import ma.ensam.consumer_service.domaine.entities.Crypto;
import ma.ensam.consumer_service.repositories.CryptoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;
@SuppressWarnings("unused")

@Service
public class ConsumerService {

    @Autowired
    private StreamBridge streamBridge;

    @Autowired
    private CryptoRepository cryptoRepository;

   // @Bean
//    public Consumer<CryptoDTO> cryptoConsumer() {
//        return (input) -> {
//            System.out.println(input.getPrice());
//            Crypto crypto = Crypto.builder().size(Double.parseDouble(input.getSize()))
//                    .ask(Double.parseDouble(input.getAsk()))
//                    .pair(input.getPair())
//                    .bid(Double.parseDouble(input.getBid()))
//                    .volume(Double.parseDouble(input.getVolume()))
//                    .rfq_volume(Double.parseDouble(input.getRfq_volume()))
//                    .time(new Date(input.getTime()))
//                    .price(Double.parseDouble(input.getPrice()))
//                    .trade_id(input.getTrade_id())
//                    .build();
//
//            cryptoRepository.save(crypto);
//        };
//    }

    @Bean
    public Consumer<CryptoDTO> cryptoConsumer() {
        return (input) -> {
            System.out.println("rfq_volume :"+input.getRfq_volume());
            Crypto crypto = Crypto.builder()
                    .size(safeDouble(input.getSize()))
                    .ask(safeDouble(input.getAsk()))
                    .pair(input.getPair())
                    .bid(safeDouble(input.getBid()))
                    .volume(safeDouble(input.getVolume()))
                    .rfq_volume(Double.valueOf(input.getRfq_volume()))
                    .time(Date.from(Instant.parse(input.getTime())))
                    .price(safeDouble(input.getPrice()))
                    .trade_id(input.getTrade_id())
                    .build();

            //cryptoRepository.save(crypto);
        };
    }

    private Double safeDouble(String value) {
        try { return Double.parseDouble(value); }
        catch (Exception e) { return null;}
    }
}
