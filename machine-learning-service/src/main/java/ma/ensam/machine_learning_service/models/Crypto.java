package ma.ensam.machine_learning_service.models;

import lombok.*;

import java.util.Date;

@Getter @Setter @AllArgsConstructor @NoArgsConstructor @Builder
public class Crypto {
    private Long id;
    private String pair;
    private double ask;
    private double bid;
    private double volume;
    private Long trade_id;
    private double price;
    private double size;
    private Date time;
    private Double rfq_volume;
}
