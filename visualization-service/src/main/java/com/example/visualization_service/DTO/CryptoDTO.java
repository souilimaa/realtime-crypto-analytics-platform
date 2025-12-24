package com.example.visualization_service.DTO;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class CryptoDTO {
    private long id;
    private String pair;
    private String ask;
    private String bid;
    private String volume;
    private Long trade_id;
    private String price;
    private String size;
    private String time;
    private String rfq_volume;

}
