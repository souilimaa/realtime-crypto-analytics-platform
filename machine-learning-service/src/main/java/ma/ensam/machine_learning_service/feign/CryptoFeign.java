package ma.ensam.machine_learning_service.feign;


import ma.ensam.machine_learning_service.models.Crypto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.List;

@FeignClient(name = "consumer-service")
public interface CryptoFeign {
    @GetMapping("/crypto")
    List<Crypto> findAll();
}
