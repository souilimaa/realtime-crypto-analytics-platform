package ma.ensam.machine_learning_service.web;

import ma.ensam.machine_learning_service.feign.CryptoFeign;
import ma.ensam.machine_learning_service.models.Crypto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class CryptoRestController {

    @Autowired
    private CryptoFeign cryptoFeign;

    @GetMapping("/crypto")
    public List<Crypto> findAll() {
        return cryptoFeign.findAll();
    }

}
