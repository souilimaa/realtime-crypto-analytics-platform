package ma.ensam.consumer_service.web;


import ma.ensam.consumer_service.domaine.entities.Crypto;
import ma.ensam.consumer_service.repositories.CryptoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class CryptoController {

    @Autowired
    private CryptoRepository cryptoRepository;

    @GetMapping("/crypto")
    public List<Crypto> findAll() {
        List<Crypto> cryptoList = cryptoRepository.findAll();
        return cryptoList;
    }


}
