package com.maxcapital.orderprocessing.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxcapital.orderprocessing.dto.ExecutionReport;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import java.util.List;

/**
 * Endpoint de prueba para emitir un stream de ER de prueba. No forma
 * parte del "producto" en si, es la herramienta de seed pedida en el
 * enunciado (punto 2 de los entregables). Publica a er.raw usando
 * numericOrderId como key, respetando el mismo particionado que
 * tendria el flujo real.
 */
@RestController
@RequestMapping("/test")
@RequiredArgsConstructor
public class TestSeedController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic-er-raw}")
    private String erRawTopic;

    /**
     * Escenarios disponibles:
     *   - interleaved: dos ordenes con ER intercalados, una termina
     *     FILLED (emite settlement) y otra CANCELLED (no emite).
     *   - duplicates: un ER (mismo secondaryTradeId+operationNumber)
     *     enviado dos veces, para probar idempotencia.
     */
    @PostMapping("/seed/{scenario}")
    public ResponseEntity<String> seed(@PathVariable String scenario) {
        List<ExecutionReport> ers = loadScenario(scenario);

        for (ExecutionReport er : ers) {
            kafkaTemplate.send(erRawTopic, String.valueOf(er.numericOrderId()), er);
        }

        return ResponseEntity.ok(ers.size() + " ExecutionReports enviados a " + erRawTopic);
    }

    private List<ExecutionReport> loadScenario(String scenario) {
        String path = "scenarios/%s.json".formatted(scenario);
        try {
            ClassPathResource resource = new ClassPathResource(path);
            if (!resource.exists()) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "Escenario no encontrado: " + scenario);
            }
            return objectMapper.readValue(resource.getInputStream(),
                objectMapper.getTypeFactory().constructCollectionType(List.class, ExecutionReport.class));
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                "Error cargando escenario " + scenario, e);
        }
    }
}
