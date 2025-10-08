package com.gabrielrq.database_converter.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Service
public class JsonService {

    @Value("${json.path}")
    private String path;

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonService() {
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public void write(Object object, String filename) {
        Path outputDir = Path.of(path);
        try {
            Path outputFile = outputDir.resolve(filename + ".json");
            Files.createDirectories(outputFile.getParent());
            if (Files.exists(outputFile)) {
                Files.delete(outputFile);
            }
            mapper.writeValue(outputFile.toFile(), object);
        } catch (IOException e) {
            throw new RuntimeException(e); // lançar excessão personalizada a ser tratada pela aplicação
        }
    }
}
