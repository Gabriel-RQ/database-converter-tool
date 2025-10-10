package com.gabrielrq.database_converter.service;


import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

@Service
public class JsonService {

    @Value("${migration.data.path}")
    private String basePath;
    @Value("${migration.transform.maps.path}")
    private String conversionMapsPath;

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonService() {
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public void write(Object object, String filename) {
        Path outputDir = Path.of(basePath);
        Path outputFile = outputDir.resolve(filename + ".json");
        try {
            Files.createDirectories(outputFile.getParent());
            if (Files.exists(outputFile)) {
                Files.delete(outputFile);
            }
            mapper.writeValue(outputFile.toFile(), object);
        } catch (IOException e) {
            throw new RuntimeException(e); // lançar excessão personalizada a ser tratada pela aplicação
        }
    }

    public void writeStream(ResultSet rs, String filename) {
        Path outputDir = Path.of(basePath);
        Path outputFile = outputDir.resolve(filename + ".json");

        try {
            Files.createDirectories(outputFile.getParent());
            try (
                    FileOutputStream fos = new FileOutputStream(outputFile.toFile());
                    BufferedOutputStream bos = new BufferedOutputStream(fos);
                    JsonGenerator generator = mapper.getFactory().createGenerator(bos, JsonEncoding.UTF8);
            ) {
                generator.writeStartArray();

                ResultSetMetaData metadata = rs.getMetaData();
                int columns = metadata.getColumnCount();
                String[] columnNames = new String[columns];

                for (int i = 1; i <= columns; i++) {
                    columnNames[i - 1] = metadata.getColumnName(i);
                }

                while (rs.next()) {
                    generator.writeStartObject();

                    for (int i = 1; i <= columns; i++) {
                        String columnName = columnNames[i - 1];
                        Object value = rs.getObject(i);
                        generator.writeObjectField(columnName, value);
                    }

                    generator.writeEndObject();
                }

                generator.writeEndArray();
                generator.flush();
            }
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e); // lançar excessão personalizada a ser tratada pela aplicação
        }
    }

    public Map<Integer, String> readConversionMap(String mapName) throws IOException {
        Path mapPath = Path.of(conversionMapsPath).resolve(mapName + ".json");

        try (InputStream stream = JsonService.class.getClassLoader().getResourceAsStream(mapPath.toString())) {
            return mapper.readValue(stream, new TypeReference<Map<Integer, String>>() {
            });
        }
    }
}