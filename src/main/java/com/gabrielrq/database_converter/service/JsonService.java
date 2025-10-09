package com.gabrielrq.database_converter.service;


import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Service
public class JsonService {

    @Value("${migration.json.path}")
    private String path;

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonService() {
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public void write(Object object, String filename) {
        Path outputDir = Path.of(path);
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
        Path outputDir = Path.of(path);
        Path outputFile = outputDir.resolve(filename + ".json");

        try (
                FileOutputStream fos = new FileOutputStream(outputFile.toFile());
                BufferedOutputStream bos = new BufferedOutputStream(fos);
                JsonGenerator generator = mapper.getFactory().createGenerator(bos, JsonEncoding.UTF8);
        ) {
            Files.createDirectories(outputFile.getParent());
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
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e); // lançar excessão personalizada a ser tratada pela aplicação
        }
    }
}