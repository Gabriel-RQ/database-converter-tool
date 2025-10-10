package com.gabrielrq.database_converter.service.etl;

import com.gabrielrq.database_converter.domain.DatabaseDefinition;
import com.gabrielrq.database_converter.domain.builder.ColumnDefinitionBuilder;
import com.gabrielrq.database_converter.domain.builder.DatabaseDefinitionBuilder;
import com.gabrielrq.database_converter.domain.builder.TableDefinitionBuilder;
import com.gabrielrq.database_converter.service.JsonService;
import com.gabrielrq.database_converter.service.SqlService;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

@Service
public class DataTransformationService {

    private final JsonService jsonService;
    private final SqlService sqlService;

    public DataTransformationService(JsonService jsonService, SqlService sqlService) {
        this.jsonService = jsonService;
        this.sqlService = sqlService;
    }

    private DatabaseDefinition mapTargetTypes(DatabaseDefinition originMetadata, Map<Integer, String> targetConversionMap) {
        DatabaseDefinitionBuilder databaseBuilder = DatabaseDefinitionBuilder.from(originMetadata).setTables(new ArrayList<>());
        for (var table : originMetadata.tables()) {
            TableDefinitionBuilder tableBuilder = TableDefinitionBuilder.from(table).setColumns(new ArrayList<>());
            for (var column : table.columns()) {
                tableBuilder.addColumn(
                        ColumnDefinitionBuilder
                                .from(column)
                                .setTargetType(targetConversionMap.getOrDefault(column.genericType(), "INVALID"))
                                .build()
                );
            }
            databaseBuilder.addTable(tableBuilder.build());
        }
        return databaseBuilder.build();
    }

    public DatabaseDefinition transform(DatabaseDefinition metadata, String target) {
        try {
            Map<Integer, String> targetConversioMap = jsonService.readConversionMap(target);
            var targetMetadata = mapTargetTypes(metadata, targetConversioMap);
            Path outputPath = Path.of(metadata.name());
            jsonService.write(targetMetadata, outputPath.resolve("target.meta").toString());

            sqlService.generate(targetMetadata);

            return targetMetadata;
        } catch (IOException e) {
            throw new RuntimeException(e); // lançar excessão personalizada que será tratada pela aplicação
        }
    }

}
