package com.gabrielrq.database_converter.service.etl;


import com.gabrielrq.database_converter.domain.builder.ColumnDefinitionBuilder;
import com.gabrielrq.database_converter.domain.builder.ForeignKeyDefinitionBuilder;
import com.gabrielrq.database_converter.domain.builder.TableDefinitionBuilder;
import com.gabrielrq.database_converter.domain.DatabaseDefinition;
import com.gabrielrq.database_converter.domain.TableDefinition;
import com.gabrielrq.database_converter.dto.DbConnectionConfigDTO;
import com.gabrielrq.database_converter.service.DatabaseConnectionService;
import com.gabrielrq.database_converter.service.JsonService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

@Service
public class DataExtractionService {

    @Value("${migration.extract.threads:0}")
    private int threadPoolSize;
    @Value("${migration.extract.threadBatchSize:0}")
    private int threadBatchSize;
    @Value("${migration.extract.fetchSize:0}")
    private int fetchSize;

    private final DatabaseConnectionService databaseConnectionService;
    private final JsonService jsonService;

    public DataExtractionService(DatabaseConnectionService databaseConnectionService, JsonService jsonService) {
        this.databaseConnectionService = databaseConnectionService;
        this.jsonService = jsonService;
    }

    private void storeToJSON(DbConnectionConfigDTO config, DatabaseDefinition metadata) {
        Path outputPath = Path.of(metadata.name());
        jsonService.write(metadata, outputPath.resolve("db.meta").toString());

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        int poolSize = threadPoolSize > 0 ? threadPoolSize : Math.max(1, availableProcessors * 2);
        int batchSize = Math.min(threadBatchSize > 0 ? threadBatchSize : Math.max(1, Math.round(poolSize / 2.0f)), poolSize);

        try (ExecutorService executor = Executors.newFixedThreadPool(poolSize)) {
            Semaphore semaphore = new Semaphore(batchSize);
            List<Future<?>> futures = new ArrayList<>();
            Map<String, Throwable> failedTables = new ConcurrentHashMap<>();

            for (TableDefinition table : metadata.tables()) {
                semaphore.acquire();

                futures.add(
                        executor.submit(() -> {
                            String schema = table.schema();
                            String tableName = table.name();

                            String fullTableName = (schema != null && !schema.isBlank())
                                    ? schema + "." + tableName
                                    : tableName;


                            String sql = "SELECT * FROM " + fullTableName;
                            try (
                                    Connection connection = databaseConnectionService.createConnection(config);
                                    Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                            ) {
                                stmt.setFetchSize(fetchSize);
                                ResultSet rs = stmt.executeQuery(sql);
                                jsonService.writeStream(rs, outputPath.resolve(fullTableName).toString());
                            } catch (SQLException e) {
                                failedTables.put(tableName, e);
                            } finally {
                                semaphore.release();
                            }
                        })
                );
            }

            executor.shutdown();
            boolean isTerminated = executor.awaitTermination(30, TimeUnit.MINUTES);

            if (!isTerminated) {
                throw new RuntimeException("Executor did not terminate successfully"); // lançar excessão personalizada a ser tratada pela aplicação
            }

            for (var future : futures) {
                future.get();
            }

            if (!failedTables.isEmpty()) {
                throw new RuntimeException("A table failed to be saved"); // lançar excessão personalizada a ser tratada pela aplicação
            }

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e); // lançar excessão personalizada a ser tratada pela aplicação
        }
    }

    private DatabaseDefinition parseMetadata(String dbName, Connection connection) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        var catalog = connection.getCatalog();
        var schema = connection.getSchema();

        List<TableDefinition> tables = new ArrayList<>();

        // Tables
        try (ResultSet tableRs = metadata.getTables(catalog, schema, null, new String[]{"TABLE"})) {
            while (tableRs.next()) {
                String tableName = tableRs.getString("TABLE_NAME");
                List<String> tablePkCols = new ArrayList<>();

                TableDefinitionBuilder table = new TableDefinitionBuilder()
                        .setName(tableName)
                        .setSchema(tableRs.getString("TABLE_SCHEM"));


                // Primary Keys
                try (ResultSet pkRs = metadata.getPrimaryKeys(catalog, schema, tableName)) {
                    while (pkRs.next()) {
                        tablePkCols.add(pkRs.getString("COLUMN_NAME"));
                    }
                }
                table.setPrimaryKeyColumns(tablePkCols);

                // Columns
                try (ResultSet colRs = metadata.getColumns(catalog, schema, tableName, null)) {
                    while (colRs.next()) {
                        table.addColumn(
                                new ColumnDefinitionBuilder()
                                        .setName(colRs.getString("COLUMN_NAME"))
                                        .setGenericType(colRs.getInt("DATA_TYPE"))
                                        .setOriginType(colRs.getString("TYPE_NAME"))
                                        .setTargetType(null)
                                        .setDefaultValue(colRs.getString("COLUMN_DEF"))
                                        .setLength(colRs.getInt("COLUMN_SIZE"))
                                        .setPrecision(colRs.getInt("COLUMN_SIZE"))
                                        .setScale(colRs.getInt("DECIMAL_DIGITS"))
                                        .setNullable(colRs.getString("IS_NULLABLE").equalsIgnoreCase("YES"))
                                        .setAutoIncrement(colRs.getString("IS_AUTOINCREMENT").equalsIgnoreCase("YES"))
                                        .setOrdinalPosition(colRs.getInt("ORDINAL_POSITION"))
                                        .build()
                        );
                    }
                }

                // Foreign Keys
                Map<String, ForeignKeyDefinitionBuilder> fkMap = new LinkedHashMap<>();
                try (ResultSet fkRs = metadata.getImportedKeys(catalog, schema, tableName)) {
                    while (fkRs.next()) {
                        String fkName = fkRs.getString("FK_NAME");
                        String pkTableName = fkRs.getString("PKTABLE_NAME");
                        String fkColumn = fkRs.getString("FKCOLUMN_NAME");
                        String pkColumn = fkRs.getString("PKCOLUMN_NAME");

                        fkMap.computeIfAbsent(fkName, name -> new ForeignKeyDefinitionBuilder(name, pkTableName))
                                .addColumnPair(fkColumn, pkColumn);
                    }
                    table.setForeignKeys(
                            fkMap.values().stream()
                                    .map(ForeignKeyDefinitionBuilder::build)
                                    .toList()
                    );
                }

                // Unique constraints
                Map<String, List<String>> constraintMap = new LinkedHashMap<>();
                try (ResultSet uniqueRs = metadata.getIndexInfo(catalog, schema, tableName, true, false)) {
                    while (uniqueRs.next()) {
                        if (uniqueRs.getBoolean("NON_UNIQUE")) continue;

                        String indexName = uniqueRs.getString("INDEX_NAME");
                        String columnName = uniqueRs.getString("COLUMN_NAME");

                        if (indexName == null || columnName == null || tablePkCols.contains(columnName))
                            continue;

                        constraintMap.computeIfAbsent(indexName, k -> new ArrayList<>())
                                .add(columnName);
                    }
                    table.setUniqueConstraints(constraintMap.values().stream().toList());
                }

                tables.add(table.build());
            }
        }

        return new DatabaseDefinition(
                dbName,
                schema,
                tables,
                Optional.ofNullable(connection.getClientInfo("characterEncoding")).orElse("utf-8")
        );
    }

    public DatabaseDefinition extract(DbConnectionConfigDTO config) {
        try (Connection connection = databaseConnectionService.createConnection(config)) {
            var metadata = parseMetadata(config.name(), connection);
            storeToJSON(config, metadata);
            return metadata;
        } catch (SQLException e) {
            throw new RuntimeException(e); // lançar excessão customizada que será tratada pela aplicação
        }
    }

}
