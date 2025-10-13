package com.gabrielrq.database_converter.util;

import com.gabrielrq.database_converter.domain.ForeignKeyDefinition;
import com.gabrielrq.database_converter.domain.TableDefinition;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableDependencyResolver {

    public static List<TableDefinition> sortTablesByDependency(List<TableDefinition> tables) {
        Map<String, TableDefinition> tableMap = tables.stream()
                .collect(Collectors.toMap(TableDefinition::name, Function.identity()));

        // Mapa de dependência: quem depende de quem (reverso)
        Map<String, List<String>> dependencies = new HashMap<>();
        // Contador de quantas dependências cada tabela tem (grau de entrada)
        Map<String, Integer> inDegree = new HashMap<>();

        // Inicializa
        for (TableDefinition table : tables) {
            dependencies.putIfAbsent(table.name(), new ArrayList<>());
            inDegree.putIfAbsent(table.name(), 0);
        }

        // Constroi o grafo de dependências
        for (TableDefinition table : tables) {
            for (ForeignKeyDefinition fk : table.foreignKeys()) {
                String from = table.name(); // tabela atual (filha)
                String to = fk.referencedTable(); // tabela referenciada (pai)

                if (!tableMap.containsKey(to)) continue; // ignora referências externas

                dependencies.get(to).add(from); // to ➝ from
                inDegree.put(from, inDegree.getOrDefault(from, 0) + 1);
            }
        }

        // Fila de nós com grau 0 (sem dependência)
        Deque<String> queue = new ArrayDeque<>();
        for (var entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        List<TableDefinition> sorted = new ArrayList<>();

        while (!queue.isEmpty()) {
            String current = queue.poll();
            sorted.add(tableMap.get(current));

            for (String dependent : dependencies.getOrDefault(current, List.of())) {
                inDegree.put(dependent, inDegree.get(dependent) - 1);
                if (inDegree.get(dependent) == 0) {
                    queue.add(dependent);
                }
            }
        }

        // Verifica se houve ciclo (graus restantes > 0)
        if (sorted.size() != tables.size()) {
            throw new IllegalStateException("Ciclo detectado entre tabelas! Verifique FKs circulares.");
        }

        return sorted;
    }
}

