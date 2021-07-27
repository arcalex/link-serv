package org.bibalex.neo4j.models;

import org.bibalex.neo4j.handlers.PropertiesHandler;
import org.bibalex.neo4j.handlers.QueryHandler;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.Values.NULL;

public class QueryResultRead extends QueryResult{

    public QueryResultRead(List<Pair<String, Value>> properties, QueryHandler queryHandler, int resultId, Record record) {
        results = convertToMap(properties, record, queryHandler, resultId);
    }

    private Map<String, Object> convertToMap(List<Pair<String, Value>> properties, Record record, QueryHandler queryHandler, int resultId) {
        Map<String, Object> fields = new HashMap<>();

        for (Pair pair : properties) {
            String queryKey = (String) pair.key();
            if (!(queryKey.contains(PropertiesHandler.getConfigProperty("idProperty")) || queryKey.contains("labels"))) {
                Value value = (Value) pair.value();
                try {
                    Node node = value.asNode();
                    Object nodeObject = handleNode(queryKey, node, queryHandler, resultId);
                    if (nodeObject != null)
                        fields.put(queryKey, nodeObject);
                } catch (Exception e1) {
                    try {
                        Path path = value.asPath();
                        Object edgeObject = handlePath(queryKey, path, null, null, queryHandler, resultId);
                        if (edgeObject != null)
                            fields.put(queryKey, edgeObject);
                    } catch (Exception e2) {
                        Object stringValue = value.asObject();
                        if (queryKey.toLowerCase().startsWith("id(r"))
                            fields.put(queryKey, resultId);
                        else if (queryKey.matches("(?i)id\\(.*?\\)") && record.get(queryKey.split("(?i)id\\(|\\)")[1] + "."+PropertiesHandler.getConfigProperty("idProperty")) != NULL) {
                            Object test = record.get(queryKey.split("(?i)id\\(|\\)")[1] + "."+PropertiesHandler.getConfigProperty("idProperty")).asObject();
                            String testStr = test.toString();
                            fields.put(queryKey, testStr.startsWith("V") ? testStr.split("V")[1] : test);
                        } else {
                            stringValue = handleProperties(queryKey, stringValue, record, queryHandler, resultId);
                            if (stringValue == "")
                                fields.put(queryKey, null);
                            else if (stringValue != null)
                                fields.put(queryKey, stringValue);
                        }
                    }
                }
            }
        }
        return fields;
    }

    public Object handleProperties(String queryKey, Object value, Record record, QueryHandler queryHandler, int resultId) {
        if (queryKey.contains(".")) {
            String key = queryKey.split("\\.")[0];
            if (record.get("labels(" + key + ")") != NULL) {
                List<Object> labels = record.get("labels(" + key + ")").asList();
                if (labels.contains(PropertiesHandler.getConfigProperty("virtualLabel"))) {
                    queryHandler.addNodeIdQueryKey(Long.valueOf(record.get(key + "."+PropertiesHandler.getConfigProperty("idProperty")).asString().replace("V", "")), queryKey);
                    queryHandler.addNodeIdResultId(Long.valueOf(record.get(key + "."+PropertiesHandler.getConfigProperty("idProperty")).asString().replace("V", "")), resultId);
                    return null;
                } else {
                    return (value == null) ? "" : value;
                }
            } else {
                return null;
            }
        } else {
            return record.get(queryKey.split("\\(")[0].split("\\)")[0]).asObject();
        }
    }

}
