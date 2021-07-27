package org.bibalex.neo4j.models;

import org.bibalex.neo4j.handlers.PropertiesHandler;
import org.bibalex.neo4j.handlers.WriteHandler;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.util.Pair;

import java.util.List;

import static org.neo4j.driver.Values.NULL;

public class QueryResultWrite extends QueryResult{

    public QueryResultWrite(List<Pair<String, Value>> properties, WriteHandler writeHandler, Record record, int shard) {
        handleWrite(properties, writeHandler, record, shard);
    }

    private void handleWrite(List<Pair<String, Value>> properties, WriteHandler writeHandler, Record record, int shard) {
        for (Pair pair : properties) {
            String queryKey = (String) pair.key();
            String queryKeyPart = queryKey.contains(".") ? queryKey.split("\\.")[0] : queryKey;

            if (writeHandler.getUnwindUsage().containsKey(queryKeyPart) && !(record.get(queryKeyPart + "."+ PropertiesHandler.getConfigProperty("idProperty")).asObject() == null)) {
                writeHandler.removeFromUnwindProperties("\"" +
                        record.get(writeHandler.getUnwindKey()).asString() + "\"");
            }
            if (writeHandler.getRelationshipKeys().contains(queryKeyPart)) {
                handleReturnRelationship(record, queryKeyPart, writeHandler, shard);
            }

        }
    }

    void handleReturnRelationship(Record record, String key, WriteHandler writeHandler, int shard) {
        if (record.get(key) != NULL) {
            Object nodeId = record.get(key).asNode().get(PropertiesHandler.getConfigProperty("idProperty")).asObject();
            String nodeIdStr = nodeId.toString();

            if (!checkIfNodeIsVirtual(nodeIdStr, key, writeHandler, -1)) {
                //get property and check if it original property
                String originalQueryRelationship = writeHandler.getOriginalRelationShipQuery();

                if(originalQueryRelationship.startsWith(key)){
                    writeHandler.addToFoundKeyWithShardAndID(key, Long.valueOf(nodeIdStr), shard);
                }
                else if (originalQueryRelationship.contains(key)) {
                    String matchingProperty = originalQueryRelationship.split("\\{|\\}")[1];
                    String[] keyProperty = matchingProperty.split("\\s*:\\s*|,", 2);
                    if (keyProperty.length > 2) {
                        //to handle later
                    } else if (keyProperty.length == 2) {
                        String str = "\"" + record.get(key).asNode().get(keyProperty[0]).asString() + "\"";
                        if (str.replaceAll("\\s+","").equals(keyProperty[1].replaceAll("\\s+",""))) {
                            writeHandler.removeFromRelationshipKeys(key);
                            writeHandler.addToFoundKeyWithShardAndID(key, Long.valueOf(nodeIdStr), shard);
                        }
                    } else {
                        writeHandler.addToFoundKeyWithShardAndID(key, Long.valueOf(nodeIdStr), shard);
                    }
                } else {
                    writeHandler.addToFoundKeyWithShardAndID(key, Long.valueOf(nodeIdStr), shard);
                }
            }
        }
    }
}
