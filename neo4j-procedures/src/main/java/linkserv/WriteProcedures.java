package linkserv;

import models.Output;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import constants.Constants;

import java.util.*;
import java.util.stream.Stream;

public class WriteProcedures {

    @Context
    public GraphDatabaseService db;

    @Procedure(value = "linkserv.addNodesAndRelationships", mode = Mode.WRITE)
    public Stream<Output> addNodesAndRelationships(@Name("url") String url, @Name("timestamp") String timestamp, @Name("outlinks") List outlinks) {
        ArrayList<Output> outputs = new ArrayList<>();
        StringBuilder queryBuilder = new StringBuilder("");
        String query;
        List<String> queryFragmentsList;

        if (outlinks.size() != 0) {
            List unwindList = Arrays.asList("UNWIND $outlinks AS row MATCH (c:Count) ");
            queryFragmentsList = new ArrayList<>(unwindList);
            queryFragmentsList.addAll(addNodeWithItsVersion(url, timestamp));
            queryFragmentsList.addAll(Arrays.asList(" MERGE (outlinks:", Constants.parentNodeLabel,
                    " {", Constants.nameProperty, ":row.url})", " ON MATCH SET c.node_count=c.node_count+1",
                    " MERGE (version)-[:", Constants.linkRelationshipType, "]->(outlinks)",
                    " ON MATCH SET c.relationship_count=c.relationship_count+1  "));
        } else {
            queryFragmentsList = addNodeWithItsVersion(url, timestamp);
        }

        for (String fragment : queryFragmentsList) {
            queryBuilder.append(fragment);
        }
        queryBuilder.append("RETURN parent." + Constants.nameProperty + ";");

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("outlinks", outlinks);
        query = queryBuilder.toString();
        Transaction tx = db.beginTx();
        Result result = tx.execute(query, parameters);
        tx.commit();
        while (result.hasNext()) {
            outputs.add(new Output(result.next()));
        }
        return outputs.stream();
    }

    private List addNodeWithItsVersion(String url, String timestamp) {
        return Arrays.asList("MERGE (parent:", Constants.parentNodeLabel,
                " {", Constants.nameProperty, ":\"", url, "\"}) MERGE (parent)-[:", Constants.versionRelationshipType,
                "]->(version:", Constants.versionNodeLabel, " {", Constants.versionProperty,
                ":\"", timestamp, "\"}) MERGE (parent)-[:", Constants.versionRelationshipType, "]->(version)");
    }
}
