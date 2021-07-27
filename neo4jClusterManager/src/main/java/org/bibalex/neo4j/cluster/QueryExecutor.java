package org.bibalex.neo4j.cluster;

import org.bibalex.neo4j.handlers.*;
import org.bibalex.neo4j.helpers.Neo4jHelper;
import org.bibalex.neo4j.models.QueryResult;
import org.neo4j.driver.Value;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.sql.SQLOutput;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.concurrent.Executors.*;

public class QueryExecutor {
    public static ExecutorService es;
    public static Neo4jHelper neo4jHelper;

    @Procedure(value = "cluster.executeQuery", mode = Mode.WRITE)
    public Stream<QueryResult> executeQuery(@Name("query") String query, @Name("parameters") Map<String, Object> parameters) {
        for(Map.Entry<String, Object> entry : parameters.entrySet()){
            System.out.println(entry.getValue());
        }
        PropertiesHandler.initializeProperties();
        if(es == null) {
            System.out.println("new thread pool");
            es = newWorkStealingPool(20);
        }
        if(neo4jHelper == null){
            neo4jHelper = new Neo4jHelper();
        }

        Pattern pattern = Pattern.compile("\\$\\w+");
        Matcher matcher = pattern.matcher(query);
        while (matcher.find())
        {
            String param = matcher.group().split("\\$")[1];
            if(parameters.get(param).getClass().equals(String.class))
                query = query.replaceAll("\\$"+param, "\"" + parameters.get(param).toString() + "\"");
            else
                query = query.replaceAll("\\$"+param, parameters.get(param).toString());
        }

        if (query.toLowerCase().contains("merge") || query.toLowerCase().contains("create")) {
            WriteHandler writeHandler = new WriteHandler(es, neo4jHelper);
            return writeHandler.executeWrite(query, null).stream();
        } else {
            ReadHandler readHandler = new ReadHandler(es, neo4jHelper);
            return readHandler.executeRead(query).stream();
        }
    }
}
