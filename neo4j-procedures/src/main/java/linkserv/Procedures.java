package linkserv;

import helpers.Neo4jHelper;
import org.neo4j.driver.Result;

public class Procedures {

    public static Neo4jHelper neo4jHelper;

    Result runNeo4jQuery(String query){
        if(neo4jHelper == null)
            neo4jHelper = new Neo4jHelper();
       return neo4jHelper.runQuery(query);
    }
}
