package helpers;

import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class Neo4jHelper {

    private Driver driver;
    private Session session;

    public Neo4jHelper(){
        initializeDriverAndSession();
    }

    public Result runQuery(String query){
        if(!session.isOpen()){
            session = driver.session();
        }
        return session.run(query);
    }

    private void initializeDriverAndSession(){
        driver = GraphDatabase.driver("bolt://localhost:7687");
        session = driver.session();
    }

}
