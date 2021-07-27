package org.bibalex.neo4j.helpers;

import org.bibalex.neo4j.handlers.PropertiesHandler;
import org.neo4j.driver.*;

import java.util.ArrayList;

public class Neo4jHelper {

    private ArrayList<Driver> drivers;
    private ArrayList<Session> sessions;

    public Neo4jHelper(){
        drivers = new ArrayList<>();
        sessions = new ArrayList<>();
        initializeDriverAndSession();
    }

    public Result runQuery(String query, Value parametersValue, int index){
        Session session = sessions.get(index);
        if(!session.isOpen()){
            System.out.println("new session " + index);
            session = drivers.get(index).session();
            sessions.set(index, session);
        }
        return session.run(query, parametersValue);
    }

    private void initializeDriverAndSession(){
        String[] neo4jInstances = PropertiesHandler.getClusterProperty("neo4jInstances").split(",");
        for(String neo4jInstance : neo4jInstances){
            Driver driver = GraphDatabase.driver(neo4jInstance);
            drivers.add(driver);
            sessions.add(driver.session());
        }
    }

}
