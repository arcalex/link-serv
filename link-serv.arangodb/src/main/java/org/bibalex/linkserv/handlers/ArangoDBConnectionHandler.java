package org.bibalex.linkserv.handlers;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.*;
import com.arangodb.model.*;
import com.arangodb.util.MapBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

import static java.util.Objects.isNull;
import static org.bibalex.linkserv.handlers.PropertiesHandler.getProperty;
import static org.bibalex.linkserv.handlers.PropertiesHandler.initializeProperties;

public class ArangoDBConnectionHandler {
    private static final Logger LOGGER = LogManager.getLogger(ArangoDBConnectionHandler.class);
    private Integer numberOfShards;
    private Integer replicationFactor;
    private String databaseName;
    private String graphName;
    private String nodesCollectionName;
    private String edgesCollectionName;
    private ArangoDB arangoDB;
    private ArangoDatabase linkData;
    public ArangoCollection nodesCollection;
    private String nameKey;
    private String versionKey;
    private String sourceAttribute;
    private String targetAttribute;
    private static final ArangoDBConnectionHandler arangoDBConnectionHandler = new ArangoDBConnectionHandler();

    private ArangoDBConnectionHandler(){
        initializeProperties();
        this.numberOfShards = Integer.valueOf(getProperty("numberOfShards"));
        this.replicationFactor = Integer.valueOf(getProperty("replicationFactor"));
        this.databaseName = getProperty("databaseName");
        this.graphName = getProperty("graphName");
        this.nodesCollectionName = getProperty("nodesCollection");
        this.edgesCollectionName = getProperty("edgesCollection");
        this.nameKey = getProperty("nameKey");
        this.versionKey = getProperty("versionKey");
        this.sourceAttribute = getProperty("sourceAttribute");
        this.targetAttribute = getProperty("targetAttribute");
        initialize();
        this.nodesCollection = linkData.collection(nodesCollectionName);
    }

    public static ArangoDBConnectionHandler getInstance(){
        return arangoDBConnectionHandler;
    }

    private void initialize() {
        // check if link-data is present, and create one if not
        buildArangoDB();
        linkData = arangoDB.db(databaseName).exists() ? arangoDB.db(databaseName) : createLinkDataGraph(arangoDB);
    }

    private void buildArangoDB() {
        InputStream arangodbProperties = ArangoDBHandler.class.getResourceAsStream("arangodb.properties");
        arangoDB = new ArangoDB.Builder()
                .loadProperties(arangodbProperties)
                .acquireHostList(true)
                .loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN)
                .build();
    }

    private void checkConnection(){
        if(isNull(arangoDB)) {
            initialize();
        }
        else if (!linkData.exists()){
            linkData = createLinkDataGraph(arangoDB);
        }
    }

    private ArangoDatabase createLinkDataGraph(ArangoDB arangoDB) {
        // create link-data
        LOGGER.info("Database " + databaseName + " not found, creating new");

        arangoDB.db(databaseName).create();
        linkData = arangoDB.db(databaseName);

        linkData.collection("duplicates").create(new CollectionCreateOptions());
        BaseDocument duplicate = new BaseDocument();
        duplicate.addAttribute("count",0);
        linkData.collection("duplicates").insertDocument(duplicate);

        // create sharded nodes collection with unique index over identifier and timestamp attributes
        linkData.collection(nodesCollectionName).
                create(new CollectionCreateOptions().
                    shardKeys(nameKey, versionKey).
                    numberOfShards(numberOfShards).
                    replicationFactor(replicationFactor));

        linkData.collection(nodesCollectionName).
                ensurePersistentIndex(Arrays.asList(nameKey, versionKey),
                        new PersistentIndexOptions().unique(true));

        // create edges collection with unique index over _to and _from attributes
        linkData.collection(edgesCollectionName).
                create(new CollectionCreateOptions().
                        type(CollectionType.EDGES).
                        numberOfShards(numberOfShards).
                        shardKeys(sourceAttribute, targetAttribute).
                        replicationFactor(replicationFactor));

        EdgeDefinition linkedToEdgeDefinition = new EdgeDefinition().
                collection(edgesCollectionName).
                from(nodesCollectionName).
                to(nodesCollectionName);

        linkData.collection(edgesCollectionName).
                ensurePersistentIndex(Arrays.asList(sourceAttribute, targetAttribute),
                        new PersistentIndexOptions().unique(true));

        LOGGER.info("Database " + databaseName + " created successfully");

        // create link-data-graph
        linkData.createGraph(graphName,
                new ArrayList<>(Arrays.asList(linkedToEdgeDefinition)),
                new GraphCreateOptions().numberOfShards(numberOfShards));

        LOGGER.info("Graph " + graphName + " created successfully");
        return linkData;
    }

    public ArangoCursor<Object> executeLinkDataQuery(String query, MapBuilder paramMapBuilder) {
        checkConnection();
//        while(true) {
            try {
                return linkData.query(
                        query,
                        paramMapBuilder.get(),
                        new AqlQueryOptions(),
                        Object.class
                );
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//                return null;
//            }
//        }
    }

    public MultiDocumentEntity<DocumentCreateEntity<BaseDocument>> insertNodes(String collectionName, ArrayList<BaseDocument> nodesToInsert) {
        checkConnection();
        return linkData.collection(collectionName).
                insertDocuments(nodesToInsert,
                        new DocumentCreateOptions().
                                returnNew(true).
                                overwriteMode(OverwriteMode.ignore));

    }

    public void insertEdges(String collectionName, ArrayList<BaseEdgeDocument> edgesToInsert) {
        ArrayList<BaseEdgeDocument> batch = new ArrayList<>();
        for(int i = 0; i < edgesToInsert.size(); i++){
            if(!(batch.isEmpty()) && (i % 100 == 0)) {
                importBatch(batch, collectionName);
                batch = new ArrayList<>();
            }
            batch.add(edgesToInsert.get(i));
        }
        importBatch(batch, collectionName);
    }

    private void importBatch(ArrayList<BaseEdgeDocument> edgesToInsert, String collectionName) {
        checkConnection();
        linkData.collection(collectionName).
                insertDocuments(edgesToInsert,
                        new DocumentCreateOptions().
                                overwriteMode(OverwriteMode.ignore));
    }
}
