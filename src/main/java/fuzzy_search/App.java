package fuzzy_search;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

// MongoDB imports
import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOneModel;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import org.bson.Document;

// Apache Soundex codec
import org.apache.commons.codec.language.Soundex;

// Command line parsing imports
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(version = "Fuzzy Search Demo 1.0", header = "%nFuzzy Search Demo%n", description = "Prints usage help and version help when requested.%n")
public class App implements Runnable {
    @Option(names = { "-h", "--help" }, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    @Option(names = { "-V", "--version" }, versionHelp = true, description = "display version info")
    boolean versionInfoRequested;

    @Option(names = {
            "--uri" }, paramLabel = "<uri>", required = false, description = "connection string for destination MongoDB (default: ${DEFAULT-VALUE})")
    String uri = "mongodb://localhost:27017/";

    @Option(names = { "-d",
            "--dbName" }, required = false, paramLabel = "<database name>", description = "name of destination database (default: ${DEFAULT-VALUE})")
    String dbName = "demo";

    @Option(names = { "-c",
            "--collectionName" }, required = false, paramLabel = "<collection name>", description = "name of destination collection (default: ${DEFAULT-VALUE})")
    String collectionName = "fuzzy";

    @Option(names = { "-b",
            "--build" }, required = false, paramLabel = "<field to soundex>", description = "path of field to build soundex values for; include multiple times for more fields")
    List<String> soundexFields;

    @Option(names = { "-s",
            "--search" }, required = false, paramLabel = "<search predicate>", description = "soundex search predicate, include multiple times for more predicates")
    List<String> searchValues;

    @Option(names = { "-i",
            "--index" }, required = false, paramLabel = "build indexes", description = "flag indicating to build soundex index (default: false)")
    boolean buildIndexes = false;

    private Soundex _soundex = new Soundex();

    private void genIndexes(MongoCollection<Document> collection) {
        ArrayList<IndexModel> indexes = new ArrayList<IndexModel>();
        Document index = new Document();
        index.append("soundex", 1);
        indexes.add(new IndexModel(index, new IndexOptions().name("soundexIndex")));

        collection.createIndexes(indexes);
    }

    public void run() {
        // Create connection string
        ConnectionString connectionString = new ConnectionString(uri);

        // Initiate client settings
        Builder builder = MongoClientSettings.builder();
        MongoClientSettings settings = builder.applyConnectionString(connectionString).build();

        // Instantiate client
        MongoClient mongoClient = MongoClients.create(settings);

        // Get database
        String connDbName = connectionString.getDatabase();
        MongoDatabase database;
        if (connDbName != null && connDbName.length() > 0) {
            database = mongoClient.getDatabase(connDbName);
        } else if (dbName != null && dbName.length() > 0) {
            database = mongoClient.getDatabase(dbName);
        } else {
            System.err.println("A database name is required in either the connection string or the dbName option!");
            return;
        }

        ping(database);

        MongoCollection<Document> collection = database.getCollection(collectionName);

        if (soundexFields != null && soundexFields.size() > 0) {
            buildSoundexField(database, collection);
        }

        if (buildIndexes) {
            buildIndexes(database);
        }

        if (searchValues != null && searchValues.size() > 0) {
            search(collection);
        }
    }

    private void search(MongoCollection<Document> collection) {
        List<String> soundexValues = new ArrayList<String>();
        for (String predicate : searchValues) {
            soundexValues.add(_soundex.encode(predicate));
        }

        System.out.println("Querying for predicates: " + searchValues);
        System.out.println("Soundex values: " + soundexValues);
        FindIterable<Document> cursor = collection.find(in("soundex", soundexValues));
        for (Document d : cursor) {
            System.out.println(d.toJson());
        }
    }

    private void ping(MongoDatabase database) {
        System.out.println("Pinging database...");
        long startTime = System.currentTimeMillis();
        // The MongoDB Drivers are "lazy". The ping command forces it to connect to the
        // DB.
        database.runCommand(new Document("ping", 1));
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("Ping time: " + duration + "ms");
        System.out.printf("Initial connection time: %d ms%n", duration);
    }

    private void buildSoundexField(MongoDatabase database, MongoCollection<Document> collection) {
        long startTime = System.currentTimeMillis();
        FindIterable<Document> cursor = collection.find().batchSize(100).maxAwaitTime(5, TimeUnit.SECONDS);
        ArrayList<UpdateOneModel<Document>> bulkOps = new ArrayList<UpdateOneModel<Document>>();
        cursor.iterator().forEachRemaining((doc) -> {
            try {
                UpdateOneModel<Document> model = new UpdateOneModel<Document>(eq("_id", doc.get("_id")),
                        addEachToSet("soundex", generateSoundex(doc)));
                bulkOps.add(model);
                if (bulkOps.size() == 100) {
                    submitBulkOps(collection, bulkOps);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        if (bulkOps.size() > 0) {
            submitBulkOps(collection, bulkOps);
        }
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        buildIndexes(database);
        System.out.println("End time: " + new Date() + " Duration: " + duration + "ms");
        System.out.printf("=============== FINISHED =================%n");
    }

    private void buildIndexes(MongoDatabase database) {
        long startTime = System.currentTimeMillis();
        System.out.println("============= BEGIN CREATING INDEXES ==============");
        genIndexes(database.getCollection(collectionName));
        System.out.printf("Created indexes in: %d ms%n", System.currentTimeMillis() - startTime);
        System.out.println("============= END CREATING INDEXES ==============");
    }

    private void submitBulkOps(MongoCollection<Document> collection, ArrayList<UpdateOneModel<Document>> bulkOps) {
        int iterations = 0;
        while (true) {
            if (iterations > 10) {
                System.out.println("ERROR: Exceeded 10 tries to submit bulk writes...");
                return;
            }
            try {
                iterations++;
                collection.bulkWrite(bulkOps);
            } catch (MongoBulkWriteException ex) {
                if (ex.getCode() == 82) {
                    System.out.println("No progress made submitting ops...");
                } else {
                    System.out.println("BulkWriteException: " + ex.getMessage());
                }
                try {
                    // Increasing wait times for each failed write
                    Thread.sleep(50 * iterations);
                } catch (InterruptedException iex) {
                    // Ignore Thread InterruptedException
                }
                continue;
            } catch (MongoException ex) {
                ex.printStackTrace();
                break;
            }
            bulkOps.clear();
            break;
        }
    }

    private List<String> generateSoundex(Document doc) {
        List<String> results = new ArrayList<String>();
        // Loop through each field indicated to build soundex values for
        for (String field : soundexFields) {
            String[] path = field.split(Pattern.quote("."));
            // Get top level sub array
            List<Document> subArray = doc.getList(path[0], Document.class);

            for (Document entry : subArray) {
                // Extract field value in each array entry
                String value = entry.getString(path[1]);

                // Sanity check
                if (value == null || value.length() == 0)
                    continue;

                // Build soundex only for individual tokens/words
                String[] values = value.split(" ");

                // Loop through tokens
                for (String v : values) {
                    // Encode Soundex value of token
                    String s = _soundex.encode(v);

                    // Append distinct Soundex values
                    if (s != "" && !results.contains(s)) {
                        results.add(s);
                    }
                }
            }
        }
        return results;
    }

    public static void main(String[] args) {
        CommandLine.run(new App(), args);
    }
}