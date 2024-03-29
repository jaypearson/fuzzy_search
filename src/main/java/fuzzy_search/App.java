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

@Command(
  version     = "Fuzzy Search Demo 1.0",
  header      = "%nFuzzy Search Demo%n",
  description = "Prints usage help and version help when requested.%n"
)
public class App implements Runnable {

    private final int BULK_RETRY_TIMES               = 10;
    private final int BULK_WRITE_WAIT_MULTIPLIER     = 50;
    private final int SERVER_BACKPRESSURE_ERROR_CODE = 82;

    @Option(
      names       = { "-h", "--help" },
      usageHelp   = true,
      description = "display a help message"
    )
    private boolean helpRequested = false;

    @Option(
      names       = { "-V", "--version" },
      versionHelp = true,
      description = "display version info"
    )
    boolean versionInfoRequested;

    @Option(
      names       = { "--uri" },
      paramLabel  = "<uri>",
      required    = false,
      description = "connection string for destination MongoDB (default: ${DEFAULT-VALUE})"
    )
    String uri = "mongodb://localhost:27017/";

    @Option(
      names       = { "-d", "--dbName" },
      required    = false,
      paramLabel  = "<database name>",
      description = "name of destination database (default: ${DEFAULT-VALUE})"
    )
    String dbName = "demo";

    @Option(
      names       = { "-c", "--collectionName" },
      required    = false,
      paramLabel  = "<collection name>",
      description = "name of destination collection (default: ${DEFAULT-VALUE})"
    )
    String collectionName = "fuzzy";

    @Option(
      names       = { "-b", "--build" },
      required    = false,
      paramLabel  = "<field to soundex>",
      description = "path of field to build soundex values for; include multiple times for more fields"
    )
    List<String> soundexFields;

    @Option(
      names       = { "-s", "--search" },
      required    = false,
      paramLabel  = "<search predicate>",
      description = "soundex search predicate, include multiple times for more predicates"
    )
    List<String> searchValues;

    @Option(
      names       = { "-i", "--index" },
      required    = false,
      paramLabel  = "build indexes",
      description = "flag indicating to build soundex index (default: false)"
    )
    boolean buildIndexes = false;

    private Soundex _soundex = new Soundex();

    private void genIndexes(MongoCollection<Document> collection) {
        List<IndexModel> indexes = new ArrayList<>();
        indexes.add(
            new IndexModel(
                new Document().append("soundex", 1),
                new IndexOptions().name("soundexIndex")
            )
        );
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
        // Loop through provided predicates and calculate soundex encoding
        for (String predicate : searchValues) {
            soundexValues.add(_soundex.encode(predicate));
        }

        System.out.println("Querying for predicates: " + searchValues);
        System.out.println("Soundex values: " + soundexValues);

        // Create find cursor using $in with soundex encoded values
        FindIterable<Document> cursor = collection.find(in("soundex", soundexValues));

        // Loop through cursor and output matching documents
        for (Document d : cursor) {
            System.out.println(d.toJson());
        }
    }

    private void ping(MongoDatabase database) {
        System.out.println("Pinging database...");
        final long startTime = System.currentTimeMillis();
        // MongoDB Drivers are "lazy". The ping command forces it to connect to the DB.
        database.runCommand(new Document("ping", 1));
        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Ping time: " + duration + "ms");
        System.out.printf("Initial connection time: %d ms%n", duration);
    }

    private void buildSoundexField(MongoDatabase database, MongoCollection<Document> collection) {
        // Store for metrics
        final long startTime = System.currentTimeMillis();

        // Open a collection scan cursor
        // Configured batch size as 100 to match bulk write ops batch size
        // Configured maxAwaitTime to short circuit default long wait on getMore ops
        FindIterable<Document> cursor = collection.find().batchSize(100).maxAwaitTime(5, TimeUnit.SECONDS);

        // Collection to contain bulk write operations
        ArrayList<UpdateOneModel<Document>> bulkOps = new ArrayList<UpdateOneModel<Document>>();

        // Loop through cursor
        cursor.iterator().forEachRemaining((doc) -> {
            try {
                // Build bulk updateOne model
                // Matches by _id of each doc
                // Uses addEachToSet to add soundex encoded values to 'soundex' array
                UpdateOneModel<Document> model = new UpdateOneModel<Document>(eq("_id", doc.get("_id")),
                        addEachToSet("soundex", generateSoundex(doc)));
                // Add bulk write model to collection
                bulkOps.add(model);
                // Check for full batch of bulk write operations
                if (bulkOps.size() == 100) {
                    // Submit the bulk write operation
                    if (submitBulkOps(collection, bulkOps)) {
                      bulkOps.clear();
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        // Clean up bulk operations collection for any left over operations
        if (bulkOps.size() > 0) {
            submitBulkOps(collection, bulkOps);
        }
        // Display metrics
        long duration = System.currentTimeMillis() - startTime;
        System.out.println("End time: " + new Date() + " Duration: " + duration + "ms");
        System.out.printf("=============== FINISHED =================%n");
    }

    private void buildIndexes(MongoDatabase database) {
        final long startTime = System.currentTimeMillis();
        System.out.println("============= BEGIN CREATING INDEXES ==============");
        genIndexes(database.getCollection(collectionName));
        System.out.printf("Created indexes in: %d ms%n", System.currentTimeMillis() - startTime);
        System.out.println("============= END CREATING INDEXES ==============");
    }
    private boolean submitBulkOps(final MongoCollection<Document> collection, final List<UpdateOneModel<Document>> bulkOps) {
        return submitBulkOps(collection, bulkOps, 1);
    }

    private boolean submitBulkOps(final MongoCollection<Document> collection, final List<UpdateOneModel<Document>> bulkOps, final int iterations) {
        boolean success = false;
        if (iterations > BULK_RETRY_TIMES) {
            System.out.println("ERROR: Exceeded " + BULK_RETRY_TIMES + " tries to submit bulk writes...");
            return success;
        }
        try {
            // Submit bulk write operation
            collection.bulkWrite(bulkOps);
            success = true;
        } catch (MongoBulkWriteException ex) {
            // Code 82 indicates that the server did NOT apply any Ops
            // This represents backpressure and the submission should be retried
            if (ex.getCode() == SERVER_BACKPRESSURE_ERROR_CODE) {
                System.out.println("No progress made submitting ops...");
            } else {
                System.out.println("BulkWriteException: " + ex.getMessage());
            }
            try {
                // Increasing wait times for each failed write
                Thread.sleep(BULK_WRITE_WAIT_MULTIPLIER * iterations);
                return submitBulkOps(collection, bulkOps, iterations + 1);
            } catch (InterruptedException iex) {
                // Ignore Thread InterruptedException
            }
        } catch (MongoException ex) {
            ex.printStackTrace();
        }
        return success;
    }

    private List<String> generateSoundex(Document doc) {
        List<String> results = new ArrayList<>();
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