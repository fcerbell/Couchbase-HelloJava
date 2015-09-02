package net.cerbelle;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) {

        Logger logger = Logger.getLogger("com.couchbase.client");
        logger.setLevel(Level.WARNING);

        Cluster cluster = CouchbaseCluster.create("192.168.56.101", "192.168.56.102");
        Bucket bucket = cluster.openBucket("default");

        JsonObject user = JsonObject.empty()
                .put("firstname", "Walter")
                .put("lastname", "White")
                .put("job", "chemistry teacher")
                .put("age", 50);
        JsonDocument doc = JsonDocument.create("walter", user);
        JsonDocument response = bucket.upsert(doc);

        JsonDocument walter = bucket.get("walter");
        System.out.println("Found: " + walter);

        System.out.println("Age: " + walter.content().getInt("age"));

        final CountDownLatch latch = new CountDownLatch(1);
        bucket
                .async()
                .get("walter")
                .flatMap(loaded -> {
                    loaded.content().put("age", 52);
                    return bucket.async().replace(loaded);
                })
                .subscribe(
                        System.out::println,
                        err -> {
                            err.printStackTrace();
                            latch.countDown();
                        },
                        latch::countDown
                );

        walter = bucket.get("walter");
        System.out.println("Found: " + walter);

        JsonDocument result = bucket
                .async()
                .get("walter")
                .flatMap(loaded -> {
                    loaded.content().put("age", 50);
                    return bucket.async().replace(loaded);
                })
                .toBlocking()
                .single();

        walter = bucket.get("walter");
        System.out.println("Found: " + walter);

        JsonDocument removed = bucket.remove(walter);

        walter = bucket.get("walter");
        System.out.println("Found: " + walter);

        bucket.close();
        cluster.disconnect();
    }
}
