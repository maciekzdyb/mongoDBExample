package org.example;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;

import com.mongodb.client.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * mongoDB Example
 *
 */
public class App {
    public static void main(String[] args) {
        String uri = "mongodb://localhost:27023"; // docker container

        //        try (MongoClient mongoClient = MongoClients.create(uri)) {
//            MongoDatabase database = mongoClient.getDatabase("test");
//            MongoCollection<Document> collection = database.getCollection("world");
//            Bson projectionFields = Projections.fields(
//                    Projections.include("regionname", "project_name", "totalamt"),
//                    Projections.excludeId());
//            Document doc = collection.find(eq("projectstatusdisplay", "Active"))
//                    .projection(projectionFields)
//                    .sort(Sorts.descending("totalamt"))
//                    .first();
//            if (doc == null) {
//                System.out.println("No results found.");
//            } else {
//                System.out.println(doc.toJson());
//            }
//        }

//        try (MongoClient mongoClient = MongoClients.create(uri)) {
//            MongoDatabase database = mongoClient.getDatabase("test");
//            MongoCollection<Document> collection = database.getCollection("world");
//            Bson projectionFields = fields(
//                    include("regionname", "project_name", "totalamt"),
//                    excludeId());
//            MongoCursor<Document> cursor = collection.find(lt("totalamt", 10000000))
//                    .projection(projectionFields)
//                    .sort(descending("title")).iterator();
//            try {
//                while (cursor.hasNext()) {
//                    System.out.println(cursor.next().toJson());
//                }
//            } finally {
//                cursor.close();
//            }
//
//        }

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("test");
            MongoCollection<Document> collection = database.getCollection("world");
            topTenAmmount(collection);
            topTenRegions(collection);
        } catch (Exception e){

        }


    }
    /**
     * 10 regionów w których zainwestowano najwięcej pieniędzy.
     * @param collections - kolekcja z bazy MongoDB.
     */
    private static void topTenAmmount(MongoCollection<Document> collections) {
        Bson group = group("$regionname", sum("totalamt", "$totalamt"));
        Bson project = project(fields(excludeId(), include("regionname","totalamt"), computed("regionname", "$_id")));
        Bson sort = sort(descending("totalamt"));
        Bson limit = limit(10);

        List<Document> results = collections.aggregate(Arrays.asList(group, project, sort, limit))
                .into(new ArrayList<Document>());
        System.out.println("==> 10 regionów w których zainwestowano najwięcej pieniędzy:");
        results.forEach(System.out::println);
    }

    /**
     * 10 regionów w których zostało zorganizowane najwięcej projektów.
     * @param collections - kolekcja z bazy MongoDB.
     */
    private static void topTenRegions(MongoCollection<Document> collections) {
        Bson group = group("$regionname", sum("sum",1));
        Bson project = project(fields(excludeId(), include("regionname","sum"), computed("regionname", "$_id")));
        Bson sort = sort(descending("sum"));
        Bson limit = limit(10);

        List<Document> results = collections.aggregate(Arrays.asList(group, project, sort, limit))
                .into(new ArrayList<Document>());
        System.out.println("==> 10 regionów w których zostało zorganizowane najwięcej projektów:");
        results.forEach(System.out::println);
    }

}
