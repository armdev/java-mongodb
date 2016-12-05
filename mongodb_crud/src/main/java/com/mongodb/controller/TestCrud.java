package com.mongodb.controller;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TestCrud {

    public static void main(String[] args) {

        mongoTestNewMethods();
        //mongoTestAuthentication();
    }

    public static void mongoTestNewMethods() {
        MongoClient mongoClient = null;

        try {
            System.out.println("Connecting using mongoTestNewMethods() to 'test' database...");
            mongoClient = new MongoClient();
            MongoDatabase db = mongoClient.getDatabase("socialnetwork");

            MongoCollection<Document> collection = db.getCollection("employee");
            System.out.println("Inserting using Map...");

            //---Insert using Map Employee #1---
            final Map<String, Object> empMap1 = new HashMap<>();
            //  empMap1.put("_id", "101");
            empMap1.put("userId", "1500");
            empMap1.put("type", "map");
            empMap1.put("first-name", "Stephen");
            empMap1.put("last-name", "Murphy");

            System.out.println("Employee: 101" + empMap1);
            collection.insertOne(new Document(empMap1));

            //---Insert using Map Employee #2---
            final Map<String, Object> empMap2 = new HashMap<>();
            // empMap2.put("_id", "102");
            empMap1.put("userId", "1600");
            empMap2.put("type", "map");
            empMap2.put("first-name", "Harold");
            empMap2.put("last-name", "Jansen");

            System.out.println("Employee: 102" + empMap2);
            collection.insertOne(new Document(empMap2));

            //Show all documents in the collection
            showAllDocuments(collection);
            findById(collection, "1500");
            findOneById(collection, "1500");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }

    public static void showAllDocuments(final MongoCollection<Document> collection) {
        System.out.println("----[Retrieve All Documents in Collection]----");
        for (Document doc : collection.find()) {
           
            System.out.println(doc.toJson());
        }
    }

    public static void findById(final MongoCollection<Document> collection, String id) {
        System.out.println("----[Retrieve All users by id in Collection]----");
        Document docs = new Document();
        docs.put("userId", id);
        for (Document doc : collection.find(docs)) {
            System.out.println(doc.toJson());
        }
    }

    public static void findOneById(final MongoCollection<Document> collection, String id) {
        System.out.println("----[Retrieve All ONE  Collection]----");
        Document docs = new Document();
        docs.put("userId", id);
        docs.put("_id", "101");
        for (Document doc : collection.find(docs)) {
            System.out.println(doc.toJson());
        }
    }

}
