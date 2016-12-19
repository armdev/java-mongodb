package com.mongodb.configs;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

public interface MongoTemplate {
    
    MongoCollection<Document> userColl();
}
