package com.mongodb.configs;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Indexes.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.model.User;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author armenar
 */
public class MongoDBConnect {

    private final MongoOptionsFactory factory;
    private final MongoFactory mongoFactory;
    private MongoClient mongoInstance;
    private MongoCollection<Document> collection;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

    public MongoDBConnect() {
        //options
        factory = new MongoOptionsFactory();
        mongoFactory = new MongoFactory();
        //set options
        mongoFactory.setMongoOptions(factory.createMongoOptions());
    }

    public static void main(String args[]) {
        UUID randomEmail = UUID.randomUUID();
        MongoDBConnect mongo = new MongoDBConnect();
        mongo.initMongo();
        User user = new User(randomEmail.toString().substring(4), randomEmail.toString().substring(6), randomEmail.toString().substring(5) + "@gmail.com");
        User retUser = mongo.saveUser(user);
        User foundUser = mongo.findById(user.getId());

        List<User> all = mongo.findAll();
        all = mongo.findAllSorted();
        all = mongo.iterateAll();
        mongo.getCollectionCount();
        String update = mongo.updateEmail(foundUser.getId(), "alola@gmail.com");
        System.out.println("update email returned message  " + update);

        foundUser.setLastname("Aloha!!!!");
        mongo.updateAllUserModel(foundUser.getId(), foundUser);
        for (User u : all) {
            System.out.println(u.getFirstname() + " / " + u.getLastname() + " / " + u.getEmail());
        }
    }

    public void initMongo() {
        //connect with DB
        mongoInstance = mongoFactory.createMongo();
        MongoDatabase db = mongoInstance.getDatabase("socialnetwork");
        collection = db.getCollection("users");

    }

    public User saveUser(User user) {
        final Map<String, Object> userDTO = new HashMap<>();
        UUID userId = UUID.randomUUID();
        user.setId(userId.toString());

        user.setRegisteredDate(formatDateValue(new Date(System.currentTimeMillis())));
        userDTO.put("id", user.getId());
        userDTO.put("firstname", user.getFirstname());
        userDTO.put("lastname", user.getLastname());
        userDTO.put("email", user.getEmail());
        userDTO.put("registeredDate", user.getRegisteredDate());

        collection.insertOne(new Document(userDTO));

        return user;
    }

    public String formatDateValue(Date datePart) {
        if (datePart == null) {
            return null;
        }
        return dateFormat.format(datePart);
    }

    public User findById(String id) {
        System.out.println("----[Retrieve  user by id in Collection]----");
        Document docs = new Document();
        docs.put("id", id);
        User user = null;
        for (Document doc : collection.find(docs)) {
            user = new User();
            user.setEmail(doc.get("email", String.class));
            user.setFirstname(doc.get("firstname", String.class));
            user.setLastname(doc.get("lastname", String.class));
            user.setId(doc.get("id", String.class));
            user.setRegisteredDate(doc.get("registeredDate", String.class));
        }

        System.out.println("Found user by id " + user.getId());
        return user;
    }

    public List<User> findAll() {
        System.out.println("----[Retrieve All USERS  Collection]----");
        List<User> userList = new ArrayList<>();
        User user = null;
        for (Document doc : collection.find()) {
            user = new User();
            user.setEmail(doc.get("email", String.class));
            user.setFirstname(doc.get("firstname", String.class));
            user.setLastname(doc.get("lastname", String.class));
            user.setId(doc.get("id", String.class));
            user.setRegisteredDate(doc.get("registeredDate", String.class));
            userList.add(user);
        }
        System.out.println("findAll size " + userList.size());
        return userList;
    }

    public List<User> findAllSorted() {
        List<User> userList = new ArrayList<>();
        User user = null;
        BasicDBObject query = new BasicDBObject();
        //add query
        for (Document doc : collection.find(query).sort(orderBy(descending("registeredDate"))).skip(0).limit(1000)) {
            user = new User();
            user.setEmail(doc.get("email", String.class));
            user.setFirstname(doc.get("firstname", String.class));
            user.setLastname(doc.get("lastname", String.class));
            user.setId(doc.get("id", String.class));
            user.setRegisteredDate(doc.get("registeredDate", String.class));
            userList.add(user);
        }
        System.out.println("findAllSorted size " + userList.size());
        return userList;
    }

    public String updateEmail(String userId, String newEmail) {
        UpdateResult result = collection.updateOne(eq("id", userId), new Document("$set",
                new Document("email", newEmail)));
        return result.toString();
    }

    public String updateAllUserModel(String userId, User user) {
        final Map<String, Object> userDTO = new HashMap<>();
        userDTO.put("id", user.getId());
        userDTO.put("firstname", user.getFirstname());
        userDTO.put("lastname", user.getLastname());
        userDTO.put("email", user.getEmail());
        UpdateResult result = collection.updateOne(eq("id", userId), new Document("$set",
                new Document(userDTO)));
        return result.toString();
    }

    public void deleteUser(String id) {
        collection.deleteOne(eq("id", id));
    }

    public List<User> iterateAll() {
        List<User> userList = new ArrayList<>();
        try (MongoCursor<Document> cursor = collection.find().iterator()) {
            User user = null;
            while (cursor.hasNext()) {
                user = new User();
                String json = cursor.next().toJson();
                ObjectMapper mapper = new ObjectMapper();
                try {
                    user = mapper.readValue(json, User.class);
                    userList.add(user);
                } catch (IOException ex) {
                    Logger.getLogger(MongoDBConnect.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        System.out.println("iterateAll size " + userList.size());
        return userList;
    }

    public long getCollectionCount() {
        System.out.println("collection is  " + collection.count());
        return collection.count();
    }

    // http://www.mastertheintegration.com/nosql-databases/mongodb/mongodb-java-driver-3-0-quick-reference.html
}
