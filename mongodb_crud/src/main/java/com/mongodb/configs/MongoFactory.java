package com.mongodb.configs;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

import java.util.Collections;
import java.util.List;

public class MongoFactory {

    private List<ServerAddress> mongoAddresses = Collections.emptyList();
    private MongoClientOptions mongoOptions = MongoClientOptions.builder().build();
    private WriteConcern writeConcern;

    /**
     * Creates a mongo instance based on the provided configuration. Read javadoc of the class to learn about the
     * configuration options. A new Mongo instance is created each time this method is called.
     *
     * @return a new Mongo instance each time this method is called.
     */
    public MongoClient createMongo() {
        MongoClient mongo;
        if (mongoAddresses.isEmpty()) {
            mongo = new MongoClient(new ServerAddress("localhost", 27017), mongoOptions);
        } else {
            mongo = new MongoClient(mongoAddresses, mongoOptions);
        }
        mongo.setWriteConcern(defaultWriteConcern());

        return mongo;
    }

    /**
     * Provide a list of ServerAddress objects to use for locating the Mongo replica set. An empty list will result in
     * a single Mongo instance being used on the default host ({@code 127.0.0.1}) and port
     * (<code>{@code com.mongodb.ServerAddress#defaultPort}</code>)
     * <p/>
     * Defaults to an empty list, which locates a single Mongo instance on the default host ({@code 127.0.0.1})
     * and port <code>({@code com.mongodb.ServerAddress#defaultPort})</code>
     *
     * @param mongoAddresses List of ServerAddress instances
     */
    public void setMongoAddresses(List<ServerAddress> mongoAddresses) {
        this.mongoAddresses = mongoAddresses;
    }

    /**
     * Provide an instance of MongoOptions to be used for the connections. Defaults to a MongoOptions with all its
     * default settings.
     *
     * @param mongoOptions MongoOptions to overrule the default
     */
    public void setMongoOptions(MongoClientOptions mongoOptions) {
        this.mongoOptions = mongoOptions;
    }

    /**
     * Provided a write concern to be used by the mongo instance. The provided concern should be compatible with the
     * number of addresses provided with {@link #setMongoAddresses(java.util.List)}. For example, providing
     * {@link WriteConcern#REPLICAS_SAFE} in combination with a single address will cause each write operation to hang.
     * <p/>
     * While safe (e.g. {@link WriteConcern#REPLICAS_SAFE}) WriteConcerns allow you to detect concurrency issues
     * immediately, you might want to use a more relaxed write concern if you have other mechanisms in place to ensure
     * consistency.
     * <p/>
     * Defaults to {@link WriteConcern#REPLICAS_SAFE} if you provided more than one address with
     * {@link #setMongoAddresses(java.util.List)}, or {@link WriteConcern#FSYNC_SAFE} if there is only one address (or
     * none at all).
     *
     * @param writeConcern WriteConcern to use for the connections
     */
    public void setWriteConcern(WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    private WriteConcern defaultWriteConcern() {
        if (writeConcern != null) {
            return this.writeConcern;
        } else if (mongoAddresses.size() > 1) {
            return WriteConcern.REPLICAS_SAFE;
        } else {
            return WriteConcern.FSYNC_SAFE;
        }
    }
}
