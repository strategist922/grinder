Streaming Realtime Map/Reduce

Grinder is a distributed map/reduce framework for state based analytics. The primary difference between Grinder and other streaming frameworks (such as Storm which Grinder runs on) is that Grinder allows for updates of state rather than pure aggregation. Any record can be created, update or deleted and Grinder will re-calculate all of the views that depend on that value. Views can have any number of inputs, including other views. Grinder takes care of cascading all of the state changes downstream. This is very similar to a CouchDB but far more powerful.

    // *** JAVA
    g.setSpout("users", new KafkaSpout(new JsonScheme("id", "name", "age")))
    g.map('{ name }', 'users(id)', '{[name, 1]}')
     .red('{ count }', 'nameCount(name)', new Sum())
    g.map('{ age }', 'users(id)', {[age, 1]})
     .red('{ total samples }', 'averageAge()', new Sum())
     .fmt('{ value }', '{[total / samples]}')
    g.publish(myDbAdapter)

    // *** SQL
    SELECT count FROM nameCounts WHERE name = 'Joe'
    SELECT value FROM averageAge

Notice that the query is SQL. Grinder itself is never queried. Grinder is a state engine and emits declarative facts that can be stored in any database you like (or K/V store, search appliance, web-service etc). In Grinder you model inputs and outputs using map/reduce operations. The queue emits facts, Grinder emits conclusions.

Grinder guarantees processing and can perform transactions across multiple data sources. For example, it can store a normalized record in POSTGRES, a fully de-normalized version in Cassandra and also make sure the right keywords are published to SOLR. When the record is deleted, Grinder will reverse everything. Grinder can model transaction even for non-transactional stores. The stream below takes three inputs and builds a combined view, a reconciled total. 

    > MAP { src dst amount } FROM transfers(id) USE {[dst, amount]}
    > MAP { src dst amount } FROM transfers(id) USE {[src, -amount]}
    > MAP { id amount } FROM deposits(id) USE {[id, amount]}
    > RED { balance } INTO accounts(id) USE { a, b -> [a.balance + b.balance]}

The above stream will emit two updated account records for every one transfer record. The guaranteed processing engine can then deliver the updated account records to any destination specified.

Grinder deconstructs the data pipeline allowing modular assembly of a data system. Each piece is pluggable, making publication to HBase instead of Cassandra a one line code change. Why not publish to both? Grinder will take care of that for you. New streams can be deployed into a live grid with no downtime and killed when no longer needed. And if you screw up? You can just replay a stream; in Grinder it is completely safe to rewind to any point in your stream and replay. Publishing to MYSQL and lose the server? Your hot standby missing five minutes of data due to lag? Just rewind the stream and let it replay, Grinder will repair the state for you. Want to build secondary indexes and store them in HBase? Grinder makes it trivial. No deadlocks, no race conditions, no contention.

Grinder runs on Twitter's Storm and re-uses many of its concepts such as Spouts and Bolts. In fact, Storm spouts and bolts can be intermixed with Grinder map/reduce operations. Its all just tuples underneath. Unlike storm and trident however Grinder is not focused on achieving millions of operations a second. Grinders aim is to change peoples relationship with data by eliminating a lot of the difficult and painful aspects of working with a large datastore. Data can be easy, safe and fun to work with even in a massive de-normalized heterogeneous environment. Grinder is the glue that can coordinate all these pieces.

