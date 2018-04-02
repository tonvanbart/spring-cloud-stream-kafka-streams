# TOC 
 - what is kafka https://kafka.apache.org/intro 
 - some big concepts:
  -- kafka supports streams of records stored in categories called topics.
  -- each record has a timestamp, a key, and a value
  - kakfa has a producer/consumer API, but it also has a streams procesing API and a connector API. what were interested in is the streams API. 
  - each partition is an ordered, immutable sequence of records that is continually appended to—a structured commit log
  - the streams API doesnt require spark cluster
  - records can be stored for very long periods of time and performance is fixed/constant. 
  - consumers keep an offset relative to a given parititon. 
  - partitions are nice because they allow clients to scale to multiple nodes, and to handle concurrently consuming data. 
  - publishers have to select which parition to use when writing, though theres a load balancing algo that kicks in 
  - consumers have group names. if memebers belong to the same consumer group then only one node gets any one message from a topic. load-balanced!
  - kafka streams goes beyond tradtional pub/sub kinda messaging. it looks more similar to technologies like Apache Spark and Apache Storm. 
  - it has some notable differences, though.
  - 
  
  - spring for kafka 
  - boot autoconfig 
  - kafkatemplate 
  
  - setup an example with a producer that sends writes of `PageViewEvent` usingg spring cloud stream kafka 
  - then add `spring-cloud-stream-binder-kafka-(streams)`
  
  
  
  - Kafka Streams lessons 
  --  kstream: stream of records
  --  latest value for a given key: ktable 
  -- some ops are statess in KS 
  -- somme require state stores. this is managed behind the scenes with kafka persisting the records. 
  -- u can window records, as well, lumping them into buckets
  --  
  
  
  
