/**
 * Brahmaputra client for D: a native driver for the Brahmaputra log
 * broker's wire protocol, using only Phobos and druntime.
 *
 * ---
 * import brahmaputra;
 *
 * ProducerConfig config;
 * config.compressionType = "gzip";
 * auto producer = new Producer("127.0.0.1:9092", config);
 * producer.send("orders", toBytes(`{"id":1}`), toBytes("user-7"));
 * producer.close();
 * ---
 */
module brahmaputra;

public import brahmaputra.protocol;
public import brahmaputra.connection;
public import brahmaputra.producer;
public import brahmaputra.consumer;
public import brahmaputra.assignor;
public import brahmaputra.group;
