package site.ycsb.db;


import com.whalin.MemCached.SockIOPool;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import site.ycsb.*;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.*;


/**
 * Concrete Memcached udp client implementation.
 */
public class MemcachedUDPClient extends DB {

  private com.whalin.MemCached.MemCachedClient client;
  private final Logger logger = Logger.getLogger(getClass());

  private SockIOPool pool;

  protected static final ObjectMapper MAPPER = new ObjectMapper();


  protected com.whalin.MemCached.MemCachedClient memcachedClient() {
    return client;
  }

  public void init() throws DBException {
//    System.out.println("init init init init init init init init init");
    try {
      String[] serverlist = new String[]{"192.168.238.129:11211"};
      pool = SockIOPool.getInstance("test", false); //NOTE false=udp
      pool.setServers(serverlist);
      pool.initialize();
      client = createMemcachedClient();
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  protected com.whalin.MemCached.MemCachedClient createMemcachedClient()
      throws Exception {
    // 第一个参数表示使用udp协议，第二个参数表示使用文本协议
    return new com.whalin.MemCached.MemCachedClient("test", false, false);
  }


  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    try {
      Object val = memcachedClient().get(key);
      if(val != null){
        fromJson((String) val, fields, result);
      }
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error encountered for key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result){
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    //System.out.println("Update:Update:Update:Update: " + key);
    key = createQualifiedKey(table, key);

    try {
      return memcachedClient().replace(key, toJson(values))? Status.OK : Status.ERROR;
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      boolean ok = memcachedClient().set(key, toJson(values));
      if(!ok){
        return Status.ERROR;
      }else{
        return Status.OK;
      }
    } catch (Exception e) {
      logger.error("Error inserting value", e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }


  protected static void fromJson(
      String value, Set<String> fields,
      Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
         jsonFields.hasNext();
      /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && !fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected static String toJson(Map<String, ByteIterator> values)
      throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }
}