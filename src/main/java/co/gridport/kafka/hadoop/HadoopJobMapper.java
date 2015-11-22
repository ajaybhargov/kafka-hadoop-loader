package co.gridport.kafka.hadoop;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HadoopJobMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {

    static private JsonFactory jsonFactory = new JsonFactory();

    @Override
    public void map(LongWritable key, BytesWritable value, Context context) throws IOException {
        try {
            Text outDateKey = map(key, value, context.getConfiguration());
            if (outDateKey != null) {
                Text outValue = new Text();
                outValue.set(value.getBytes(), 0, value.getLength());
                context.write(outDateKey, outValue);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Text map(LongWritable key, BytesWritable value, Configuration conf) throws IOException {
        System.out.println("map method called");
        String inputFormat = conf.get("input.format");
        if (inputFormat.equals("json")) {
            Map<String, String> fields = new HashMap<String, String>();
            fields.put("timestamp", null);
            fields.put("date", null);
            fields.put("event_type", null);
            fields.put("type", null);
            try {
                parseMinimumJsonMessage(value.getBytes(), fields);
                String eventDate = fields.get("date");
                String eventType = fields.get("event_type") != null ? fields.get("event_type") : fields.get("type");
                if (eventDate != null && eventType != null) {
                    Text outDateKey = new Text();
                    outDateKey.set(eventDate.getBytes());
                    return outDateKey;
                } else {
                    //JIRA EDA-33 - only loading tracking events to HDFS
                    return null;
                }
            } catch (JsonParseException e) {
                //JIRA EDA-23 - handling invalid json
                System.out.println("Failed to parse json event message `" + new String(value.getBytes()));
                return null;
            }
        } else if (inputFormat.equals("protobuf")) {
            //Open the proto
            throw new NotImplementedException("Protobuf input disabled");
            /*
            Event event = Event.parseFrom(value.copyBytes());
            date.set(event.getDate());
            out.set( JsonFormat.printToString(event));
            */
        } else if (inputFormat.equals("avro")) {
            throw new NotImplementedException("Avro input format not implemented");
        } else {
            throw new IOException("Invalid mapper input.format");
        }
    }

    private void parseMinimumJsonMessage(byte[] json, Map<String, String> fields) throws JsonParseException, IOException {
        //read only the necessary fields (streaming jackson)
        JsonParser jp;
        jp = jsonFactory.createJsonParser(json);
        try {
            int filled = 0;
            int objects = 0;
            while (jp.nextToken() != null && filled < fields.size()) {
                JsonToken token = jp.getCurrentToken();
                if (token == JsonToken.START_OBJECT)
                    objects++;
                if (token == JsonToken.END_OBJECT && --objects == 0)
                    break;
                if (token == JsonToken.FIELD_NAME) {
                    String fieldName = jp.getCurrentName();
                    if (jp.nextToken() == JsonToken.START_OBJECT) {
                        objects++;
                        continue;
                    }
                    String value = jp.getText();
                    if (fields.containsKey(fieldName)) {
                        value = value.equals("0") ? null : value;
                        fields.put(fieldName, value);
                        ++filled;
                    }
                }
            }
        } finally {
            jp.close();
        }

    }
}
