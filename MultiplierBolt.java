import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.util.Map;

public class MultiplierBolt extends BaseRichBolt {
    OutputCollector basicOutputCollector;
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        basicOutputCollector = collector;
    }

    public void execute(Tuple input) {
        try {
            saveState(input);
            String stateFileName = getStateFileName();
            // [PL20200407]This is used for RS3 testing
			// You need to change the ip to your server ip address.
            pastry_main pastryMain = new pastry_main(stateFileName, 9001, "192.168.204.132", 9001, 10);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String string = input.getString(0);
        string += "a";
        System.out.println(string);
        basicOutputCollector.emit(new Values(string));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field"));
    }
}
