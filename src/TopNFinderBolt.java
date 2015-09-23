import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
  private int N;

  private long intervalToReport = 20;
  private long lastReportTime = System.currentTimeMillis();

  public TopNFinderBolt(int N) {
    this.N = N;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words


    ------------------------------------------------- */
    final HashMap<String, Integer> topWordsMap = new HashMap<String, Integer>();
    String word = tuple.getStringByField("word");
    Integer count = tuple.getIntegerByField("count");
    topWordsMap.put(word, count);

    List <String> topWordsKeyList = new ArrayList<String>(topWordsMap.keySet());
    Collections.sort(topWordsKeyList, new Comparator<String>() {
      public int compare(String o1,
                         String o2) {
        if (topWordsMap.get(o1) < topWordsMap.get(o2)) return 1;
        return 0;
      }
    });

    if (!topWordsKeyList.isEmpty()) {
      for (int i=0; i< topWordsKeyList.size(); i++) {
        currentTopWords.put(topWordsKeyList.get(i), topWordsMap.get(topWordsKeyList.get(i)));
        if (i == this.N) break;
      }
    }

    /* End of TODO */

    //reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values(printMap()));
      lastReportTime = System.currentTimeMillis();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

     declarer.declare(new Fields("top-N"));

  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
      stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    stringBuilder.append("]");
    return stringBuilder.toString();

  }
}
