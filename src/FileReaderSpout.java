
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  /* TODO */
  private FileReader fileReader;
  private boolean completed = false;
  /* End of TODO */

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

     /*
    ----------------------TODO-----------------------
    Task: initialize the file reader


    ------------------------------------------------- */
    try {
      this.fileReader = new FileReader(conf.get("inputFile").toString());
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error reading file " + conf.get("inputFile"));
    }
    /* End of TODO */

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
    if (completed) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {

      }
    }
    String str;
    BufferedReader reader = new BufferedReader(this.fileReader);
    try {
      while ((str = reader.readLine()) != null) {
        str = str.trim();
        if (str.equals("")) continue;
        this._collector.emit(new Values(str), str);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error reading typle", e);
    } finally {
      completed = true;
    }
    /* End of TODO */

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
    try {
      this.fileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    /* End of TODO */
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
