import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class PRNodeWritable implements Writable {

  public static class EdgeWritable extends ArrayWritable {

    public EdgeWritable() {
      super(IntWritable.class);
    }

    public EdgeWritable(IntWritable[] edges) {
      super(IntWritable.class, edges);
    }
  }

  public PRNodeWritable() {
    id = new IntWritable(-1);
    rank = new DoubleWritable(-1);
  }

  public PRNodeWritable(IntWritable id) {
    this.id = id;
    rank = new DoubleWritable(-1);
  }

  IntWritable id; // Node ID
  DoubleWritable rank; // Rank
  EdgeWritable edges = new EdgeWritable(new IntWritable[0]); // Adjacency list

  // Output format: (nodeID, distance, prev)
  // Serialize
  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    rank.write(out);
    edges.write(out);
  }

  // Deserialize
  @Override
  public void readFields(DataInput in) throws IOException {
    id.readFields(in);
    rank.readFields(in);
    edges.readFields(in);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(rank.get() + " ");
    for (Writable toNode : edges.get()) {
      IntWritable toNodeId = (IntWritable) toNode;
      builder.append(toNodeId.get()).append(" ");
    }
    return builder.toString();
  }

  public void setEdges(IntWritable[] edges) {
    this.edges.set(edges);
  }
}