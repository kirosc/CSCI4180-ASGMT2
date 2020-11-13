import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

public class PDNodeWritable implements Writable {

  public static class EdgeWritable extends TwoDArrayWritable {

    public EdgeWritable() {
      super(IntWritable.class);
    }

    public EdgeWritable(IntWritable[][] edges) {
      super(IntWritable.class, edges);
    }
  }

  public PDNodeWritable() {
    id = new IntWritable(-1);
    distance = new IntWritable(Integer.MAX_VALUE);
    prevNodeId = new IntWritable(-1);
    edges = new EdgeWritable();
  }

  public PDNodeWritable(IntWritable id, IntWritable distance) {
    this.id = id;
    this.distance = distance;
    prevNodeId = new IntWritable(-1);
    edges = new EdgeWritable();
  }

  IntWritable id; // Node ID
  IntWritable distance; // Shortest path length
  IntWritable prevNodeId; // Previous node
  EdgeWritable edges; // Adjacency list

  // Output format: (nodeID, distance, prev),
  // Serialize
  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    distance.write(out);
    prevNodeId.write(out);
    edges.write(out);
  }

  // Deserialize
  @Override
  public void readFields(DataInput in) throws IOException {
    id.readFields(in);
    distance.readFields(in);
    prevNodeId.readFields(in);
    edges.readFields(in);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(new String(this.distance + " "));
    for (Writable[] writable : edges.get()) {
      IntWritable toNodeId = (IntWritable) writable[0];
      IntWritable weight = (IntWritable) writable[1];
      builder.append("(").append(toNodeId.get()).append(", ").append(weight.get()).append("), ");
    }
    if (builder.length() > 0) {
      builder.setLength(builder.length() - 2);
    }
    return builder.toString();
  }

  public void setEdges(IntWritable[][] edges) {
    this.edges.set(edges);
  }
}