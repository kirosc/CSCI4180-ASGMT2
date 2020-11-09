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

  public PDNodeWritable(IntWritable id) {
    this.id = id;
    this.distance = new IntWritable(Integer.MAX_VALUE);
    this.prevNodeId = new IntWritable(Integer.MIN_VALUE);
    this.edges = new EdgeWritable();
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

  public void setEdges(EdgeWritable edges) {
    this.edges = edges;
  }
}