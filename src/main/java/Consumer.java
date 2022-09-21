import java.util.ArrayList;
import java.util.List;

public class Consumer  implements  Comparable{

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    private String id;
    private final double capacity;
    private double remainingCapacity;
    private  ArrayList<Partition> partitions;

    public void setRemainingCapacity(double currentCapacity) {
        this.remainingCapacity = currentCapacity;
    }

    public double getRemainingCapacity() {
        return remainingCapacity;
    }

    public Consumer(double capacity) {
        this.capacity = capacity;
        partitions = new ArrayList<>();
        this.remainingCapacity = capacity;
    }

    public ArrayList<Partition> getAssignedPartitions() {
        return partitions;
    }

    public double remainingCapacity(){
        return remainingCapacity;
    }

    public void  assign(Partition i) {
        partitions.add(i);
        remainingCapacity -= i.getArrivalRate();
    }

    public double getCapacity() {
        return capacity;
    }

    public void setItems(ArrayList<Partition> items) {
        partitions = items;
    }

    public void  removeAssignment() {
        partitions.clear();
        remainingCapacity= capacity;
    }

    @Override
    public String toString() {
        return "Consumer{" +
                "Id=" + id +
                ", capacity=" + capacity +
                ", remainingCapacity=" + remainingCapacity +
                ", partitions=" + partitions +
                '}';
    }

/*

    @Override
    public int compareTo(Object o) {
        return Integer.compare(partitions.size(), ((Consumer) o).partitions.size());
    }
*/

    @Override
    public int compareTo(Object o) {
        return Double.compare(remainingCapacity , ((Consumer)o).remainingCapacity);
    }


   /* @Override
    public int compareTo(Object o) {
        return Double.compare(remainingCapacity , ((Consumer)o).remainingCapacity);
    }
*/

}
