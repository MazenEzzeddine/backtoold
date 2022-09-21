import java.util.ArrayList;
import java.util.List;

public class Consumer  implements  Comparable{
    /*private final Long lagCapacity;
    private final double arrivalCapacity;
    private final String id;
    private double remainingArrivalCapacity;
    private List<Partition> assignedPartitions;
    private Long remainingLagCapacity;

    public Consumer(String id, Long lagCapacity, double arrivalCapacity) {
        this.lagCapacity = lagCapacity;
        this.arrivalCapacity = arrivalCapacity;
        this.id = id;

        this.remainingLagCapacity = lagCapacity;
        this.remainingArrivalCapacity = arrivalCapacity;
        assignedPartitions = new ArrayList<>();
    }

    public double getArrivalCapacity() {
        return arrivalCapacity;
    }


    public String getId() {
        return id;
    }
    public Long getRemainingLagCapacity() {
        return remainingLagCapacity;
    }
    public double getRemainingArrivalCapacity() {
        return remainingArrivalCapacity;
    }


    public void setPartitions(List<Partition> partitions) {
        assignedPartitions = partitions;
    }



    //TODO attention to when bin packing using average arrival rates or average lag
    //TODO set remaining capacities accordingly

    public void assignPartition(Partition partition) {
        assignedPartitions.add(partition);
        remainingLagCapacity -= partition.getLag();
        remainingArrivalCapacity -= partition.getArrivalRate();
    }

    @Override
    public String toString() {
        return "\nConsumer{" + "id=" + id +
                ",  lagCapacity= " + lagCapacity +
                ", remainingArrivalCapacity= " + String.format("%.2f", remainingArrivalCapacity) +
                ", arrivalCapacity= " + String.format("%.2f", arrivalCapacity) +
                ", remainingLagCapacity= " + remainingLagCapacity +
                ", assignedPartitions= \n" + assignedPartitions +
                "}";
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = lagCapacity != null ? lagCapacity.hashCode() : 0;
        temp = Double.doubleToLongBits(remainingArrivalCapacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (assignedPartitions != null ? assignedPartitions.hashCode() : 0);
        temp = Double.doubleToLongBits(arrivalCapacity);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (remainingLagCapacity != null ? remainingLagCapacity.hashCode() : 0);
        return result;
    }


    public List<Partition> getAssignedPartitions() {
        return assignedPartitions;
    }*/

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

    @Override
    public int compareTo(Object o) {
        return Double.compare(remainingCapacity , ((Consumer)o).remainingCapacity);
    }


}
