import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FFD {
    private double capacity;
    private List<Partition> items;

    public FFD(double capacity, List<Partition> items) {
        this.capacity = capacity;
        this.items = items;
    }

    public List<Consumer> fitFFD() {
        Collections.sort(items, Collections.reverseOrder());
        List<Consumer> bins = new ArrayList<>();

        Consumer bin = new Consumer(capacity);
        bins.add(bin);
        Consumer newbin = null;
        for (Partition itt : items) {
            for (Consumer b : bins) {
                if (itt.getArrivalRate() <= b.remainingCapacity()) {
                    b.assign(itt);
                    break;
                }
                if (b == bins.get(bins.size() - 1)) {
                    newbin = new Consumer(capacity);
                    newbin.assign(itt);
                }
            }
            if (newbin != null) {
                bins.add(newbin);
                newbin = null;
            }
        }

    /*    for (Consumer b : bins) {
            System.out.println(b);
        }
*/
        return bins;

    }
}
