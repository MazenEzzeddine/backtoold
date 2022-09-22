import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FirstFitDecHetero {
    private List<Double> capacities;
    private List<Partition> items;

    public FirstFitDecHetero(List<Partition> items, List<Double> capacities) {
        this.capacities = capacities;
        this.items = items;
    }
    List<Consumer> fftFFDHetero() {
        List<Consumer> bins = new FFD(capacities.get(capacities.size() - 1),items).fitFFD();
        capacities.sort(new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                return Double.compare(o1, o2);
            }
        });

        //System.out.println(bins);
        List<Consumer> newbins = new ArrayList<>();
        for (Consumer b : bins) {
            for (Double capacity : capacities) {
                if ((b.getCapacity()- b.remainingCapacity()) <= capacity) {
                    Consumer newbin = new Consumer(capacity);
                    newbin.setItems(b.getAssignedPartitions());
                    newbin.setRemainingCapacity(b.getRemainingCapacity() - (b.getCapacity() - newbin.getCapacity()));
                    newbins.add(newbin);
                    break;
                }
            }
        }

        System.out.println(capacities);
        System.out.println(newbins);
        List<Consumer> fairbins =  new ArrayList<>();
        for(Consumer b: newbins){
            fairbins.add(new Consumer(b.getCapacity()));
        }

        for(Partition i: items) {
            Collections.sort(fairbins, Collections.reverseOrder());
            fairbins.get(0).assign(i);
        }
     /*   System.out.println("fair bins are");
        System.out.println(fairbins);
*/
        return fairbins;
    }
}
