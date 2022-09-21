import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LeastLoadedFFD {

    private List<Partition> items;
    private double bincapacity;

    public LeastLoadedFFD(List<Partition> items, double bincapacity) {
        this.items = items;
        this.bincapacity = bincapacity;
    }

    public List<Consumer> fitFFD(){
        List<Consumer> bins = new ArrayList<>();

        Collections.sort(items, Collections.reverseOrder());
        Consumer bin = new Consumer(bincapacity);
        bins.add(bin);
        Consumer newbin = null;

        while(true) {
            for (Consumer b : bins) {
                b.removeAssignment();
            }
            int i;
            for ( i = 0; i< items.size(); i++) {
                Partition itt = items.get(i);

                Collections.sort(bins, Collections.reverseOrder());
                for (Consumer b : bins) {
                    if (itt.getArrivalRate() <= b.remainingCapacity()) {
                        b.assign(itt);
                        break;
                    }
                    if (b == bins.get(bins.size() - 1)) {
                        newbin = new Consumer(bincapacity);
                        newbin.assign(itt);
                    }
                }
                if (newbin != null) {
                    bins.add(newbin);
                    newbin = null;
                    break;
                }
            }

            if ( i == items.size()) {
                break;
            }
        }

        for (Consumer b : bins) {
            System.out.println(b);
        }


        return bins;
    }

    public List<Consumer> LeastLoadFFDHeterogenous(){
        List<Double> capacities = Arrays.asList(95.0, 180.0);
        int capacityIndex = 0;
        List<Consumer> bins = new ArrayList<>();
        Collections.sort(items, Collections.reverseOrder());
        Consumer bin = new Consumer(bincapacity);
        //bin.setId("cons" + (int)bin.getCapacity());
        bins.add(bin);
        capacityIndex = 1;
        Consumer newbin = null;
        while(true) {
            for (Consumer b : bins) {
                b.removeAssignment();
            }
            int i;
            for ( i = 0; i< items.size(); i++) {
                Partition itt = items.get(i);

                Collections.sort(bins, Collections.reverseOrder());
                for (Consumer b : bins) {
                    if (itt.getArrivalRate() <= b.remainingCapacity()) {
                        b.assign(itt);
                        break;
                    }
                    if (b == bins.get(bins.size() - 1)) {
                        if(capacityIndex < capacities.size()) {
                            newbin = new Consumer(capacities.get(capacityIndex));
                           // newbin.setId("cons" + (int)bin.getCapacity());

                        } else {
                            capacityIndex=0;
                            newbin = new Consumer(capacities.get(capacityIndex));
                           // newbin.setId("cons" + (int)bin.getCapacity());
                        }
                    }
                }
                if (newbin != null) {
                    if(bins.get(bins.size()-1).getCapacity() != capacities.get(capacities.size() -1) ) {
                        bins.remove(bins.size()-1);
                        capacityIndex++;
                    }
                    bins.add(newbin);
                    newbin = null;
                    break;
                }
            }
            if ( i == items.size()) {
                break;
            }
        }

      /*  for (Consumer b : bins) {
            System.out.println(b);
        }*/
        return bins;
    }

}
