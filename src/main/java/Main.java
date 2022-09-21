import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {



        AssignmentServer server = new AssignmentServer(5002);
        Controller controller = new Controller();
        Thread serverthread = new Thread(server);
        Thread controllerthread = new Thread(controller);

        controllerthread.start();
        serverthread.start();


    }

}