package zoo.barrier;

import java.time.Duration;
import java.util.Random;

public final class AnimalRunner {
    private static final int SLEEP_TIME_MILLIS = 100;

    private AnimalRunner() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: AnimalRunner <animalName> <hostPort> <partySize>");
            return;
        }

        String animalName = args[0];
        String hostPort = args[1];
        int partySize = Integer.parseInt(args[2]);

        System.out.println("Starting animal runner");
        try (Animal animal = new Animal(animalName, hostPort, "/zoo", partySize)) {
            if (!animal.enter(Duration.ofSeconds(30))) {
                System.out.println(animal.name() + " was not permitted to the zoo in time.");
                return;
            }

            System.out.println(animal.name() + " entered.");
            int iterations = 1 + new Random().nextInt(20);
            for (int i = 0; i < iterations; i++) {
                Thread.sleep(SLEEP_TIME_MILLIS);
                System.out.println(animal.name() + " is running...");
            }
        } catch (Exception e) {
            System.out.println("Animal was not permitted to the zoo. " + e);
            throw e;
        }
    }
}
