package zoo.philosophers;

import java.time.Duration;

public final class PhilosopherMain {
    private PhilosopherMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("Usage: PhilosopherMain <name> <hostPort> <id> <count> <eatMillis>");
            return;
        }

        Philosopher philosopher = new Philosopher(
                args[0],
                args[1],
                Integer.parseInt(args[2]),
                Integer.parseInt(args[3]),
                "/philosophers");

        boolean ate = philosopher.dine(Duration.ofSeconds(30), Duration.ofMillis(Long.parseLong(args[4])));
        System.out.println(args[0] + (ate ? " ate successfully." : " could not acquire forks."));
    }
}
