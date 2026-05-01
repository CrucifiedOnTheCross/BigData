package zoo.twopc;

import java.time.Duration;

public final class CoordinatorMain {
    private CoordinatorMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.out.println("Usage: CoordinatorMain <hostPort> <root> <txId> <participantCount> <value> <timeoutMillis>");
            return;
        }

        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator(args[0], args[1]);
        Decision decision = coordinator.coordinate(
                args[2],
                args[4],
                Integer.parseInt(args[3]),
                Duration.ofMillis(Long.parseLong(args[5])));
        System.out.println("Transaction " + args[2] + " decision: " + decision);
    }
}
