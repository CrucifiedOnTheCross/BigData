package zoo;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import zoo.philosophers.Philosopher;

class DiningPhilosophersTest {
    @Test
    void philosophersEventuallyEatWithoutDeadlock() throws Exception {
        try (EmbeddedZooKeeper server = new EmbeddedZooKeeper()) {
            var executor = Executors.newFixedThreadPool(5);
            List<CompletableFuture<Boolean>> runs = new ArrayList<>();

            try {
                for (int i = 0; i < 5; i++) {
                    int id = i;
                    runs.add(CompletableFuture.supplyAsync(() -> runPhilosopher(server.connectString(), id), executor));
                }

                for (CompletableFuture<Boolean> run : runs) {
                    assertTrue(run.get(10, TimeUnit.SECONDS));
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private static boolean runPhilosopher(String connectString, int id) {
        try {
            return new Philosopher("philosopher-" + id, connectString, id, 5, "/philosophers")
                    .dine(Duration.ofSeconds(5), Duration.ofMillis(50));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
