package zoo;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import zoo.barrier.Animal;

class AnimalBarrierTest {
    @Test
    void animalsStartOnlyAfterWholePartyEntersZoo() throws Exception {
        try (EmbeddedZooKeeper server = new EmbeddedZooKeeper()) {
            CountDownLatch allEntered = new CountDownLatch(3);
            CountDownLatch mayLeave = new CountDownLatch(1);
            var executor = Executors.newFixedThreadPool(3);

            try {
                List<CompletableFuture<Boolean>> animals = List.of("lion", "tiger", "bear").stream()
                        .map(name -> CompletableFuture.supplyAsync(() -> runAnimal(
                                name,
                                server.connectString(),
                                allEntered,
                                mayLeave), executor))
                        .toList();

                assertTrue(allEntered.await(20, TimeUnit.SECONDS));
                mayLeave.countDown();
                for (CompletableFuture<Boolean> animal : animals) {
                    assertTrue(animal.get(20, TimeUnit.SECONDS));
                }
            } finally {
                mayLeave.countDown();
                executor.shutdownNow();
            }
        }
    }

    private static boolean runAnimal(
            String name,
            String connectString,
            CountDownLatch allEntered,
            CountDownLatch mayLeave) {
        try (Animal animal = new Animal(name, connectString, "/zoo", 3)) {
            boolean entered = animal.enter(Duration.ofSeconds(15));
            if (entered) {
                allEntered.countDown();
                mayLeave.await(5, TimeUnit.SECONDS);
            }
            return entered;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
