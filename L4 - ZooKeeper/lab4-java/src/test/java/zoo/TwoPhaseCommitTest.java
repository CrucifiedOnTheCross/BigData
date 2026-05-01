package zoo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import zoo.twopc.Decision;
import zoo.twopc.RegisterParticipant;
import zoo.twopc.TwoPhaseCommitCoordinator;

class TwoPhaseCommitTest {
    @Test
    void commitsReplicatedRegisterWhenAllParticipantsVoteCommit() throws Exception {
        try (EmbeddedZooKeeper server = new EmbeddedZooKeeper()) {
            var executor = Executors.newFixedThreadPool(4);
            try {
                TwoPhaseCommitCoordinator coordinator =
                        new TwoPhaseCommitCoordinator(server.connectString(), "/ha-register");
                CompletableFuture<Decision> decision = CompletableFuture.supplyAsync(
                        () -> coordinate(coordinator, "tx-commit", "value-2", 3), executor);

                List<RegisterParticipant> participants = List.of(
                        new RegisterParticipant("node-a", server.connectString(), "/ha-register", "value-1"),
                        new RegisterParticipant("node-b", server.connectString(), "/ha-register", "value-1"),
                        new RegisterParticipant("node-c", server.connectString(), "/ha-register", "value-1"));

                List<CompletableFuture<Decision>> results = participants.stream()
                        .map(participant -> CompletableFuture.supplyAsync(
                                () -> vote(participant, "tx-commit", Decision.COMMIT), executor))
                        .toList();

                assertEquals(Decision.COMMIT, decision.get(5, TimeUnit.SECONDS));
                for (CompletableFuture<Decision> result : results) {
                    assertEquals(Decision.COMMIT, result.get(5, TimeUnit.SECONDS));
                }
                for (RegisterParticipant participant : participants) {
                    assertEquals("value-2", participant.committedValue());
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void abortsReplicatedRegisterWhenAnyParticipantVotesAbort() throws Exception {
        try (EmbeddedZooKeeper server = new EmbeddedZooKeeper()) {
            var executor = Executors.newFixedThreadPool(4);
            try {
                TwoPhaseCommitCoordinator coordinator =
                        new TwoPhaseCommitCoordinator(server.connectString(), "/ha-register");
                CompletableFuture<Decision> decision = CompletableFuture.supplyAsync(
                        () -> coordinate(coordinator, "tx-abort", "value-2", 3), executor);

                RegisterParticipant nodeA =
                        new RegisterParticipant("node-a", server.connectString(), "/ha-register", "value-1");
                RegisterParticipant nodeB =
                        new RegisterParticipant("node-b", server.connectString(), "/ha-register", "value-1");
                RegisterParticipant nodeC =
                        new RegisterParticipant("node-c", server.connectString(), "/ha-register", "value-1");

                List<CompletableFuture<Decision>> results = List.of(
                        CompletableFuture.supplyAsync(() -> vote(nodeA, "tx-abort", Decision.COMMIT), executor),
                        CompletableFuture.supplyAsync(() -> vote(nodeB, "tx-abort", Decision.ABORT), executor),
                        CompletableFuture.supplyAsync(() -> vote(nodeC, "tx-abort", Decision.COMMIT), executor));

                assertEquals(Decision.ABORT, decision.get(5, TimeUnit.SECONDS));
                for (CompletableFuture<Decision> result : results) {
                    assertEquals(Decision.ABORT, result.get(5, TimeUnit.SECONDS));
                }
                assertEquals("value-1", nodeA.committedValue());
                assertEquals("value-1", nodeB.committedValue());
                assertEquals("value-1", nodeC.committedValue());
            } finally {
                executor.shutdownNow();
            }
        }
    }

    private static Decision coordinate(
            TwoPhaseCommitCoordinator coordinator,
            String transactionId,
            String value,
            int participants) {
        try {
            return coordinator.coordinate(transactionId, value, participants, Duration.ofSeconds(5));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Decision vote(RegisterParticipant participant, String transactionId, Decision vote) {
        try {
            return participant.voteAndApply(transactionId, vote, Duration.ofSeconds(5));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
