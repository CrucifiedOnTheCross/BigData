package zoo.twopc;

import java.time.Duration;

public final class RegisterParticipantMain {
    private RegisterParticipantMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.out.println("Usage: RegisterParticipantMain <name> <hostPort> <root> <txId> <COMMIT|ABORT> <timeoutMillis>");
            return;
        }

        RegisterParticipant participant = new RegisterParticipant(args[0], args[1], args[2], "");
        Decision decision = participant.voteAndApply(
                args[3],
                Decision.valueOf(args[4]),
                Duration.ofMillis(Long.parseLong(args[5])));
        System.out.println(args[0] + " applied decision " + decision + ", value=" + participant.committedValue());
    }
}
