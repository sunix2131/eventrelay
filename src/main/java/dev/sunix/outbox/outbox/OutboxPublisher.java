package dev.sunix.outbox.outbox;

import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxPublisher {
    private final OutboxStore outbox;
    private final EventSender sender;
    private final Clock clock;
    private final int batchSize;
    private final int maxAttempts;

    public OutboxPublisher(OutboxStore outbox, EventSender sender, Clock clock,
            @Value("${app.outbox.batch-size:50}") int batchSize,
            @Value("${app.outbox.max-attempts:8}") int maxAttempts) {
        if (batchSize < 1 || batchSize > 1000 || maxAttempts < 1) {
            throw new IllegalArgumentException("batch size must be 1–1000 and max attempts must be positive");
        }
        this.outbox = outbox;
        this.sender = sender;
        this.clock = clock;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
    }

    @Scheduled(
            fixedDelayString = "${app.outbox.poll-interval:500ms}",
            initialDelayString = "${app.outbox.initial-delay:0ms}")
    @Transactional
    public int publishBatch() {
        var events = outbox.lockReady(batchSize);
        for (OutboxEvent event : events) {
            try {
                sender.send(event);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("outbox publication interrupted", exception);
            } catch (Exception exception) {
                deferOrStop(event, exception);
                continue;
            }
            // A storage failure must roll back the batch, not be classified as a send failure.
            outbox.markPublished(event.id(), clock.instant());
        }
        return events.size();
    }

    private void deferOrStop(OutboxEvent event, Exception exception) {
        int attempts = event.attempts() + 1;
        String error = boundedError(exception);
        if (attempts >= maxAttempts) {
            outbox.markDead(event.id(), attempts, error);
            return;
        }
        long delaySeconds = Math.min(300, 1L << Math.min(attempts - 1, 9));
        outbox.scheduleRetry(event.id(), attempts, clock.instant().plus(Duration.ofSeconds(delaySeconds)), error);
    }

    private static String boundedError(Exception exception) {
        String message = exception.getMessage();
        String value = exception.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message);
        return value.substring(0, Math.min(value.length(), 500));
    }
}
