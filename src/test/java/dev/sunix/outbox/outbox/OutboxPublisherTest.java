package dev.sunix.outbox.outbox;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OutboxPublisherTest {
    private static final Instant NOW = Instant.parse("2026-01-15T12:00:00Z");
    private final OutboxStore store = mock(OutboxStore.class);
    private final EventSender sender = mock(EventSender.class);
    private final OutboxPublisher publisher =
            new OutboxPublisher(store, sender, Clock.fixed(NOW, ZoneOffset.UTC), 10, 3);

    @Test
    void marksAcknowledgedEventAsPublished() throws Exception {
        OutboxEvent event = event(0);
        when(store.lockReady(10)).thenReturn(List.of(event));

        publisher.publishBatch();

        verify(sender).send(event);
        verify(store).markPublished(event.id(), NOW);
    }

    @Test
    void schedulesBoundedBackoffAfterTransientFailure() throws Exception {
        OutboxEvent event = event(0);
        when(store.lockReady(10)).thenReturn(List.of(event));
        org.mockito.Mockito.doThrow(new IllegalStateException("broker unavailable"))
                .when(sender)
                .send(event);

        publisher.publishBatch();

        verify(store).scheduleRetry(event.id(), 1, NOW.plusSeconds(1), "IllegalStateException: broker unavailable");
    }

    @Test
    void movesEventToDeadStateAfterLastAttempt() throws Exception {
        OutboxEvent event = event(2);
        when(store.lockReady(10)).thenReturn(List.of(event));
        org.mockito.Mockito.doThrow(new IllegalStateException("still unavailable"))
                .when(sender)
                .send(event);

        publisher.publishBatch();

        verify(store).markDead(event.id(), 3, "IllegalStateException: still unavailable");
    }

    @Test
    void databaseFailureIsNotMisreportedAsKafkaRetry() throws Exception {
        OutboxEvent event = event(0);
        when(store.lockReady(10)).thenReturn(List.of(event));
        doThrow(new IllegalStateException("database unavailable")).when(store).markPublished(event.id(), NOW);
        assertThatThrownBy(publisher::publishBatch).hasMessage("database unavailable");
        verify(store).lockReady(10);
        verify(store).markPublished(event.id(), NOW);
        verifyNoMoreInteractions(store);
    }

    @Test
    void interruptionStopsTheBatchAndPreservesTheInterruptFlag() throws Exception {
        OutboxEvent first = event(0);
        when(store.lockReady(10)).thenReturn(List.of(first, event(0)));
        doThrow(new InterruptedException("shutdown")).when(sender).send(first);
        try {
            assertThatThrownBy(publisher::publishBatch).hasMessageContaining("interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(sender).send(first);
            verifyNoMoreInteractions(sender);
            verify(store).lockReady(10);
            verifyNoMoreInteractions(store);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void backoffReachesFiveMinuteCap() throws Exception {
        OutboxEvent event = event(9);
        when(store.lockReady(10)).thenReturn(List.of(event));
        doThrow(new IllegalStateException("offline")).when(sender).send(event);
        new OutboxPublisher(store, sender, Clock.fixed(NOW, ZoneOffset.UTC), 10, 20).publishBatch();
        verify(store).scheduleRetry(event.id(), 10, NOW.plusSeconds(300), "IllegalStateException: offline");
    }

    private static OutboxEvent event(int attempts) {
        return new OutboxEvent(UUID.randomUUID(), UUID.randomUUID(), "payment.accepted.v1", "{}", attempts, NOW);
    }
}
