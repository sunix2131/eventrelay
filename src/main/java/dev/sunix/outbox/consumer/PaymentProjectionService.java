package dev.sunix.outbox.consumer;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;

@Service
public class PaymentProjectionService {
    private final JdbcTemplate jdbc;
    private final Clock clock;

    public PaymentProjectionService(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    @Transactional
    public boolean apply(JsonNode event) {
        UUID eventId = UUID.fromString(event.required("eventId").stringValue());
        UUID paymentId = UUID.fromString(event.required("paymentId").stringValue());
        UUID accountId = UUID.fromString(event.required("accountId").stringValue());
        BigDecimal amount = new BigDecimal(event.required("amount").stringValue())
                .setScale(2, RoundingMode.UNNECESSARY);
        String currency = event.required("currency").stringValue();
        Instant occurredAt = Instant.parse(event.required("occurredAt").stringValue());
        if (amount.signum() <= 0 || amount.precision() > 19 || !currency.matches("[A-Z]{3}")) {
            throw new IllegalArgumentException("invalid payment amount or currency in event");
        }
        int inserted = jdbc.update(
                "insert into processed_event (event_id, processed_at) values (?, ?) on conflict do nothing",
                eventId, Timestamp.from(clock.instant()));
        if (inserted == 0) {
            return false;
        }
        jdbc.update(
                """
                insert into payment_projection
                    (payment_id, account_id, amount, currency, accepted_at, applied_count)
                values (?, ?, ?, ?, ?, 1)
                """,
                paymentId,
                accountId, amount, currency, Timestamp.from(occurredAt));
        return true;
    }
}
