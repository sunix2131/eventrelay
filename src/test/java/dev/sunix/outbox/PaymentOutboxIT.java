package dev.sunix.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.sunix.outbox.api.CreatePaymentRequest;
import dev.sunix.outbox.api.PaymentResponse;
import dev.sunix.outbox.consumer.PaymentProjectionService;
import dev.sunix.outbox.outbox.OutboxPublisher;
import dev.sunix.outbox.payment.PaymentService;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

@Testcontainers
@AutoConfigureMockMvc
@SpringBootTest(properties = "app.outbox.initial-delay=1h")
class PaymentOutboxIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired MockMvc mvc;
    @Autowired PaymentService payments;
    @Autowired OutboxPublisher publisher;
    @Autowired PaymentProjectionService projection;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper objectMapper;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from payment_projection");
        jdbc.update("delete from processed_event");
        jdbc.update("delete from outbox_event");
        jdbc.update("delete from payment");
    }

    @Test
    void storesPaymentAndOutboxEventInOneCommandAndPublishesToKafka() throws Exception {
        UUID accountId = UUID.randomUUID();
        String request = """
                {"accountId":"%s","amount":"1250.40","currency":"rub"}
                """.formatted(accountId);

        mvc.perform(post("/payments")
                        .header("Idempotency-Key", "checkout-481")
                        .contentType("application/json")
                        .content(request))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.currency").value("RUB"))
                .andExpect(jsonPath("$.replayed").value(false));

        assertThat(jdbc.queryForObject("select count(*) from payment", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from outbox_event", Integer.class)).isEqualTo(1);
        assertThat(publisher.publishBatch()).isEqualTo(1);

        await(Duration.ofSeconds(10), () -> jdbc.queryForObject(
                "select count(*) from payment_projection", Integer.class) == 1);
        assertThat(jdbc.queryForObject("select status from outbox_event", String.class)).isEqualTo("PUBLISHED");
        assertThat(jdbc.queryForObject("select applied_count from payment_projection", Integer.class)).isEqualTo(1);
    }

    @Test
    void returnsOriginalResultForSameKeyAndRejectsChangedPayload() throws Exception {
        UUID accountId = UUID.randomUUID();
        String first = """
                {"accountId":"%s","amount":"10.00","currency":"EUR"}
                """.formatted(accountId);
        mvc.perform(post("/payments").header("Idempotency-Key", "same-key")
                        .contentType("application/json").content(first))
                .andExpect(status().isCreated());
        mvc.perform(post("/payments").header("Idempotency-Key", "same-key")
                        .contentType("application/json").content(first))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.replayed").value(true));
        mvc.perform(post("/payments").header("Idempotency-Key", "same-key")
                        .contentType("application/json")
                        .content(first.replace("10.00", "11.00")))
                .andExpect(status().isConflict());

        assertThat(jdbc.queryForObject("select count(*) from payment", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from outbox_event", Integer.class)).isEqualTo(1);
    }

    @Test
    void rejectsMoneyOutsideDatabasePrecisionBeforeOpeningTheCommand() throws Exception {
        String request = """
                {"accountId":"%s","amount":"1.001","currency":"EUR"}
                """.formatted(UUID.randomUUID());

        mvc.perform(post("/payments").header("Idempotency-Key", "invalid-money")
                        .contentType("application/json").content(request))
                .andExpect(status().isBadRequest());

        assertThat(jdbc.queryForObject("select count(*) from payment", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("select count(*) from outbox_event", Integer.class)).isZero();
    }

    @Test
    void concurrentRetriesCreateOnePaymentAndOneEvent() throws Exception {
        UUID accountId = UUID.randomUUID();
        var request = new CreatePaymentRequest(accountId, new BigDecimal("50.00"), "USD");
        try (var executor = Executors.newFixedThreadPool(2)) {
            var first = executor.submit(() -> payments.create("parallel-key", request));
            var second = executor.submit(() -> payments.create("parallel-key", request));
            PaymentResponse a = first.get();
            PaymentResponse b = second.get();
            assertThat(a.id()).isEqualTo(b.id());
            assertThat(a.replayed() ^ b.replayed()).isTrue();
        }
        assertThat(jdbc.queryForObject("select count(*) from payment", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from outbox_event", Integer.class)).isEqualTo(1);
    }

    @Test
    void consumerIgnoresRepeatedEventId() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        String payload = """
                {"eventId":"%s","eventType":"payment.accepted.v1","paymentId":"%s",
                 "accountId":"%s","amount":"9.99","currency":"USD","occurredAt":"2026-01-15T12:00:00Z"}
                """.formatted(eventId, paymentId, UUID.randomUUID());
        var event = objectMapper.readTree(payload);

        assertThat(projection.apply(event)).isTrue();
        assertThat(projection.apply(event)).isFalse();
        assertThat(jdbc.queryForObject("select applied_count from payment_projection", Integer.class)).isEqualTo(1);
    }

    @Test
    void malformedEventDoesNotConsumeItsDeduplicationKey() {
        String payload = """
                {"eventId":"%s","paymentId":"%s","accountId":"%s",
                 "amount":"9.999","currency":"USD","occurredAt":"2026-01-15T12:00:00Z"}
                """.formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        assertThatThrownBy(() -> projection.apply(objectMapper.readTree(payload)))
                .isInstanceOf(ArithmeticException.class);
        assertThat(jdbc.queryForObject("select count(*) from processed_event", Integer.class)).isZero();
        assertThat(projection.apply(objectMapper.readTree(payload.replace("9.999", "9.99")))).isTrue();
    }

    @Test
    void newEventIdCannotOverwriteAnAcceptedPayment() {
        UUID eventId = UUID.randomUUID();
        String payload = """
                {"eventId":"%s","paymentId":"%s","accountId":"%s",
                 "amount":"9.99","currency":"USD","occurredAt":"2026-01-15T12:00:00Z"}
                """.formatted(eventId, UUID.randomUUID(), UUID.randomUUID());
        projection.apply(objectMapper.readTree(payload));
        String conflict = payload.replace(eventId.toString(), UUID.randomUUID().toString()).replace("9.99", "8.00");
        assertThatThrownBy(() -> projection.apply(objectMapper.readTree(conflict)))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThat(jdbc.queryForObject("select count(*) from processed_event", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select amount from payment_projection", BigDecimal.class))
                .isEqualByComparingTo("9.99");
    }

    private static void await(Duration timeout, java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("condition was not met before timeout");
    }
}
