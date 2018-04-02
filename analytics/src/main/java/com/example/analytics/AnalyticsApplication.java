package com.example.analytics;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.QueryableStoreRegistry;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.example.analytics.AnalyticsBinding.*;

@EnableBinding(AnalyticsBinding.class)
@SpringBootApplication
public class AnalyticsApplication {

		@Component
		public static class PageEventSource implements ApplicationRunner {

				private final Log log = LogFactory.getLog(getClass());

				private final List<String> pages = Arrays.asList("blog", "initializr", "news", "rss", "sitemap", "about", "colophon");
				private final List<String> users = Arrays.asList("jlong", "jwatters", "dsyer", "pwebb", "mfisher");

				private final MessageChannel out;

				public PageEventSource(AnalyticsBinding binding) {
						this.out = binding.pageViewEventsOut();
				}

				@Override
				public void run(ApplicationArguments args) throws Exception {

						Runnable runnable = () -> {
								PageViewEvent pageViewEvent = new PageViewEvent(random(this.users), random(this.pages), Math.random() > .5 ? 10 : 1000);
								Message<PageViewEvent> message = MessageBuilder
									.withPayload(pageViewEvent)
									.setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUser().getBytes())
									.build();
								try {
										this.out.send(message);
										this.log.info("sent " + pageViewEvent);
								}
								catch (Exception e) {
										this.log.error(e);
								}
						};
						Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
				}

				private static <T> T random(List<T> ts) {
						return ts.get(new Random().nextInt(ts.size()));
				}
		}

		@Component
		public static class PageEventSink {

				@StreamListener
				@SendTo(PAGE_COUNTS_OUT)
				public KStream<String, Long> process(@Input(PAGE_VIEW_IN) KStream<String, PageViewEvent> events) {
						return events
							.filter((key, value) -> value.getDurationSpentOnPage() > 10)
							.map((key, value) -> new KeyValue<>(value.getPage(), "0"))
							.groupByKey()
							.count(Materialized.as(PAGE_COUNTS_MV))
							.toStream();
				}
		}

		@Component
		public static class PageCountSink {

				private final Log log = LogFactory.getLog(getClass());

				@StreamListener
				public void process(@Input(PAGE_COUNTS_IN) KTable<String, Long> counts) {
						counts
							.toStream()
							.foreach((key, value) -> log.info(key + '=' + value));
				}
		}

		@RestController
		public static class CountsRestController {

				private final QueryableStoreRegistry registry;

				public CountsRestController(QueryableStoreRegistry registry) {
						this.registry = registry;
				}

				@GetMapping("/counts")
				Map<String, Long> counts() {
						ReadOnlyKeyValueStore<String, Long> store = registry.getQueryableStoreType(PAGE_COUNTS_MV, QueryableStoreTypes.keyValueStore());

						Map<String, Long> m = new HashMap<>();
						KeyValueIterator<String, Long> iterator = store.all();
						while (iterator.hasNext()) {
								KeyValue<String, Long> next = iterator.next();
								m.put(next.key, next.value);
						}
						return m;
				}
		}

		public static void main(String[] args) {
				SpringApplication.run(AnalyticsApplication.class, args);
		}
}

interface AnalyticsBinding {

		String PAGE_VIEW_OUT = "pveo";
		String PAGE_VIEW_IN = "pvei";

		String PAGE_COUNTS_OUT = "pco";
		String PAGE_COUNTS_IN = "pci";
		String PAGE_COUNTS_MV = "pcmview";

		@Input(PAGE_COUNTS_IN)
		KTable<String, Long> pageCountsIn();

		@Output(PAGE_COUNTS_OUT)
		KStream<String, Long> pageCountOut();

		@Output(PAGE_VIEW_OUT)
		MessageChannel pageViewEventsOut();

		@Input(PAGE_VIEW_IN)
		KStream<String, PageViewEvent> pageViewEventsIn();
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent {
		private String user, page;
		private long durationSpentOnPage;
}