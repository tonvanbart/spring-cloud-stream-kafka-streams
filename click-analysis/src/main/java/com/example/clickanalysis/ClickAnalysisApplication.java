package com.example.clickanalysis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
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
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.example.clickanalysis.PageViewBinding.PAGE_TO_COUNTS;
import static com.example.clickanalysis.PageViewBinding.PAGE_VIEW_EVENTS_IN;

@SpringBootApplication
@EnableBinding(PageViewBinding.class)
public class ClickAnalysisApplication {

		private final Log log = LogFactory.getLog(getClass());

		@Component
		public static class Producer implements ApplicationRunner {

				private final MessageChannel pageViewsOut;
				private final Log log = LogFactory.getLog(getClass());

				public Producer(PageViewBinding binding) {
						this.pageViewsOut = binding.pageViewEventsOut();
				}

				@Override
				public void run(ApplicationArguments args) {
						List<String> pages = Arrays.asList("news", "initializr", "blog", "about", "sitemap", "colophon");
						List<String> users = Arrays.asList("jgrelle", "mbhave", "cdavis", "ehendrickson", "dsyer", "pwebb", "jlong", "mgray");
						Runnable runnable = () -> {
								String user = users.get(random(users.size() - 1));
								String page = pages.get(random(pages.size() - 1));
								PageViewEvent event = new PageViewEvent(
									user, page, Math.random() > .5 ? 10 : 100);
								Message<PageViewEvent> message = MessageBuilder
									.withPayload(event)
									.setHeader(KafkaHeaders.MESSAGE_KEY, event.getUserId().getBytes())
									.build();
								try {
										this.pageViewsOut.send(message);
										log.info("sending " + event.toString());
								}
								catch (Exception e) {
										log.error(e);
								}
						};
						Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
				}

				private int random(int size) {
						return Math.max(0, Math.min(size, (int) (Math.random() * size)));
				}
		}

 	@Configuration
		public static class TableConsumer {

				private Log log = LogFactory.getLog(getClass());

				@StreamListener
				public void counts (@Input(PAGE_TO_COUNTS) KTable<String, Long> counts) {
						log.info("calling counts");
						counts
							.foreach(new ForeachAction<String, Long>() {
									@Override
									public void apply(String key, Long value) {
											log.info(key + "=" + value);
									}
							});
				}
		}

		@Configuration
		public static class StreamConsumer {

				private final Log log = LogFactory.getLog(getClass());

				@StreamListener
				public void processPageViews(@Input(PAGE_VIEW_EVENTS_IN) KStream<String, PageViewEvent> views) {
						views
							.filter((key, value) -> value.getTimeSpentInMilliseconds() > 10)
							.map((key, value) -> new KeyValue<>(value.getPage(), Long.toString(0)))
							.groupByKey()
							.count(Materialized.as(PAGE_TO_COUNTS));
//							.foreach((key, value) -> log.info(key + "=" + value));
				}
		}

		@RestController
		public static class CountRestController {

				private final QueryableStoreRegistry registry;

				public CountRestController(QueryableStoreRegistry registry) {
						this.registry = registry;
				}

				@GetMapping("/counts")
				Map<String, Long> counts() {
						ReadOnlyKeyValueStore<String, Long> type = this.registry
							.getQueryableStoreType(PAGE_TO_COUNTS, QueryableStoreTypes.keyValueStore());
						Map<String, Long> m = new HashMap<>();
						KeyValueIterator<String, Long> all = type.all();
						while (all.hasNext()) {
								KeyValue<String, Long> keyValue = all.next();
								m.put(keyValue.key, keyValue.value);
						}
						return m;
				}
		}

		public static void main(String[] args) {
				SpringApplication.run(ClickAnalysisApplication.class, args);
		}
}

interface PageViewBinding {

		String PAGE_TO_COUNTS = "ptc";

		String PAGE_VIEW_EVENTS_OUT = "pveout";
		String PAGE_VIEW_EVENTS_IN = "pvein";

		@Output(PAGE_VIEW_EVENTS_OUT)
		MessageChannel pageViewEventsOut();

		@Input(PAGE_VIEW_EVENTS_IN)
		KStream<String, PageViewEvent> pageViewEventsIn();

 @Input(PAGE_TO_COUNTS)	KTable<String, Long> updatedCounts();
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent {
		private String userId;
		private String page;
		private long timeSpentInMilliseconds;
}
