package wc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.java.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
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
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class WordCountApplication {


		@Component
		public static class WordsProducer implements ApplicationRunner {

				private final MessageChannel outbound;

				private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

				public WordsProducer(WordCountChannels channels) {
						this.outbound = channels.wordsOutbound();
				}

				private void produce() {
						try (BufferedReader br = new BufferedReader(
							     new InputStreamReader(WordCountApplication.class.getResourceAsStream("/data.txt")))) {
								String line;
								while ((line = br.readLine()) != null) {
										outbound.send(MessageBuilder.withPayload(line).build());
								}
						}
						catch (Exception e) {
								LogFactory.getLog(getClass()).error(e);
						}
				}

				@Override
				public void run(ApplicationArguments args) {
						this.executorService.scheduleWithFixedDelay(this::produce, 1, 10, TimeUnit.SECONDS);
				}
		}

		@Log
		@Configuration
		@EnableBinding(WordCountChannels.class)
		public static class WordsConsumer {

				@StreamListener
				public void process(@Input(WordCountChannels.WORDS_INBOUND) KStream<Object, String> words) {

						KTable<Windowed<String>, Long> count = words
							.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
							.map((key, value) -> new KeyValue<>(value, value))
							.groupByKey()
							.windowedBy(TimeWindows.of(10_000))
							.count(Materialized.as("word-counts"));

						count
							.toStream()
							.map((key, value) -> new KeyValue<>(null,
								new WordCount(key.key(), value, new Date(key.window().start()), new Date(key.window().end()))))
							.foreach((key, value) -> log.info("value: " + value));
				}
		}

		@RestController
		public static class WordsRestController {

				private final QueryableStoreRegistry queryableStoreRegistry;

				public WordsRestController(QueryableStoreRegistry queryableStoreRegistry) {
						this.queryableStoreRegistry = queryableStoreRegistry;
				}

				@GetMapping("/words/{name}")
				Map<String, Object> name(@PathVariable String name) {
						ReadOnlyWindowStore<String, Long> store = queryableStoreRegistry.getQueryableStoreType("word-counts", QueryableStoreTypes.windowStore());
						long timeFrom = 0;
						long timeTo = System.currentTimeMillis();
						WindowStoreIterator<Long> iterator = store.fetch(name, timeFrom, timeTo);
						Map<Date, Long> map = new HashMap<>();
						while (iterator.hasNext()) {
								KeyValue<Long, Long> next = iterator.next();
								Long timestamp = next.key;
								Long cardinality = next.value;
								map.put(new Date(timestamp), cardinality);
						}
						Map<String, Object> ret = new HashMap<>();
						ret.put("keyword", name);
						ret.put("changelog", map);
						return ret;
				}
		}

		public static void main(String[] args) {
				SpringApplication.run(WordCountApplication.class, args);
		}
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class WordCount {

		private String word;
		private long count;
		private Date start, end;
}


interface WordCountChannels {

		String WORDS_INBOUND = "wordsInbound";
		String WORDS_OUTBOUND = "wordsOutbound";

		@Input(WORDS_INBOUND)
		KStream<?, ?> wordsInbound();

		@Output(WORDS_OUTBOUND)
		MessageChannel wordsOutbound();
}

