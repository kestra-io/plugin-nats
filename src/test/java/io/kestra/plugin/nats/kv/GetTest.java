package io.kestra.plugin.nats.kv;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class GetTest {

	@Inject
	protected RunContextFactory runContextFactory;

	@Test
	public void getPair() throws Exception {
		String bucket = createBucket();

		Map<String, Object> subMap = Map.of(
			"Title", "kestra test",
			"Description", "Unit test for Get task"
		);

		Map<String, Object> keyValuePair = Map.of(
			"key1", "value1",
			"key2", 2,
			"key3", subMap
		);

		putPair(bucket, keyValuePair);

		Get.Output getOutput = Get.builder()
			.url("localhost:4222")
			.username("kestra")
			.password("k3stra")
			.bucketName(bucket)
			.keys(
				new ArrayList<>(keyValuePair.keySet())
			)
			.build()
			.run(runContextFactory.of());

		Map<String, Object> result = getOutput.getOutput();

		assertThat(result, notNullValue());

		assertThat(result, Matchers.hasEntry("key1", "value1"));
		assertThat(result, Matchers.hasEntry("key2", 2));
		assertThat(result, Matchers.hasEntry("key3", subMap));
	}

	public void putPair(String bucket, Map<String, Object> keyValuePair) throws Exception {
		Put.builder()
			.url("localhost:4222")
			.username("kestra")
			.password("k3stra")
			.bucketName(bucket)
			.values(
				keyValuePair
			)
			.build()
			.run(runContextFactory.of());
	}

	public String createBucket() throws Exception {
		CreateBucket.Output bucketOutput = CreateBucket.builder()
			.url("localhost:4222")
			.username("kestra")
			.password("k3stra")
			.name(IdUtils.create())
			.build()
			.run(runContextFactory.of());

		return bucketOutput.getBucket();
	}

}
