package io.kestra.plugin.nats.kv;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class DeleteTest {

	@Inject
	protected RunContextFactory runContextFactory;

	@Test
	public void deletePair() throws Exception {
		String bucket = createBucket();
		Map<String, Object> keyValuePair = putPair(bucket);
		List<String> keys = new ArrayList<>(keyValuePair.keySet());

		Delete.builder()
			.url("localhost:4222")
			.username(Property.ofValue("kestra"))
			.password(Property.ofValue("k3stra"))
			.bucketName(bucket)
			.keys(Property.ofValue(
				keys
			))
			.build()
			.run(runContextFactory.of());

		Get.Output getOutput = Get.builder()
			.url("localhost:4222")
			.username(Property.ofValue("kestra"))
			.password(Property.ofValue("k3stra"))
			.bucketName(bucket)
			.keys(Property.ofValue(
				new ArrayList<>(keyValuePair.keySet())
			))
			.build()
			.run(runContextFactory.of());

		assertThat(getOutput.getOutput(), anEmptyMap());
	}

	public Map<String, Object> putPair(String bucket) throws Exception {
		Map<String, Object> keyValuePair = Map.of(
			"key1", "value1",
			"key2", "value2",
			"key3", "3"
		);

		Put.builder()
			.url("localhost:4222")
			.username(Property.ofValue("kestra"))
			.password(Property.ofValue("k3stra"))
			.bucketName(bucket)
			.values(Property.ofValue(
				keyValuePair
			))
			.build()
			.run(runContextFactory.of());

		return keyValuePair;
	}

	public String createBucket() throws Exception {
		CreateBucket.Output bucketOutput = CreateBucket.builder()
			.url("localhost:4222")
			.username(Property.ofValue("kestra"))
			.password(Property.ofValue("k3stra"))
			.name(IdUtils.create())
			.build()
			.run(runContextFactory.of());

		return bucketOutput.getBucket();
	}

}
