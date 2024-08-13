package io.kestra.plugin.nats.kv;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class PutTest {

	@Inject
	protected RunContextFactory runContextFactory;

	@Test
	public void putPair() throws Exception {
		String bucket = createBucket();

		Put.Output putOutput = Put.builder()
			.url("localhost:4222")
			.username("kestra")
			.password("k3stra")
			.bucketName(bucket)
			.values(
				Map.of(
					"key1", "value1",
					"key2", 2L,
					"key3", Map.of("subkey", "subvalue")
				)
			)
			.build()
			.run(runContextFactory.of());

		assertThat(putOutput.getRevisions(), notNullValue());
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
