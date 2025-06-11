package io.kestra.plugin.nats.kv;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
public class CreateBucketTest {

	@Inject
	protected RunContextFactory runContextFactory;

	@Test
	public void createBucket() throws Exception {
		String bucketName = getBucketName();

		CreateBucket.Output bucketOutput = CreateBucket.builder()
			.url("localhost:4222")
			.username(Property.ofValue("kestra"))
			.password(Property.ofValue("k3stra"))
			.name(bucketName)
			.build()
			.run(runContextFactory.of());

		assertThat(bucketOutput.getBucket(), is(bucketName));

		assertThat(bucketOutput.getHistory(), is(1L));
		assertThat(bucketOutput.getEntryCount(), is(0L));
		assertThat(bucketOutput.getBucketSize(), is(-1L));
		assertThat(bucketOutput.getValueSize(), is(-1L));

		assertThat(bucketOutput.getDescription(), nullValue());

		assertThat(bucketOutput.getMetadata(), notNullValue());
	}

	@Test
	public void createFullBucket() throws Exception {
		String bucketName = getBucketName();

		CreateBucket.Output bucketOutput = CreateBucket.builder()
			.url("localhost:4222")
			.username(Property.ofValue("kestra"))
			.password(Property.ofValue("k3stra"))
			.name(bucketName)
			.description("My Test Bucket")
			.bucketSize(Property.ofValue(1024L))
			.valueSize(Property.ofValue(1024L))
			.metadata(Property.ofValue(Map.of("key1", "value1", "key2", "value2")))
			.build()
			.run(runContextFactory.of());

		assertThat(bucketOutput.getBucket(), is(bucketName));

		assertThat(bucketOutput.getHistory(), is(1L));
		assertThat(bucketOutput.getEntryCount(), is(0L));
		assertThat(bucketOutput.getBucketSize(), is(1024L));
		assertThat(bucketOutput.getValueSize(), is(1024L));

        assertThat(bucketOutput.getDescription(), is("My Test Bucket"));

		assertThat(
			bucketOutput.getMetadata(),
			Matchers.allOf(
				Matchers.hasEntry("key1", "value1"),
				Matchers.hasEntry("key2", "value2")
			)
		);
	}

	private static String getBucketName() {
		return "kestra-test-" + IdUtils.create();
	}

}
