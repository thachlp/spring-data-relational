package org.springframework.data.jdbc.repository;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import lombok.Data;
import lombok.Value;

// TODO: Should these be moved into  JdbcRepositoryWithCollectionsIntegrationTests?
@ExtendWith(SpringExtension.class)
public class JdbcRepositoryMappedCollectionIntegrationTests {
	@Autowired EntityWithCollectiblesRepository entityWithCollectiblesRepository;

	@Autowired OtherEntityRepository otherEntityRepository;

	@Test
	void mapsCollectionsAsSet() {
		OtherEntity otherEntity = new OtherEntity();
		otherEntity.name = "someName";
		otherEntity = otherEntityRepository.save(otherEntity);

		EntityWithCollectibles entity = new EntityWithCollectibles();
		SetCollectible setCollectible = new SetCollectible();
		setCollectible.otherEntityId = AggregateReference.to(otherEntity.id);
		entity.setCollectibles = new HashSet<>(singletonList(setCollectible));

		entity = entityWithCollectiblesRepository.save(entity);

		EntityWithCollectibles reloaded = entityWithCollectiblesRepository.findById(entity.id).get();
		assertThat(reloaded.setCollectibles).containsExactly(setCollectible);
	}

	@Test
	void mapsCollectionsAsList() {
		EntityWithCollectibles entity = new EntityWithCollectibles();
		ListCollectible listCollectible = new ListCollectible("someName");
		entity.listCollectibles = singletonList(listCollectible);

		entity = entityWithCollectiblesRepository.save(entity);

		EntityWithCollectibles reloaded = entityWithCollectiblesRepository.findById(entity.id).get();
		assertThat(reloaded.listCollectibles).containsExactly(listCollectible);
	}

	@Test
	void mapsCollectionsAsMap() {
		EntityWithCollectibles entity = new EntityWithCollectibles();
		MapCollectible mapCollectible = new MapCollectible(123);
		entity.mapCollectibles = singletonMap("someName", mapCollectible);

		entity = entityWithCollectiblesRepository.save(entity);

		EntityWithCollectibles reloaded = entityWithCollectiblesRepository.findById(entity.id).get();
		assertThat(reloaded.mapCollectibles.get("someName")).isEqualTo(mapCollectible);
	}

	interface EntityWithCollectiblesRepository extends CrudRepository<EntityWithCollectibles, Long> {}

	interface OtherEntityRepository extends CrudRepository<OtherEntity, Long> {}

	static class EntityWithCollectibles {
		@Id Long id;

		@MappedCollection(idColumn = "ENTITY_WITH_COLLECTIBLES_ID", keyColumn = "OTHER_ENTITY_ID")
		Set<SetCollectible> setCollectibles;

		@MappedCollection(idColumn = "ENTITY_WITH_COLLECTIBLES_ID", keyColumn = "INDEX")
		List<ListCollectible> listCollectibles;

		@MappedCollection(idColumn = "ENTITY_WITH_COLLECTIBLES_ID", keyColumn = "NAME")
		Map<String, MapCollectible> mapCollectibles;
	}

	@Value
	static class MapCollectible {
		Integer value;
	}

	@Data
	static class SetCollectible {
		AggregateReference<OtherEntity, Long> otherEntityId;
	}

	@Value
	static class ListCollectible {
		String name;
	}

	@Data
	static class OtherEntity {
		@Id Long id;
		String name;
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {
		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryMappedCollectionIntegrationTests.class;
		}

		@Bean
		EntityWithCollectiblesRepository dummyEntityRepository() {
			return factory.getRepository(EntityWithCollectiblesRepository.class);
		}

		@Bean
		OtherEntityRepository otherEntityRepository() {
			return factory.getRepository(OtherEntityRepository.class);
		}
	}
}
