package org.springframework.data.jdbc.repository;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
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
import org.springframework.transaction.annotation.Transactional;

// TODO: Should these be moved into  JdbcRepositoryWithCollectionsIntegrationTests?
@ExtendWith(SpringExtension.class)
@Transactional
public class JdbcRepositoryMappedCollectionIntegrationTests {
	@Autowired EntityWithCollectiblesRepository entityWithCollectiblesRepository;

	@Autowired EntityWithNestedCollectiblesRepository entityWithNestedCollectiblesRepository;

	@Autowired OtherEntityRepository otherEntityRepository;

	private OtherEntity otherEntity = new OtherEntity();

	@BeforeEach
	void setUp() {
		otherEntity.name = "someName";
		otherEntity = otherEntityRepository.save(otherEntity);
	}

	@Test
	void mapsCollectionsAsSet() {

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

	@Test
	void mapsCollectionsNestedUnderListElement() {
		EntityWithNestedCollectibles entity = new EntityWithNestedCollectibles();
		RootListCollectible rootListCollectible = new RootListCollectible();
		NestedCollectible nestedCollectible = new NestedCollectible("someName");
		rootListCollectible.nestedCollectibles = singletonList(nestedCollectible);
		entity.rootListCollectibles = singletonList(rootListCollectible);

		entity = entityWithNestedCollectiblesRepository.save(entity);

		EntityWithNestedCollectibles reloaded = entityWithNestedCollectiblesRepository.findById(entity.id).get();
		assertThat(reloaded.rootListCollectibles).hasSize(1);
		assertThat(reloaded.rootListCollectibles.get(0).nestedCollectibles).containsExactly(nestedCollectible);
	}

	@Test
	void mapsCollectionsNestedUnderSetElement() {
		EntityWithNestedCollectibles entity = new EntityWithNestedCollectibles();
		RootSetCollectible rootSetCollectible = new RootSetCollectible();
		NestedCollectible nestedCollectible = new NestedCollectible("someName");
		rootSetCollectible.otherEntityId = AggregateReference.to(otherEntity.id);
		rootSetCollectible.nestedCollectibles = singletonList(nestedCollectible);
		entity.rootSetCollectibles = singleton(rootSetCollectible);

		entity = entityWithNestedCollectiblesRepository.save(entity);

		EntityWithNestedCollectibles reloaded = entityWithNestedCollectiblesRepository.findById(entity.id).get();
		assertThat(reloaded.rootSetCollectibles).hasSize(1);
		assertThat(reloaded.rootSetCollectibles.stream().findFirst().get().nestedCollectibles).containsExactly(nestedCollectible);
	}

	@Test
	void mapsCollectionsNestedUnderMapElement() {
		EntityWithNestedCollectibles entity = new EntityWithNestedCollectibles();
		RootMapCollectible rootMapCollectible = new RootMapCollectible();
		NestedCollectible nestedCollectible = new NestedCollectible("someName");
		rootMapCollectible.value = 123;
		rootMapCollectible.nestedCollectibles = singletonList(nestedCollectible);
		entity.rootMapCollectibles = singletonMap("someKeyName", rootMapCollectible);

		entity = entityWithNestedCollectiblesRepository.save(entity);

		EntityWithNestedCollectibles reloaded = entityWithNestedCollectiblesRepository.findById(entity.id).get();
		assertThat(reloaded.rootMapCollectibles.get("someKeyName").nestedCollectibles).hasSize(1);
		assertThat(reloaded.rootMapCollectibles.get("someKeyName").nestedCollectibles).containsExactly(nestedCollectible);
	}

	interface EntityWithNestedCollectiblesRepository extends CrudRepository<EntityWithNestedCollectibles, Long> {}

	static class EntityWithNestedCollectibles {
		@Id Long id;

		@MappedCollection(idColumn = "ENTITY_WITH_NESTED_COLLECTIBLES_ID", keyColumn = "INDEX")
		List<RootListCollectible> rootListCollectibles;

		@MappedCollection(idColumn = "ENTITY_WITH_NESTED_COLLECTIBLES_ID", keyColumn = "OTHER_ENTITY_ID")
		Set<RootSetCollectible> rootSetCollectibles;

		@MappedCollection(idColumn = "ENTITY_WITH_NESTED_COLLECTIBLES_ID", keyColumn = "NAME")
		Map<String, RootMapCollectible> rootMapCollectibles;
	}

	static class RootListCollectible {
//		@Id Long id; // Adding this causes test to pass, but should not be necessary

		@MappedCollection(idColumn = "ROOT_COLLECTIBLE_ID", keyColumn = "INDEX")
		List<NestedCollectible> nestedCollectibles;
	}

	static class RootSetCollectible {
//		@Id Long id; // Adding this causes test to pass, but should not be necessary

		AggregateReference<OtherEntity, Long> otherEntityId;

		@MappedCollection(idColumn = "ROOT_COLLECTIBLE_ID", keyColumn = "INDEX")
		List<NestedCollectible> nestedCollectibles;
	}

	static class RootMapCollectible {
//		@Id Long id; // Adding this causes test to pass, but should not be necessary

		Integer value;

		@MappedCollection(idColumn = "ROOT_COLLECTIBLE_ID", keyColumn = "INDEX")
		List<NestedCollectible> nestedCollectibles;
	}

	@Value
	static class NestedCollectible {
		String name;
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

		@Bean
		EntityWithNestedCollectiblesRepository entityWithNestedCollectiblesRepository() {
			return factory.getRepository(EntityWithNestedCollectiblesRepository.class);
		}
	}
}
