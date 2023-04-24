/*
 * Copyright 2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.jdbc.core.convert;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.context.TestExecutionListeners.MergeMode.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.testing.AssumeFeatureTestExecutionListener;
import org.springframework.data.jdbc.testing.EnabledOnFeature;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.jdbc.testing.TestDatabaseFeatures;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.annotation.Transactional;

@ContextConfiguration
@Transactional
@TestExecutionListeners(value = AssumeFeatureTestExecutionListener.class, mergeMode = MERGE_WITH_DEFAULTS)
@ExtendWith(SpringExtension.class)
public class SingleSelectIntegrationTests {

	@Autowired JdbcMappingContext jdbcMappingContext;
	@Autowired JdbcConverter converter;

	@Autowired Dialect dialect;

	@Autowired JdbcAggregateTemplate aggregateTemplate;

	@Autowired NamedParameterJdbcTemplate jdbcTemplate;

	AggregateReaderFactory readerFactory;

	@BeforeEach
	void before() {
		readerFactory = new AggregateReaderFactory(jdbcMappingContext, dialect, converter, jdbcTemplate);
	}

	@Nested
	class Conditions {
		@Test
		@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SINGLE_SELECT_QUERY)
		void findById() {
			RelationalPersistentEntity<DummyEntity> entity = (RelationalPersistentEntity<DummyEntity>) jdbcMappingContext
					.getRequiredPersistentEntity(DummyEntity.class);

			AggregateReader<DummyEntity> reader = readerFactory.createAggregateReaderFor(entity);

			DummyEntity jens = aggregateTemplate.save(new DummyEntity(null, "Jens"));
			DummyEntity mark = aggregateTemplate.save(new DummyEntity(null, "Mark"));

			assertThat(reader.findById(mark.id)).isEqualTo(mark);
		}


		@Test
		@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SINGLE_SELECT_QUERY)
		void findSingleSetById() {

			SingleSet singleSetOne = aggregateTemplate.save(new SingleSet(null, new HashSet<>(asList( //
					new DummyEntity(null, "Jens"), //
					new DummyEntity(null, "Mark") //
			))));

			SingleSet singleSetTwo = aggregateTemplate.save(new SingleSet(null, new HashSet<>(asList( //
					new DummyEntity(null, "Olli") //
			))));

			RelationalPersistentEntity<SingleSet> singleSetPersistentEntity = (RelationalPersistentEntity<SingleSet>) jdbcMappingContext
					.getRequiredPersistentEntity(SingleSet.class);

			AggregateReader<SingleSet> reader = readerFactory.createAggregateReaderFor(singleSetPersistentEntity);
			assertThat(reader.findById(singleSetOne.id)).isEqualTo(singleSetOne);
		}

		@Test
		@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SINGLE_SELECT_QUERY)
		void findAllById() {
			RelationalPersistentEntity<DummyEntity> entity = (RelationalPersistentEntity<DummyEntity>) jdbcMappingContext
					.getRequiredPersistentEntity(DummyEntity.class);

			AggregateReader<DummyEntity> reader = readerFactory.createAggregateReaderFor(entity);

			DummyEntity jens = aggregateTemplate.save(new DummyEntity(null, "Jens"));
			DummyEntity mark = aggregateTemplate.save(new DummyEntity(null, "Mark"));
			DummyEntity olli = aggregateTemplate.save(new DummyEntity(null, "Olli"));

			assertThat(reader.findAllById(asList(mark.id, olli.id))).containsExactly(mark, olli);
		}

	}

	@Nested
	class Mappings {
		@Test
		@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SINGLE_SELECT_QUERY)
		void simpleEntity() {
			RelationalPersistentEntity<DummyEntity> entity = (RelationalPersistentEntity<DummyEntity>) jdbcMappingContext
					.getRequiredPersistentEntity(DummyEntity.class);

			AggregateReader<DummyEntity> reader = readerFactory.createAggregateReaderFor(entity);

			DummyEntity saved = aggregateTemplate.save(new DummyEntity(null, "Jens"));

			assertThat(reader.findAll()).containsExactly(saved);
		}

		@Test
		@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SINGLE_SELECT_QUERY)
		void singleReference() {

			RelationalPersistentEntity<SingleReference> entity = (RelationalPersistentEntity<SingleReference>) jdbcMappingContext
					.getRequiredPersistentEntity(SingleReference.class);

			AggregateReader<SingleReference> reader = readerFactory.createAggregateReaderFor(entity);

			SingleReference singleReference = new SingleReference(null, new DummyEntity(null, "Jens"));
			SingleReference saved = aggregateTemplate.save(singleReference);

			assertThat(reader.findAll()).containsExactly(saved);
		}
		@Test
		@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SINGLE_SELECT_QUERY)
		void singleSet() {

			RelationalPersistentEntity<SingleSet> entity = (RelationalPersistentEntity<SingleSet>) jdbcMappingContext
					.getRequiredPersistentEntity(SingleSet.class);

			AggregateReader<SingleSet> reader = readerFactory.createAggregateReaderFor(entity);

			SingleSet aggregateRoot = new SingleSet(null, new HashSet<>(asList( //
					new DummyEntity(null, "Jens"), //
					new DummyEntity(null, "Mark") //
			)));
			SingleSet saved = aggregateTemplate.save(aggregateRoot);

			assertThat(reader.findAll()).containsExactly(saved);
		}

		@Test
		@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SINGLE_SELECT_QUERY)
		void singleList() {

			RelationalPersistentEntity<SingleList> entity = (RelationalPersistentEntity<SingleList>) jdbcMappingContext
					.getRequiredPersistentEntity(SingleList.class);

			AggregateReader<SingleList> reader = readerFactory.createAggregateReaderFor(entity);

			SingleList aggregateRoot = new SingleList(null, asList( //
					new DummyEntity(null, "Jens"), //
					new DummyEntity(null, "Mark") //
			));
			SingleList saved = aggregateTemplate.save(aggregateRoot);

			assertThat(reader.findAll()).containsExactly(saved);
		}

		@Test
		@EnabledOnFeature(TestDatabaseFeatures.Feature.SUPPORTS_SINGLE_SELECT_QUERY)
		void singleMap() {

			RelationalPersistentEntity<SingleMap> entity = (RelationalPersistentEntity<SingleMap>) jdbcMappingContext
					.getRequiredPersistentEntity(SingleMap.class);

			AggregateReader<SingleMap> reader = readerFactory.createAggregateReaderFor(entity);

			HashMap<String, DummyEntity> map = new HashMap<>();
			map.put("one", new DummyEntity(null, "Jens"));
			map.put("two", new DummyEntity(null, "Mark"));

			SingleMap aggregateRoot = new SingleMap(null, map);
			SingleMap saved = aggregateTemplate.save(aggregateRoot);

			assertThat(reader.findAll()).containsExactly(saved);
		}
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		Class<?> testClass() {
			return SingleSelectIntegrationTests.class;
		}

		@Bean
		JdbcAggregateOperations operations(ApplicationEventPublisher publisher, RelationalMappingContext context,
				DataAccessStrategy dataAccessStrategy, JdbcConverter converter) {
			return new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
		}
	}

	record DummyEntity(@Id Integer id, String name) {
	}

	record SingleReference(@Id Integer id, DummyEntity dummy) {
	}


	record SingleSet(@Id Integer id, Set<DummyEntity> dummies) {
	}

	record SingleList(@Id Integer id, List<DummyEntity> dummies) {
	}

	record SingleMap(@Id Integer id, Map<String, DummyEntity> dummies) {
	}
}
