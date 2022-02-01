/*
 * Copyright 2020-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.mockito.Mockito.*;

import lombok.Data;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Value;
import org.assertj.core.api.SoftAssertions;
import org.h2.tools.SimpleResultSet;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.dialect.H2Dialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Unit tests for {@link BasicJdbcConverter}.
 *
 * @author Mark Paluch
 */
public class BasicJdbcConverterUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();
	StubbedJdbcTypeFactory typeFactory = new StubbedJdbcTypeFactory();
	NamedParameterJdbcOperations namedParameterJdbcOperations = mock(NamedParameterJdbcOperations.class);
	BasicJdbcConverter converter = new BasicJdbcConverter( //
			context, //
			this::findAllByPath, //
			new JdbcCustomConversions(), //
			typeFactory, IdentifierProcessing.ANSI //
	);

	private Iterable<Object> findAllByPath(Identifier identifier, PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
		SqlGeneratorSource sqlGeneratorSource = new SqlGeneratorSource(context, converter, H2Dialect.INSTANCE);
		return new DefaultDataAccessStrategy(sqlGeneratorSource, context, converter, namedParameterJdbcOperations)
				.findAllByPath(identifier, path);
	}

	@Test // DATAJDBC-104, DATAJDBC-1384
	public void testTargetTypesForPropertyType() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		SoftAssertions softly = new SoftAssertions();

		checkTargetType(softly, entity, "someEnum", String.class);
		checkTargetType(softly, entity, "localDateTime", LocalDateTime.class);
		checkTargetType(softly, entity, "localDate", Timestamp.class);
		checkTargetType(softly, entity, "localTime", Timestamp.class);
		checkTargetType(softly, entity, "zonedDateTime", String.class);
		checkTargetType(softly, entity, "offsetDateTime", OffsetDateTime.class);
		checkTargetType(softly, entity, "instant", Timestamp.class);
		checkTargetType(softly, entity, "date", Date.class);
		checkTargetType(softly, entity, "timestamp", Timestamp.class);
		checkTargetType(softly, entity, "uuid", UUID.class);

		softly.assertAll();
	}

	@Test // DATAJDBC-259
	public void classificationOfCollectionLikeProperties() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		RelationalPersistentProperty listOfString = entity.getRequiredPersistentProperty("listOfString");
		RelationalPersistentProperty arrayOfString = entity.getRequiredPersistentProperty("arrayOfString");

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(converter.getColumnType(arrayOfString)).isEqualTo(String[].class);
		softly.assertThat(converter.getColumnType(listOfString)).isEqualTo(String[].class);

		softly.assertAll();
	}

	@Test // DATAJDBC-221
	public void referencesAreNotEntitiesAndGetStoredAsTheirId() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		SoftAssertions softly = new SoftAssertions();

		RelationalPersistentProperty reference = entity.getRequiredPersistentProperty("reference");

		softly.assertThat(reference.isEntity()).isFalse();
		softly.assertThat(converter.getColumnType(reference)).isEqualTo(Long.class);

		softly.assertAll();
	}

	@Test // DATAJDBC-637
	void conversionOfDateLikeValueAndBackYieldsOriginalValue() {

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(DummyEntity.class);

		assertSoftly(softly -> {
			LocalDateTime testLocalDateTime = LocalDateTime.of(2001, 2, 3, 4, 5, 6, 123456789);
			checkConversionToTimestampAndBack(softly, persistentEntity, "localDateTime", testLocalDateTime);
			checkConversionToTimestampAndBack(softly, persistentEntity, "localDate", LocalDate.of(2001, 2, 3));
			checkConversionToTimestampAndBack(softly, persistentEntity, "localTime", LocalTime.of(1, 2, 3, 123456789));
			checkConversionToTimestampAndBack(softly, persistentEntity, "instant",
					testLocalDateTime.toInstant(ZoneOffset.UTC));
		});

	}

	@Test // GH-945
	void conversionOfPrimitiveArrays() {

		int[] ints = { 1, 2, 3, 4, 5 };
		JdbcValue converted = converter.writeJdbcValue(ints, ints.getClass(), JdbcUtil.sqlTypeFor(ints.getClass()));

		assertThat(converted.getValue()).isInstanceOf(Array.class);
		assertThat(typeFactory.arraySource).containsExactly(1, 2, 3, 4, 5);
	}

	@Test
	void readsEntityCollectionsAsList_orderingElementsByKey() throws SQLException {
		SimpleResultSet resultSet = new SimpleResultSet();
		resultSet.addColumn("ID", JDBCType.BIGINT.getVendorTypeNumber(), String.valueOf(Long.MAX_VALUE).length(), 0);
		resultSet.addRow(1L);
		assertThat(resultSet.next()).isTrue();

		converter.mapRow(context.getRequiredPersistentEntity(EntityWithCollectibles.class), resultSet, 1);

		ArgumentCaptor<SqlParameterSource> sqlParameterSourceCaptor = ArgumentCaptor.forClass(SqlParameterSource.class);
		ArgumentCaptor<RowMapper<?>> rowMapperCaptor = ArgumentCaptor.forClass(RowMapper.class);
		verify(namedParameterJdbcOperations).query(
				eq("SELECT \"LIST_COLLECTIBLE\".\"NAME\" AS \"NAME\", \"LIST_COLLECTIBLE\".\"ENTITY_WITH_COLLECTIBLES_KEY\" AS \"ENTITY_WITH_COLLECTIBLES_KEY\" " +
						"FROM \"LIST_COLLECTIBLE\" " +
						"WHERE \"LIST_COLLECTIBLE\".\"ENTITY_WITH_COLLECTIBLES\" = :entity_with_collectibles " +
						"ORDER BY \"ENTITY_WITH_COLLECTIBLES_KEY\""),
				sqlParameterSourceCaptor.capture(),
				rowMapperCaptor.capture());
		SqlParameterSource sqlParameterSource = sqlParameterSourceCaptor.getValue();
		assertThat(sqlParameterSource.getValue("entity_with_collectibles")).isEqualTo(1L);
	}

	@Test
	void readsEntityCollectionsAsMap() throws SQLException {
		SimpleResultSet resultSet = new SimpleResultSet();
		resultSet.addColumn("ID", JDBCType.BIGINT.getVendorTypeNumber(), String.valueOf(Long.MAX_VALUE).length(), 0);
		resultSet.addRow(1L);
		assertThat(resultSet.next()).isTrue();

		converter.mapRow(context.getRequiredPersistentEntity(EntityWithCollectibles.class), resultSet, 1);

		ArgumentCaptor<SqlParameterSource> sqlParameterSourceCaptor = ArgumentCaptor.forClass(SqlParameterSource.class);
		ArgumentCaptor<RowMapper<?>> rowMapperCaptor = ArgumentCaptor.forClass(RowMapper.class);
		verify(namedParameterJdbcOperations).query(
				eq("SELECT \"MAP_COLLECTIBLE\".\"VALUE\" AS \"VALUE\", \"MAP_COLLECTIBLE\".\"ENTITY_WITH_COLLECTIBLES_KEY\" AS \"ENTITY_WITH_COLLECTIBLES_KEY\" " +
						"FROM \"MAP_COLLECTIBLE\" " +
						"WHERE \"MAP_COLLECTIBLE\".\"ENTITY_WITH_COLLECTIBLES\" = :entity_with_collectibles"),
				sqlParameterSourceCaptor.capture(),
				rowMapperCaptor.capture());
		SqlParameterSource sqlParameterSource = sqlParameterSourceCaptor.getValue();
		assertThat(sqlParameterSource.getValue("entity_with_collectibles")).isEqualTo(1L);
	}

	@Test
	void readsEntityCollectionsAsSet() throws SQLException {
		SimpleResultSet resultSet = new SimpleResultSet();
		resultSet.addColumn("ID", JDBCType.BIGINT.getVendorTypeNumber(), String.valueOf(Long.MAX_VALUE).length(), 0);
		resultSet.addRow(1L);
		assertThat(resultSet.next()).isTrue();

		converter.mapRow(context.getRequiredPersistentEntity(EntityWithCollectibles.class), resultSet, 1);

		ArgumentCaptor<SqlParameterSource> sqlParameterSourceCaptor = ArgumentCaptor.forClass(SqlParameterSource.class);
		ArgumentCaptor<RowMapper<?>> rowMapperCaptor = ArgumentCaptor.forClass(RowMapper.class);
		verify(namedParameterJdbcOperations).query(
				eq("SELECT \"SET_COLLECTIBLE\".\"VALUE\" AS \"VALUE\" " +
						"FROM \"SET_COLLECTIBLE\" " +
						"WHERE \"SET_COLLECTIBLE\".\"ENTITY_WITH_COLLECTIBLES\" = :entity_with_collectibles"),
				sqlParameterSourceCaptor.capture(),
				rowMapperCaptor.capture());
		SqlParameterSource sqlParameterSource = sqlParameterSourceCaptor.getValue();
		assertThat(sqlParameterSource.getValue("entity_with_collectibles")).isEqualTo(1L);
	}

	static class EntityWithCollectibles {
		@Id Long id;

		List<ListCollectible> listCollectibles;

		Map<String, MapCollectible> mapCollectibles;

		Set<SetCollectible> setCollectibles;

		@Value
		static class ListCollectible {
			String name;
		}

		@Value
		static class MapCollectible {
			Integer value;
		}

		@Value
		static class SetCollectible {
			Integer value;
		}
	}

	private void checkConversionToTimestampAndBack(SoftAssertions softly, RelationalPersistentEntity<?> persistentEntity,
			String propertyName, Object value) {

		RelationalPersistentProperty property = persistentEntity.getRequiredPersistentProperty(propertyName);

		Object converted = converter.writeValue(value, ClassTypeInformation.from(converter.getColumnType(property)));
		Object convertedBack = converter.readValue(converted, property.getTypeInformation());

		softly.assertThat(convertedBack).describedAs(propertyName).isEqualTo(value);
	}

	private void checkTargetType(SoftAssertions softly, RelationalPersistentEntity<?> persistentEntity,
			String propertyName, Class<?> expected) {

		RelationalPersistentProperty property = persistentEntity.getRequiredPersistentProperty(propertyName);

		softly.assertThat(converter.getColumnType(property)).describedAs(propertyName).isEqualTo(expected);
	}

	@Data
	@SuppressWarnings("unused")
	private static class DummyEntity {

		@Id private final Long id;
		private final SomeEnum someEnum;
		private final LocalDateTime localDateTime;
		private final LocalDate localDate;
		private final LocalTime localTime;
		private final ZonedDateTime zonedDateTime;
		private final OffsetDateTime offsetDateTime;
		private final Instant instant;
		private final Date date;
		private final Timestamp timestamp;
		private final AggregateReference<DummyEntity, Long> reference;
		private final UUID uuid;

		// DATAJDBC-259
		private final List<String> listOfString;
		private final String[] arrayOfString;
		private final List<OtherEntity> listOfEntity;
		private final OtherEntity[] arrayOfEntity;

	}

	@SuppressWarnings("unused")
	private enum SomeEnum {
		ALPHA
	}

	@SuppressWarnings("unused")
	private static class OtherEntity {}

	private static class StubbedJdbcTypeFactory implements JdbcTypeFactory {
		public Object[] arraySource;

		@Override
		public Array createArray(Object[] value) {
			arraySource = value;
			return mock(Array.class);
		}
	}
}
