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

package org.springframework.data.jdbc.core.convert.sqlgeneration;

import static org.springframework.data.jdbc.core.convert.sqlgeneration.SqlAssert.*;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.IdentifierProcessing;

class AnalyticSqlGeneratorTests {

	JdbcMappingContext context = new JdbcMappingContext();
	AliasFactory aliasFactory = new AliasFactory();
	AnalyticSqlGenerator sqlGenerator = new AnalyticSqlGenerator(TestDialect.INSTANCE, new AggregateToStructure(context),
			new StructureToSelect(aliasFactory));

	@Nested
	class Conditions {
		@Test
		void findById() {

			RelationalPersistentEntity<?> dummyEntity = getRequiredPersistentEntity(DummyEntity.class);

			String sql = sqlGenerator.findById(dummyEntity);

			Assertions.assertThat(sql).isNotNull();

			assertThatParsed(sql).hasWhereClause();

		}
	}

	@Nested
	class SelectConstruction {
		@Test
		void simpleEntity() {

			RelationalPersistentEntity<?> dummyEntity = getRequiredPersistentEntity(DummyEntity.class);

			String sql = sqlGenerator.findAll(dummyEntity);

			assertThatParsed(sql).withAliases(aliasFactory)//
					.hasExactColumns( //
							from(dummyEntity) //
									.property("id") //
									.property("aColumn"));
		}

		@Test
		void singleReference() {

			RelationalPersistentEntity<?> singleRefEntity = getRequiredPersistentEntity(SingleReference.class);

			String sql = sqlGenerator.findAll(singleRefEntity);

			System.out.println(sql);

			assertThatParsed(sql) //
					.withAliases(aliasFactory) //
					.hasExactColumns( //
							from(singleRefEntity) //
									.property("dummy.id") //
									.property("dummy.aColumn") //
									.alias("RN0001") //
									.fk("FK0001_SINGLEREFERENCEID")
									.greatest()
									.greatest()
					).assignsAliasesExactlyOnce() //
					.selectsInternally("dummy", "single_reference")

			;
		}
	}
	private ColumnsSpec from(RelationalPersistentEntity<?> entity) {
		return SqlAssert.from(context, entity);
	}

	private RelationalPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityClass) {
		return context.getRequiredPersistentEntity(entityClass);
	}

	static class TestDialect extends AnsiDialect {

		static TestDialect INSTANCE = new TestDialect();

		@Override
		public IdentifierProcessing getIdentifierProcessing() {
			return IdentifierProcessing.NONE;
		}
	}

	static class DummyEntity {
		@Id Long id;

		String aColumn;
	}

	static class SingleReference {
		@Id Long id;
		DummyEntity dummy;
	}
}
