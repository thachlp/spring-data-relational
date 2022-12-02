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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;

import java.util.List;

class AggregateToStructureUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();

	AggregateToStructure ats = new AggregateToStructure(context);
	RelationalPersistentEntity<?> rootEntity;

	@Test
	void simpleTable() {

		rootEntity = context.getRequiredPersistentEntity(DummyEntity.class);
		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select select = ats
				.createSelectStructure(rootEntity);

		AnalyticAssertions.assertThat(select) //
				.hasExactColumns( //
						path("id"), //
						path("aColumn"))
				.isInstanceOf(AnalyticStructureBuilder.TableDefinition.class) //
				.hasId(new PersistentPropertyPathExtension(context,
						context.getPersistentPropertyPath("id", rootEntity.getType())));
	}

	@Test
	void singleReference() {

		rootEntity = context.getRequiredPersistentEntity(SingleReference.class);
		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select select = ats
				.createSelectStructure(rootEntity);

		AnalyticAssertions.assertThat(select) //
				.hasAtLeastColumns( //
						path("dummy.id"), //
						path("dummy.aColumn") //
				).isInstanceOf(AnalyticStructureBuilder.AnalyticJoin.class);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select child = ((AnalyticStructureBuilder.AnalyticJoin) select)
				.getChild();

		Assertions.assertThat(child).isInstanceOf(AnalyticStructureBuilder.AnalyticView.class);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticView view = (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticView) child;

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select viewChild = view
				.getFroms().get(0);
		Assertions.assertThat(viewChild).isInstanceOf(AnalyticStructureBuilder.TableDefinition.class);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition tableDefinition = (AnalyticStructureBuilder.TableDefinition) viewChild;

		// TODO write a proper assert or drop this test.
	}

	/* TODO:
	 * - make these test so that they check properly of the present of columns
	 * - add test that checks for the list_key column
	 * - modify AggregateToStructure, to take care of the key column
	 */

	@Test
	void singleList() {

		rootEntity = context.getRequiredPersistentEntity(SingleList.class);
		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select select = ats
				.createSelectStructure(rootEntity);

		AnalyticAssertions.assertThat(select) //
				.hasAtLeastColumns( //
						path("dummy.id"), //
						path("dummy.aColumn") //
				).isInstanceOf(AnalyticStructureBuilder.AnalyticJoin.class);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select child = ((AnalyticStructureBuilder.AnalyticJoin) select)
				.getChild();

		Assertions.assertThat(child).isInstanceOf(AnalyticStructureBuilder.AnalyticView.class);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticView view = (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticView) child;

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select viewChild = view
				.getFroms().get(0);
		Assertions.assertThat(viewChild).isInstanceOf(AnalyticStructureBuilder.TableDefinition.class);

		AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition tableDefinition = (AnalyticStructureBuilder.TableDefinition) viewChild;

		// TODO write a proper assert or drop this test.
	}

	private PersistentPropertyPathExtension path(String path) {
		return new PersistentPropertyPathExtension(context, context.getPersistentPropertyPath(path, rootEntity.getType()));
	}

	static class DummyEntity {
		@Id Long id;

		String aColumn;
	}

	static class SingleReference {
		@Id Long id;
		DummyEntity dummy;
	}
	static class SingleList {
		@Id Long id;
		List<DummyEntity> dummy;
	}
}
