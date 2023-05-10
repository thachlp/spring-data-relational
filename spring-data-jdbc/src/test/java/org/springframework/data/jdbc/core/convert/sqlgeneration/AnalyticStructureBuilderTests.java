/*
 * Copyright 2022 the original author or authors.
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

import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticAssertions.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticJoinPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticViewPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.ConditionPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.ForeignKeyPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.CoalescePattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.KeyPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.LiteralPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.MaxOverPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.RowNumberPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.TableDefinitionPattern.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class AnalyticStructureBuilderTests {

	/**
	 * A simple table should result in a simple select. Columns are represented by
	 * {@link org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticStructureBuilder.BaseColumn} since they are
	 * directly referenced.
	 */
	@Test
	void simpleTableWithColumns() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>() //
				.addTable("person", td -> td.withId("person_id").withColumns("value", "lastname")) //
				.build();

		assertThat(structure).hasExactColumns("person_id", "value", "lastname") //
				.hasId("person_id") //
				.hasStructure(td("person"));

	}

	@Test
	void simpleTableWithColumnsAddedInMultipleSteps() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>() //
				.addTable("person", td -> td.withId("person_id").withColumns("value").withColumns("lastname")) //
				.build();

		assertThat(structure).hasExactColumns("person_id", "value", "lastname") //
				.hasId("person_id") //
				.hasStructure(td("person"));

	}

	@Test
	void tableWithSingleChild() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parent-value", "parent-lastname"))
				.addChildTo("parent", "parent-child", "child", td -> td.withColumns("child-value", "child-lastname")) //
				.build();

		assertThat(structure).hasExactColumns( //
				"parent-value", "parent-lastname", //
				"child-value", "child-lastname", //
				fk("child", "parentId"), //
				coalesce("parentId", fk("child", "parentId")), //
				rn(fk("child", "parentId")), //
				coalesce(lit(1), rn(fk("child", "parentId"))) //
		).hasId("parentId") //
				.hasStructure(aj(av(td("parent")), av(td("child")), //
						eq("parentId", fk("child", "parentId")), // <-- should fail due to wrong column value
						eq(lit(1), rn(fk("child", "parentId"))) //
				));
	}

	@Test
	void columnsPassedThroughAreRepresentedAsDerivedColumn() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parent-value", "parent-lastname"))
				.addChildTo("parent","parent-child", "child", td -> td.withColumns("child-value", "child-lastname")) //
				.build();

		assertThat(structure.getSelect().getColumns()) //
				.allMatch(c -> //
				c instanceof AnalyticStructureBuilder.DerivedColumn //
						|| c instanceof AnalyticStructureBuilder.Coalesce //
						|| (c instanceof AnalyticStructureBuilder.BaseColumn bc && bc.column.toString().startsWith("parent"))
				);

	}

	@Test
	void tableWithSingleChildWithKey() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parentName", "parentLastname"))
				.addChildTo("parent", "parent-child","child", td -> td.withColumns("childName", "childLastname").withKeyColumn("childKey")) //
				.build();

		assertThat(structure) //
				.hasExactColumns("parentName", "parentLastname", //
						fk("child", "parentId"), //
						coalesce("parentId", fk("child", "parentId")), //
						rn(fk("child", "parentId")), //
						coalesce(lit(1), rn(fk("child", "parentId"))), //
						key("childKey"), "childName", "childLastname") //
				.hasId("parentId") //
				.hasStructure( //
						aj( //
								av(td("parent")), //
								av(td("child")), //
								eq("parentId", fk("child", "parentId")), //
								eq(lit(1), rn(fk("child", "parentId"))) //
						) //
				); //
	}

	@Test
	void tableWithMultipleChildren() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parentName", "parentLastname"))
				.addChildTo("parent", "parent-child1","child1", td -> td.withColumns("childName", "childLastname"))
				.addChildTo("parent", "parent-child2","child2", td -> td.withColumns("siblingName", "siblingLastName")) //
				.build();

		CoalescePattern<LiteralPattern> rnChild1 = coalesce(lit(1), rn(fk("child1", "parentId")));

		assertThat(structure) //
				.hasExactColumns("parentName", "parentLastname", //
						fk("child1", "parentId"), //
						coalesce("parentId", fk("child1", "parentId")), //
						rn(fk("child1", "parentId")), //
						rnChild1, //
						"childName", "childLastname", //

						fk("child2", "parentId"), //
						coalesce("parentId", fk("child2", "parentId")), //
						rn(fk("child2", "parentId")), //
						coalesce(rnChild1, rn(fk("child2", "parentId"))), //
						"siblingName", //
						"siblingLastName")
				.hasId("parentId") //
				.hasStructure( //
						aj( //
								aj(av(td("parent")), av(td("child1")), //
										eq("parentId", fk("child1", "parentId")), //
										eq(lit(1), rn(fk("child1", "parentId"))) //
								), //
								av(td("child2")), //
								eq("parentId", fk("child2", "parentId")), //
								eq(rnChild1, rn(fk("child2", "parentId"))) //
						));
	}

	@Nested
	class TableWithChainOfChildren {

		@Test
		void middleChildHasId() {

			AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny","granny-parent", "parent", td -> td.withId("parentId").withColumns("parentName"))
					.addChildTo("parent", "parent-child","child", td -> td.withColumns("childName")) //
					.build();

			assertThat(structure) //
					.hasExactColumns( //
							// child
							"childName", //
							fk("child", "parentId"), // join by FK
							rn(fk("child", "parentId")), // join by RN <-- not found, but really should be there

							// parent
							"parentName", //
							fk("parent", "grannyId"), // join

							// child + parent
							coalesce("parentId", fk("child", "parentId")), // completed parentId for joining with granny, only
																															// necessary for joining with further children?
							rn(maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId")))), // completed RN for joining with granny
							maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId"))),

							// granny table
							"grannyName", //
							// (parent + child) --> granny
							coalesce("grannyId", maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId")))), // completed
																																																											// grannyId
							coalesce(lit(1), rn(maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId"))))) // completed RN for granny.

					) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									av(td("granny")), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq("parentId", fk("child", "parentId")), //
											eq(lit(1), rn(fk("child", "parentId"))) //
									), //
									eq("grannyId", maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId")))), //
									eq(lit(1), rn(maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId"))))) // corrected
							) //
					);
		}

		/**
		 * middle children that don't have an id, nor a key can't referenced by further children
		 */
		@Test
		void middleChildHasNoId() {

			// assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
			AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "granny-parent","parent", td -> td.withColumns("parentName"))
					.addChildTo("parent", "parent-child","child", td -> td.withColumns("childName")) //
					.build();

			structure.getSelect();
		}

		@Test
		void middleChildWithKeyHasId() {

			AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "granny-parent","parent",
							td -> td.withId("parentId").withKeyColumn("parentKey").withColumns("parentName"))
					.addChildTo("parent", "parent-child","child", td -> td.withColumns("childName")) //
					.build();

			ForeignKeyPattern<String, String> fkChildToParent = fk("child", "parentId");
			CoalescePattern<String> idOfJoinParentWithChild = coalesce("parentId", fkChildToParent);
			ForeignKeyPattern<String, String> fkParentToGranny = fk("parent", "grannyId");
			MaxOverPattern<ForeignKeyPattern<String, String>> fkJoinParentWithChildToGranny = maxOver(fkParentToGranny,
					idOfJoinParentWithChild);
			CoalescePattern<LiteralPattern> rnJoinParentWithChild = coalesce(lit(1), rn(fkJoinParentWithChildToGranny)); // todo: should be ordered by key and rownumber

			assertThat(structure).hasExactColumns( //

							// child columns
							"childName", //
							fkChildToParent, //

							// parent
							key("parentKey"), "parentName", //
							fkParentToGranny, //

							// join of parent + child
							rn(fkChildToParent), // rownumber for the join itself. should be in the result because it is a single
							// valued indicator if a child is present in this row. Relevant when there are
							// siblings
							idOfJoinParentWithChild, // guarantees a parent id in all rows and may serve as a join
							// column for siblings of child.
							fkJoinParentWithChildToGranny, //

							// granny
							"grannyName", //
							// join of granny + (parent + child)
							coalesce("grannyId", fkJoinParentWithChildToGranny), // grannyId
							rnJoinParentWithChild, //
							rn(fkJoinParentWithChildToGranny)// TODO: order by does not get tested
					) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									av(td("granny")), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq("parentId", fkChildToParent), //
											eq(lit(1), rn(fkChildToParent)) //
									), //
									eq("grannyId", fkJoinParentWithChildToGranny), //
									eq(lit(1), rn(fkJoinParentWithChildToGranny)) //
							) //
					);
		}

		@Test
		void middleChildWithKeyHasNoId() {

			AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "granny-parent","parent", td -> td.withColumns("parentName").withKeyColumn("parentKey"))
					.addChildTo("parent", "parent-child","child", td -> td.withColumns("childName")) //
					.build();

			assertThat(structure).hasExactColumns( //
					"grannyName", //
					coalesce(lit(1), rn(maxOver(fk("parent", "grannyId"), coalesce(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
							coalesce(key("parentKey"), fk("child", key("parentKey")))))),
					coalesce("grannyId",
							maxOver(fk("parent", "grannyId"),
									coalesce(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
									coalesce(key("parentKey"), fk("child", key("parentKey"))))),
					maxOver(fk("parent", "grannyId"), coalesce(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
							coalesce(key("parentKey"), fk("child", key("parentKey")))),
					"parentName", //

					fk("child", fk("parent", "grannyId")), //
					coalesce(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))), //
					fk("child", key("parentKey")), //
					coalesce(key("parentKey"), fk("child", key("parentKey"))), //
					rn(fk("child", fk("parent", "grannyId")), fk("child", key("parentKey"))), //
					rn(maxOver(fk("parent", "grannyId"), coalesce(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
							coalesce(key("parentKey"), fk("child", key("parentKey"))))), //
					"childName" //
			) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									av(td("granny")), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))), //
											eq(key("parentKey"), fk("child", key("parentKey"))), //
											eq(lit(1), rn(fk("child", fk("parent", "grannyId")), fk("child", key("parentKey")))) //
									), //
									eq("grannyId",
											maxOver(fk("parent", "grannyId"),
													coalesce(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
													coalesce(key("parentKey"), fk("child", key("parentKey"))))), //
									eq(lit(1), rn(maxOver(fk("parent", "grannyId"), coalesce(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
											coalesce(key("parentKey"), fk("child", key("parentKey")))))) //
							) //
					);
		}

		@Test
		void middleSingleChildHasId() {

			AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addSingleChildTo("granny", "granny-parent","parent", td -> td.withId("parentId").withColumns("parentName"))
					.addChildTo("parent", "parent-child","child", td -> td.withColumns("childName")) //
					.build();

			assertThat(structure) //
					.hasExactColumns( //
							// child
							"childName", //
							fk("child", "parentId"), // join by FK
							rn(fk("child", "parentId")), // join by RN <-- not found, but really should be there

							// parent
							"parentName", //
							fk("parent", "grannyId"), // join

							// child + parent
							coalesce("parentId", fk("child", "parentId")), // completed parentId for joining with granny, only
							// necessary for joining with further children?
							rn(maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId")))), // completed RN for joining with granny
							maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId"))),

							// granny table
							"grannyName", //
							// (parent + child) --> granny
							coalesce("grannyId", maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId")))), // completed
							// grannyId
							coalesce(lit(1), rn(maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId"))))) // completed RN for granny.

					) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									av(td("granny")), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq("parentId", fk("child", "parentId")), //
											eq(lit(1), rn(fk("child", "parentId"))) //
									), //
									eq("grannyId", maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId")))), //
									eq(lit(1), rn(maxOver(fk("parent", "grannyId"), coalesce("parentId", fk("child", "parentId"))))) // corrected
							) //
					);
		}

		@Test
		void middleSingleChildHasNoId() {

			AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addSingleChildTo("granny", "granny-parent","parent", td -> td.withColumns("parentName"))
					.addChildTo("parent","parent-child", "child", td -> td.withColumns("childName")) //
					.build();

			ForeignKeyPattern<String, String> fkParentToGranny = fk("parent", "grannyId");
			ForeignKeyPattern<String, ForeignKeyPattern<String, String>> fkChildToGranny = fk("child", fkParentToGranny);
			assertThat(structure).hasExactColumns( //
					"grannyName", //
					rn(fkChildToGranny), //
					rn(maxOver(fkParentToGranny, coalesce(fkParentToGranny, fkChildToGranny))), //
					"parentName", //
					fkChildToGranny, //
					coalesce(fkParentToGranny, fkChildToGranny), //
					"childName", //
					maxOver(fkParentToGranny, coalesce(fkParentToGranny, fkChildToGranny)), //
					coalesce("grannyId", maxOver(fkParentToGranny, coalesce(fkParentToGranny, fkChildToGranny))), //
					coalesce(lit(1), rn(maxOver(fkParentToGranny, coalesce(fkParentToGranny, fkChildToGranny)))) //
			).hasId("grannyId") //
					.hasStructure( //
							aj( //
									av(td("granny")), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq(fkParentToGranny, fkChildToGranny), //
											eq(lit(1), rn(fkChildToGranny)) //
									), //
									eq("grannyId", maxOver(fkParentToGranny, coalesce(fkParentToGranny, fkChildToGranny))), //
									eq(lit(1), rn(maxOver(fkParentToGranny, coalesce(fkParentToGranny, fkChildToGranny)))) //
							) //
					);
		}

	}






}
