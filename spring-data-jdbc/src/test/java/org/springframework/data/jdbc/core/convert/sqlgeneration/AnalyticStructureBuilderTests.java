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
import static org.springframework.data.jdbc.core.convert.sqlgeneration.GreatestPattern.*;
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
				greatest("parentId", fk("child", "parentId")), //
				rn(fk("child", "parentId")), //
				greatest(lit(1), rn(fk("child", "parentId"))) //
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
						|| c instanceof AnalyticStructureBuilder.Greatest //
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
						greatest("parentId", fk("child", "parentId")), //
						rn(fk("child", "parentId")), //
						greatest(lit(1), rn(fk("child", "parentId"))), //
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

		GreatestPattern<LiteralPattern> rnChild1 = greatest(lit(1), rn(fk("child1", "parentId")));

		assertThat(structure) //
				.hasExactColumns("parentName", "parentLastname", //
						fk("child1", "parentId"), //
						greatest("parentId", fk("child1", "parentId")), //
						rn(fk("child1", "parentId")), //
						rnChild1, //
						"childName", "childLastname", //

						fk("child2", "parentId"), //
						greatest("parentId", fk("child2", "parentId")), //
						rn(fk("child2", "parentId")), //
						greatest(rnChild1, rn(fk("child2", "parentId"))), //
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
							greatest("parentId", fk("child", "parentId")), // completed parentId for joining with granny, only
																															// necessary for joining with further children?
							greatest(lit(1), rn(fk("child", "parentId"))), // completed RN for joining with granny
							maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId"))),

							// granny table
							"grannyName", //
							// (parent + child) --> granny
							greatest("grannyId", maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId")))), // completed
																																																											// grannyId
							greatest(lit(1), greatest(lit(1), rn(fk("child", "parentId")))) // completed RN for granny.

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
									eq("grannyId", maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId")))), //
									eq(lit(1), greatest(lit(1), rn(fk("child", "parentId")))) // corrected
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
			GreatestPattern<String> idOfJoinParentWithChild = greatest("parentId", fkChildToParent);
			ForeignKeyPattern<String, String> fkParentToGranny = fk("parent", "grannyId");
			GreatestPattern<LiteralPattern> rnJoinParentWithChild = greatest(lit(1), rn(fkParentToGranny));
			MaxOverPattern<ForeignKeyPattern<String, String>> fkJoinParentWithChildToGranny = maxOver(fkParentToGranny,
					idOfJoinParentWithChild);

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
					greatest("grannyId", fkJoinParentWithChildToGranny), // grannyId
					// for every column
					rnJoinParentWithChild, //
					greatest(lit(1), greatest(lit(1), rn(fkChildToParent))) //
			) //
					.hasId("grannyId") //
					.hasStructure( //
							aj( //
									td("granny"), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq("parentId", fkChildToParent), //
											eq(lit(1), rn(fkChildToParent)) //
									), //
									eq("grannyId", fkJoinParentWithChildToGranny), //
									eq(lit(1), rnJoinParentWithChild) //
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
					greatest(lit(1), greatest(lit(1), rn(fk("child", fk("parent", "grannyId")), fk("child", key("parentKey"))))),
					greatest("grannyId",
							maxOver(fk("parent", "grannyId"),
									greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
									greatest(key("parentKey"), fk("child", key("parentKey"))))),
					maxOver(fk("parent", "grannyId"), greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
							greatest(key("parentKey"), fk("child", key("parentKey")))),
					"parentName", //

					fk("child", fk("parent", "grannyId")), //
					greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))), //
					fk("child", key("parentKey")), //
					greatest(key("parentKey"), fk("child", key("parentKey"))), //
					rn(fk("child", fk("parent", "grannyId")), fk("child", key("parentKey"))), //
					greatest(lit(1), rn(fk("child", fk("parent", "grannyId")), fk("child", key("parentKey")))), //
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
													greatest(fk("parent", "grannyId"), fk("child", fk("parent", "grannyId"))),
													greatest(key("parentKey"), fk("child", key("parentKey"))))), //
									eq(lit(1), greatest(lit(1), rn(fk("child", fk("parent", "grannyId")), fk("child", key("parentKey"))))) //
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
							greatest("parentId", fk("child", "parentId")), // completed parentId for joining with granny, only
							// necessary for joining with further children?
							greatest(lit(1), rn(fk("child", "parentId"))), // completed RN for joining with granny
							maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId"))),

							// granny table
							"grannyName", //
							// (parent + child) --> granny
							greatest("grannyId", maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId")))), // completed
							// grannyId
							greatest(lit(1), greatest(lit(1), rn(fk("child", "parentId")))) // completed RN for granny.

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
									eq("grannyId", maxOver(fk("parent", "grannyId"), greatest("parentId", fk("child", "parentId")))), //
									eq(lit(1), greatest(lit(1), rn(fk("child", "parentId")))) // corrected
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
					rn(fkParentToGranny), //
					greatest(lit(1), rn(fkParentToGranny)), //
					"parentName", //
					fkChildToGranny, //
					greatest(fkParentToGranny, fkChildToGranny), //
					"childName", //
					maxOver(fkParentToGranny, greatest(fkParentToGranny, fkChildToGranny)), //
					greatest("grannyId", maxOver(fkParentToGranny, greatest(fkParentToGranny, fkChildToGranny))), //
					greatest(lit(1), greatest(lit(1), rn(fkChildToGranny))) //
			).hasId("grannyId") //
					.hasStructure( //
							aj( //
									td("granny"), //
									aj( //
											td("parent"), //
											av(td("child")), //
											eq(fkParentToGranny, fkChildToGranny), //
											eq(lit(1), rn(fkChildToGranny)) //
									), //
									eq("grannyId", maxOver(fkParentToGranny, greatest(fkParentToGranny, fkChildToGranny))), //
									eq(lit(1), greatest(lit(1), rn(fkChildToGranny))) //
							) //
					);
		}

	}

	@Test
	void mediumComplexHierarchy() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>() //
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName")) //
				.addChildTo("customer","customer-address", "address", td -> td.withId("addressId").withColumns("addressName")) //
				.addChildTo("address", "address-city","city", td -> td.withColumns("cityName")) //
				.addChildTo("customer", "customer-order","order", td -> td.withId("orderId").withColumns("orderName")) //
				.addChildTo("address", "address-type", "type", td -> td.withColumns("typeName")) //
				.build();

		ForeignKeyPattern<String, String> fkCityToAddress = fk("city", "addressId");
		GreatestPattern<String> idJoinCityAndAddress = greatest("addressId", fkCityToAddress);
		ForeignKeyPattern<String, String> fkAddressToCustomer = fk("address", "customerId");
		MaxOverPattern<ForeignKeyPattern<String, String>> fkJoinCityAndAddressToCustomer = maxOver(fkAddressToCustomer,
				idJoinCityAndAddress);
		ForeignKeyPattern<String, String> fkTypeToAddress = fk("type", "addressId");
		GreatestPattern<String> idJoinTypeAndAddress = greatest("addressId", fkTypeToAddress);
		MaxOverPattern<MaxOverPattern<ForeignKeyPattern<String, String>>> fkJoinCityAndAddressAndTypeToCustomer = maxOver(
				fkJoinCityAndAddressToCustomer, idJoinTypeAndAddress);
		GreatestPattern<LiteralPattern> rnJoinAddressToCustomerr = greatest(lit(1), rn(fkAddressToCustomer));
		ForeignKeyPattern<String, String> fkOrderToCustomer = fk("order", "customerId");
		GreatestPattern<String> idJoinOrderAndCustomer = greatest("customerId", fkOrderToCustomer);
		GreatestPattern<GreatestPattern<LiteralPattern>> rnJoinAddressAndCustomerAndOrder = greatest(
				rnJoinAddressToCustomerr, rn(fkOrderToCustomer));
		GreatestPattern<String> idJoinCustomerAndCityAndAddressAndType = greatest("customerId",
				fkJoinCityAndAddressAndTypeToCustomer);
		RowNumberPattern rnCity = rn(fkCityToAddress);
		RowNumberPattern rnType = rn(fkTypeToAddress);

		assertThat(structure).hasExactColumns( //
				"customerName", //
				fkAddressToCustomer, //
				rn(fkAddressToCustomer), //
				fkJoinCityAndAddressToCustomer, //
				fkJoinCityAndAddressAndTypeToCustomer, //
				rnJoinAddressToCustomerr, //
				"addressName", //

				fkCityToAddress, //
				idJoinCityAndAddress, //
				rnCity, //
				"cityName", //

				fkOrderToCustomer, //
				idJoinOrderAndCustomer, //
				rn(fkOrderToCustomer), //
				rnJoinAddressAndCustomerAndOrder, //
				"orderId", "orderName", //

				fkTypeToAddress, //
				idJoinTypeAndAddress, //
				"typeName", //
				idJoinCustomerAndCityAndAddressAndType, greatest(lit(1), greatest(greatest(lit(1), rnCity), rnType)),
				greatest(greatest(lit(1), greatest(greatest(lit(1), rnCity), rnType)), rn(fkOrderToCustomer)))
				.hasId("customerId") //
				.hasStructure( //
						aj( //
								aj( //
										td("customer"), //
										aj( //
												aj( //
														td("address"), //
														av(td("city")), //
														eq("addressId", fkCityToAddress), //
														eq(lit(1), rnCity) //
												), //
												av(td("type")), //
												eq("addressId", fkTypeToAddress), //
												eq(greatest(lit(1), rnCity), rnType) //
										), //
										eq("customerId", fkJoinCityAndAddressAndTypeToCustomer), //
										eq(lit(1), greatest(greatest(lit(1), rnCity), rnType)) //
								), //
								av(td("order")), //
								eq("customerId", fkOrderToCustomer), //
								eq(greatest(lit(1), greatest(greatest(lit(1), rnCity), rnType)), rn(fkOrderToCustomer)) //
						) //
				);

	}

	@Test
	void mediumComplexHierarchy2() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>() //
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName")) //
				.addChildTo("customer","customer-keyAccount", "keyAccount", td -> td.withId("keyAccountId").withColumns("keyAccountName")) //
				.addChildTo("keyAccount","keyAccount-assistant", "assistant", td -> td.withColumns("assistantName")) //
				.addChildTo("customer", "customer-order","order", td -> td.withId("orderId").withColumns("orderName")) //
				.addChildTo("keyAccount","keyAccount-office", "office", td -> td.withColumns("officeName")) //
				.addChildTo("order", "oder-item","item", td -> td.withId("itemId").withColumns("itemName")) //
				.build();

		ForeignKeyPattern<String, String> fkOfficeToKeyAccount = fk("office", "keyAccountId");
		ForeignKeyPattern<String, String> fkAssistantToKeyAccount = fk("assistant", "keyAccountId");
		RowNumberPattern rnOffice = rn(fkOfficeToKeyAccount);
		RowNumberPattern rnAssistant = rn(fkAssistantToKeyAccount);
		ForeignKeyPattern<String, String> fkKeyAccountToCustomerId = fk("keyAccount", "customerId");
		GreatestPattern<String> idJoinAssistantAndKeyAccount = greatest("keyAccountId", fkAssistantToKeyAccount);
		GreatestPattern<String> idJoinOfficeAndKeyAccount = greatest("keyAccountId", fkOfficeToKeyAccount);
		MaxOverPattern<ForeignKeyPattern<String, String>> fkJoinAssistantAndKeyAccountToCustomer = maxOver(
				fkKeyAccountToCustomerId, idJoinAssistantAndKeyAccount);
		MaxOverPattern<MaxOverPattern<ForeignKeyPattern<String, String>>> fkJoinOfficeAndAssistantAndKeyAccountToCustomer = maxOver(
				fkJoinAssistantAndKeyAccountToCustomer, idJoinOfficeAndKeyAccount);
		assertThat(structure).hasStructure( //
				aj( //
						aj( //
								av(td("customer")), //
								aj( //
										aj( //
												td("keyAccount"), //
												av(td("assistant")), //
												eq("keyAccountId", fkAssistantToKeyAccount), //
												eq(lit(1), rnAssistant) //
										), //
										av(td("office")), //
										eq("keyAccountId", fkOfficeToKeyAccount), //
										eq(greatest(lit(1), rnAssistant), rnOffice) //
								), //
								eq("customerId", fkJoinOfficeAndAssistantAndKeyAccountToCustomer), //
								eq(lit(1), greatest(greatest(lit(1), rnAssistant), rnOffice)) //
						), //
						aj( //
								td("order"), //
								av(td("item")), //
								eq("orderId", fk("item", "orderId")), //
								eq(lit(1), rn(fk("item", "orderId"))) //
						), //
						eq("customerId", maxOver(fk("order", "customerId"), greatest("orderId", fk("item", "orderId")))), //
						eq(greatest(lit(1), greatest(greatest(lit(1), rnAssistant), rnOffice)),
								greatest(lit(1), rn(fk("item", "orderId")))) //
				) //
		);

	}

	@Test
	void complexHierarchy() {

		AnalyticStructure<String, String> structure = new AnalyticStructureBuilder<String, String>() //
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName")) //
				.addChildTo("customer", "customer-keyAccount","keyAccount", td -> td.withId("keyAccountId").withColumns("keyAccountName")) //
				.addChildTo("keyAccount", "keyAccount-assistant","assistant", td -> td.withColumns("assistantName")) //
				.addChildTo("customer", "customer-order","order", td -> td.withId("orderId").withColumns("orderName")) //
				.addChildTo("keyAccount", "keyAccount-office","office", td -> td.withId("officeId").withColumns("officeName")) //
				.addChildTo("order", "order-item", "item", td -> td.withColumns("itemName")) //
				.addChildTo("order", "order-shipment","shipment", td -> td.withColumns("shipmentName")) //
				.addChildTo("office", "office-room", "room", td -> td.withColumns("roomNumber")) //
				.build();

		GreatestPattern<LiteralPattern> rnKeyAccount = greatest(lit(1), rn(fk("keyAccount", "customerId")));
		ForeignKeyPattern<String, String> fkAssistantToKeyAccount = fk("assistant", "keyAccountId");
		GreatestPattern<LiteralPattern> rnAssistant = greatest(lit(1), rn(fkAssistantToKeyAccount));
		ForeignKeyPattern<String, String> fkItemToOrder = fk("item", "orderId");
		GreatestPattern<LiteralPattern> rnItem = greatest(lit(1), rn(fkItemToOrder));

		ForeignKeyPattern<String, String> fkRoomToOffice = fk("room", "officeId");
		GreatestPattern<String> idJoinRoomAndOffice = greatest("officeId", fkRoomToOffice);
		MaxOverPattern<ForeignKeyPattern<String, String>> fkJoinRommAndOfficeToKeyAccount = maxOver(
				fk("office", "keyAccountId"), idJoinRoomAndOffice);
		RowNumberPattern rnRoom = rn(fkRoomToOffice);
		assertThat(structure).hasExactColumns( //
				"customerName", //
				fk("keyAccount", "customerId"), //
				rn(fk("keyAccount", "customerId")), //
				rnKeyAccount, //
				"keyAccountName", //

				fkAssistantToKeyAccount, //
				rn(fkAssistantToKeyAccount), //
				rnAssistant, //
				"assistantName", //

				fk("office", "keyAccountId"), //
				rn(fk("office", "keyAccountId")), //
				greatest(rnAssistant, rn(fk("office", "keyAccountId"))), //
				"officeName", //

				fk("order", "customerId"), //
				rn(fk("order", "customerId")), //
				"orderName", //

				fkItemToOrder, //
				greatest("orderId", fkItemToOrder), //
				rnItem, //
				"itemName", //

				fk("shipment", "orderId"), //
				greatest("orderId", fk("shipment", "orderId")), //
				"shipmentName", //

				fkRoomToOffice, //
				idJoinRoomAndOffice, //
				"roomNumber", //

				greatest("keyAccountId", fkAssistantToKeyAccount), //
				maxOver(fk("keyAccount", "customerId"), greatest("keyAccountId", fkAssistantToKeyAccount)), //
				fkJoinRommAndOfficeToKeyAccount, //
				greatest("keyAccountId", fkJoinRommAndOfficeToKeyAccount), //
				maxOver(maxOver(fk("keyAccount", "customerId"), greatest("keyAccountId", fkAssistantToKeyAccount)),
						greatest("keyAccountId", fkJoinRommAndOfficeToKeyAccount)), //
				greatest(greatest(lit(1), rn(fkAssistantToKeyAccount)), greatest(lit(1), rnRoom)), //
				greatest("customerId",
						maxOver(maxOver(fk("keyAccount", "customerId"), greatest("keyAccountId", fkAssistantToKeyAccount)),
								greatest("keyAccountId", fkJoinRommAndOfficeToKeyAccount))), //
				greatest(lit(1), greatest(greatest(lit(1), rn(fkAssistantToKeyAccount)), greatest(lit(1), rnRoom))), //
				maxOver(fk("order", "customerId"), greatest("orderId", fkItemToOrder)), //
				maxOver(maxOver(fk("order", "customerId"), greatest("orderId", fkItemToOrder)),
						greatest("orderId", fk("shipment", "orderId"))), //
				greatest("customerId",
						maxOver(maxOver(fk("order", "customerId"), greatest("orderId", fkItemToOrder)),
								greatest("orderId", fk("shipment", "orderId")))), //
				greatest(greatest(lit(1), greatest(greatest(lit(1), rn(fkAssistantToKeyAccount)), greatest(lit(1), rnRoom))),
						greatest(greatest(lit(1), rn(fkItemToOrder)), rn(fk("shipment", "orderId")))) //

		).hasId("customerId") //
				.hasStructure( //
						aj( //
								aj( //
										td("customer"), //
										aj( //
												aj( //
														td("keyAccount"), //
														av(td("assistant")), //
														eq("keyAccountId", fkAssistantToKeyAccount), //
														eq(lit(1), rn(fkAssistantToKeyAccount)) //
												), //
												aj( //
														td("office"), //
														av(td("room")), //
														eq("officeId", fkRoomToOffice), //
														eq(lit(1), rnRoom) //
												), //
												eq("keyAccountId", fkJoinRommAndOfficeToKeyAccount), //
												eq(greatest(lit(1), rn(fkAssistantToKeyAccount)), greatest(lit(1), rnRoom)) //
										), //
										eq("customerId",
												maxOver(
														maxOver(fk("keyAccount", "customerId"), greatest("keyAccountId", fkAssistantToKeyAccount)),
														greatest("keyAccountId", fkJoinRommAndOfficeToKeyAccount))), //
										eq(lit(1), greatest(greatest(lit(1), rn(fkAssistantToKeyAccount)), greatest(lit(1), rnRoom))) //
								), //
								aj( //
										aj( //
												td("order"), //
												av(td("item")), //
												eq("orderId", fkItemToOrder), //
												eq(lit(1), rn(fkItemToOrder)) //
										), //
										av(td("shipment")), //
										eq("orderId", fk("shipment", "orderId")), //
										eq(greatest(lit(1), rn(fkItemToOrder)), rn(fk("shipment", "orderId"))) //
								), //
								eq("customerId",
										maxOver(maxOver(fk("order", "customerId"), greatest("orderId", fkItemToOrder)),
												greatest("orderId", fk("shipment", "orderId")))), //
								eq(greatest(lit(1), greatest(greatest(lit(1), rn(fkAssistantToKeyAccount)), greatest(lit(1), rnRoom))),
										greatest(greatest(lit(1), rn(fkItemToOrder)), rn(fk("shipment", "orderId")))) //
						) //
				);

	}
}
