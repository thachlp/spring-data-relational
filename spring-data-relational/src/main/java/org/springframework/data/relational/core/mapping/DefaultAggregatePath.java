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

package org.springframework.data.relational.core.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents a path within an aggregate starting from the aggregate root.
 *
 * @since 3.2
 * @author Jens Schauder
 */
class DefaultAggregatePath implements AggregatePath {

	private final RelationalMappingContext context;

	@Nullable private final RelationalPersistentEntity<?> rootType;

	@Nullable private final PersistentPropertyPath<? extends RelationalPersistentProperty> path;

	private final List<AggregatePath> ancestorList;

	DefaultAggregatePath(RelationalMappingContext context,
			PersistentPropertyPath<? extends RelationalPersistentProperty> path) {

		Assert.notNull(context, "context must not be null");
		Assert.notNull(path, "path must not be null");

		this.context = context;
		this.path = path;
		this.rootType = null;

		ancestorList = new ArrayList<>(getLength());
		collectAncestors(ancestorList, this);
	}

	DefaultAggregatePath(RelationalMappingContext context, RelationalPersistentEntity<?> rootType) {

		Assert.notNull(context, "context must not be null");
		Assert.notNull(rootType, "rootType must not be null");

		this.context = context;
		this.rootType = rootType;
		this.path = null;

		ancestorList = new ArrayList<>(getLength());
		collectAncestors(ancestorList, this);
	}

	private static SqlIdentifier constructTableAlias(AggregatePath path) {

		String alias = path.stream() //
				.filter(p -> !p.isRoot()) //
				.map(p -> p.isEmbedded() //
						? p.getRequiredLeafProperty().getEmbeddedPrefix()//
						: p.getRequiredLeafProperty().getName() + (p == path ? "" : "_") //
				) //
				.collect(new ReverseJoinCollector());
		return SqlIdentifier.quoted(alias);
	}

	@Override
	public boolean isWritable() {
		return stream().allMatch(path -> path.isRoot() || path.getRequiredLeafProperty().isWritable());
	}

	public boolean isRoot() {
		return path == null;
	}

	/**
	 * Returns the path that has the same beginning but is one segment shorter than this path.
	 *
	 * @return the parent path. Guaranteed to be not {@literal null}.
	 * @throws IllegalStateException when called on an empty path.
	 */
	public AggregatePath getParentPath() {

		if (isRoot()) {
			throw new IllegalStateException("The parent path of a root path is not defined.");
		}

		if (path.getLength() == 1) {
			return context.getAggregatePath(path.getLeafProperty().getOwner());
		}

		return context.getAggregatePath(path.getParentPath());
	}

	/**
	 * The {@link RelationalPersistentEntity} associated with the leaf of this path.
	 *
	 * @return Might return {@literal null} when called on a path that does not represent an entity.
	 */
	@Nullable
	public RelationalPersistentEntity<?> getLeafEntity() {
		return isRoot() ? rootType : context.getPersistentEntity(path.getLeafProperty().getActualType());
	}

	/**
	 * The {@link RelationalPersistentEntity} associated with the leaf of this path or throw {@link IllegalStateException}
	 * if the leaf cannot be resolved.
	 *
	 * @return the required {@link RelationalPersistentEntity} associated with the leaf of this path.
	 * @throws IllegalStateException if the persistent entity cannot be resolved.
	 */
	public RelationalPersistentEntity<?> getRequiredLeafEntity() {

		RelationalPersistentEntity<?> entity = getLeafEntity();

		if (entity == null) {

			throw new IllegalStateException(
					String.format("Couldn't resolve leaf PersistentEntity for type %s", path.getLeafProperty().getActualType()));
		}

		return entity;
	}

	/**
	 * @return {@literal true} if this path represents an entity which has an Id attribute.
	 */
	public boolean hasIdProperty() {

		RelationalPersistentEntity<?> leafEntity = getLeafEntity();
		return leafEntity != null && leafEntity.hasIdProperty();
	}

	/**
	 * Returns the longest ancestor path that has an {@link org.springframework.data.annotation.Id} property.
	 *
	 * @return A path that starts just as this path but is shorter. Guaranteed to be not {@literal null}.
	 */
	public AggregatePath getIdDefiningParentPath() {

		Predicate<AggregatePath> idDefiningPathFilter = ap -> !ap.equals(this) && (ap.isRoot() || ap.hasIdProperty());
		return stream().filter(idDefiningPathFilter).findFirst().orElseThrow();

	}

	public RelationalPersistentProperty getRequiredIdProperty() {
		return isRoot() ? rootType.getRequiredIdProperty() : getRequiredLeafEntity().getRequiredIdProperty();

	}

	public int getLength() {
		return isRoot() ? 0 : path.getLength();
	}

	@Override
	public TableInfo getTableInfo() {

		AggregatePath tableOwner = getTableOwningAncestor();

		RelationalPersistentEntity<?> leafEntity = tableOwner.getRequiredLeafEntity();
		SqlIdentifier qualifiedTableName = leafEntity.getQualifiedTableName();

		SqlIdentifier tableAlias = tableOwner.isRoot() ? null : constructTableAlias(tableOwner);

		ColumnInfo reverseColumnInfo = tableOwner.isRoot() ? null
				: new ColumnInfo(tableOwner.getRequiredLeafProperty().getReverseColumnName(tableOwner),
						prefixWithTableAlias(tableOwner.getRequiredLeafProperty().getReverseColumnName(tableOwner)));

		ColumnInfo qualifierColumnInfo = null;
		if (!isRoot()) {
			SqlIdentifier keyColumn = path.getLeafProperty().getKeyColumn();
			if (keyColumn != null) {
				qualifierColumnInfo = new ColumnInfo(keyColumn, keyColumn);
			}
		}

		Class<?> qualifierColumnType = null;
		if (!isRoot() && path.getLeafProperty().isQualified()) {
			qualifierColumnType = path.getLeafProperty().getQualifierColumnType();
		}

		SqlIdentifier idColumnName = leafEntity.hasIdProperty() ? leafEntity.getIdColumn() : null;

		SqlIdentifier effectiveIdColumnName = tableOwner.isRoot() ? idColumnName : reverseColumnInfo.name();

		return new TableInfo(qualifiedTableName, tableAlias, reverseColumnInfo, qualifierColumnInfo, qualifierColumnType,
				idColumnName, effectiveIdColumnName);
	}

	@Override
	public ColumnInfo getColumnInfo() {

		Assert.state(!isRoot(), "Path is null");

		SqlIdentifier name = assembleColumnName(path.getLeafProperty().getColumnName());
		return new ColumnInfo(name, prefixWithTableAlias(name));
	}

	private SqlIdentifier assembleColumnName(SqlIdentifier suffix) {
		return suffix.transform(constructEmbeddedPrefix()::concat);
	}

	private String constructEmbeddedPrefix() {

		return stream() //
				.filter(p -> p != this) //
				.takeWhile(p -> p.isEmbedded()).map(p -> p.getRequiredLeafProperty().getEmbeddedPrefix()) //
				.collect(new ReverseJoinCollector());
	}

	/**
	 * Returns {@literal true} exactly when the path is non empty and the leaf property an embedded one.
	 *
	 * @return if the leaf property is embedded.
	 */
	public boolean isEmbedded() {
		return !isRoot() && path.getLeafProperty().isEmbedded();
	}

	/**
	 * @return {@literal true} when this is an empty path or the path references an entity.
	 */
	public boolean isEntity() {
		return isRoot() || path.getLeafProperty().isEntity();
	}

	/**
	 * Finds and returns the longest path with ich identical or an ancestor to the current path and maps directly to a
	 * table.
	 *
	 * @return a path. Guaranteed to be not {@literal null}.
	 */
	public AggregatePath getTableOwningAncestor() {

		return stream().filter(ap -> ap.isEntity() && !ap.isEmbedded()).findFirst().orElseThrow();

	}

	@Override
	public String toString() {
		return "AggregatePath["
				+ (rootType == null ? path.getBaseProperty().getOwner().getType().getName() : rootType.getName()) + "]"
				+ ((isRoot()) ? "/" : path.toDotPath());
	}

	private SqlIdentifier prefixWithTableAlias(SqlIdentifier columnName) {

		AggregatePath tableOwner = getTableOwningAncestor();
		SqlIdentifier tableAlias = tableOwner.isRoot() ? null : constructTableAlias(tableOwner);

		return tableAlias == null ? columnName : columnName.transform(name -> tableAlias.getReference() + "_" + name);
	}

	public String toDotPath() {
		return isRoot() ? "" : path.toDotPath();
	}

	/**
	 * Returns {@literal true} if there are multiple values for this path, i.e. if the path contains at least one element
	 * that is a collection and array or a map.
	 *
	 * @return {@literal true} if the path contains a multivalued element.
	 */
	public boolean isMultiValued() {

		return !isRoot() && //
				(path.getLeafProperty().isCollectionLike() //
						|| path.getLeafProperty().isQualified() //
						|| getParentPath().isMultiValued() //
				);
	}

	/**
	 * @return {@literal true} if the leaf property of this path is a {@link java.util.Map}.
	 * @see RelationalPersistentProperty#isMap()
	 */
	public boolean isMap() {
		return !isRoot() && path.getLeafProperty().isMap();
	}

	/**
	 * @return {@literal true} when this is references a {@link java.util.List} or {@link java.util.Map}.
	 */
	public boolean isQualified() {
		return !isRoot() && path.getLeafProperty().isQualified();
	}

	public RelationalPersistentProperty getRequiredLeafProperty() {

		if (isRoot()) {
			throw new IllegalStateException("root path does not have a leaf property");
		}
		return path.getLeafProperty();
	}

	public RelationalPersistentProperty getBaseProperty() {

		if (isRoot()) {
			throw new IllegalStateException("root path does not have a base property");
		}
		return path.getBaseProperty();
	}

	/**
	 * @return {@literal true} when this is references a {@link java.util.Collection} or an array.
	 */
	public boolean isCollectionLike() {
		return !isRoot() && path.getLeafProperty().isCollectionLike();
	}

	/**
	 * @return whether the leaf end of the path is ordered, i.e. the data to populate must be ordered.
	 * @see RelationalPersistentProperty#isOrdered()
	 */
	public boolean isOrdered() {
		return !isRoot() && path.getLeafProperty().isOrdered();
	}

	/**
	 * Creates a new path by extending the current path by the property passed as an argument.
	 *
	 * @param property must not be {@literal null}.
	 * @return Guaranteed to be not {@literal null}.
	 */
	public AggregatePath append(RelationalPersistentProperty property) {

		PersistentPropertyPath<? extends RelationalPersistentProperty> newPath = isRoot() //
				? context.getPersistentPropertyPath(property.getName(), rootType.getType()) //
				: context.getPersistentPropertyPath(path.toDotPath() + "." + property.getName(),
						path.getBaseProperty().getOwner().getType());

		return context.getAggregatePath(newPath);
	}

	public PersistentPropertyPath<? extends RelationalPersistentProperty> getRequiredPersistentPropertyPath() {

		Assert.state(!isRoot(), "path must not be null");
		return path;
	}

	PersistentPropertyPathExtension getPathExtension() {

		if (isRoot()) {
			return new PersistentPropertyPathExtension(context, rootType);
		}
		return new PersistentPropertyPathExtension(context, path);
	}

	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		DefaultAggregatePath that = (DefaultAggregatePath) o;
		return Objects.equals(context, that.context) && Objects.equals(rootType, that.rootType)
				&& Objects.equals(path, that.path);
	}

	@Override
	public int hashCode() {

		return Objects.hash(context, rootType, path);
	}

	/**
	 * creates an {@link Iterator} that iterates over the current path and all ancestors. It will start with the current
	 * path, followed by its parent and so one until ending with the root.
	 */
	@Override
	public Iterator<AggregatePath> iterator() {
		return ancestorList.iterator();
	}

	private static void collectAncestors(List<AggregatePath> ancestorList, AggregatePath current) {

		ancestorList.add(current);
		if (!current.isRoot()) {
			collectAncestors(ancestorList, current.getParentPath());
		}
	}

	private static class ReverseJoinCollector implements Collector<String, StringBuilder, String> {
		@Override
		public Supplier<StringBuilder> supplier() {
			return () -> new StringBuilder();
		}

		@Override
		public BiConsumer<StringBuilder, String> accumulator() {
			return ((stringBuilder, s) -> stringBuilder.insert(0, s));
		}

		@Override
		public BinaryOperator<StringBuilder> combiner() {
			return (a, b) -> b.append(a);
		}

		@Override
		public Function<StringBuilder, String> finisher() {
			return StringBuilder::toString;
		}

		@Override
		public Set<Characteristics> characteristics() {
			return Collections.emptySet();
		}
	}
}
