# Schema-Related Concepts/Conventions

!!! note

    If you're a newcomer to Legolas.jl, please familiarize yourself with the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/main/examples/tour.jl) before diving into this documentation.

## [Schema Version Identifiers](@id schema_version_identifier_specification)

Legolas defines "schema version identifiers" as strings of the form:

- `name@version` where:
    - `name` is a lowercase alphanumeric string and may include the special characters `.` and `-`.
    - `version` is a non-negative integer.
- or, `x>y` where `x` and `y` are valid schema version identifiers and `>` denotes "extends from".

A schema version identifier is said to be *fully qualified* if it includes the identifiers of all ancestors of the particular schema version that it directly identifies.

Schema authors should follow the below conventions when choosing the name of a new schema:

1. Include a namespace. For example, assuming the schema is defined in a package Foo.jl, `foo.automobile` is good, `automobile` is bad.
2. Prefer singular over plural. For example, `foo.automobile` is good, `foo.automobiles` is bad.
3. Don't "overqualify" a schema name with ancestor-derived information that is better captured by the fully qualified identifier of a specific schema version. For example, `bar.automobile` should be preferred over `bar.foo.automobile`, since `bar.automobile@1>foo.automobile@1` is preferable to `bar.foo.automobile@1>foo.automobile@1`. Similarly, `baz.supercar` should be preferred over `baz.automobile.supercar`, since `baz.supercar@1>bar.automobile@1` is preferable to `baz.automobile.supercar@1>bar.automobile@1`.

## Schema Versioning: You Break It, You Bump It

While it is fairly established practice to [semantically version source code](https://semver.org/), the world of data/artifact versioning is a bit more varied. As presented in the tour, each `Legolas.SchemaVersion` carries a single version integer. The central rule that governs Legolas' schema versioning approach is:

**Do not introduce a change to an existing schema version that might cause existing compliant data to become non-compliant; instead, incorporate the intended change in a new schema version whose version number is one greater than the previous version number.**

A schema author must introduce a new schema version if any of the following changes are introduced:

- A new type-constrained and/or value-constrained field is declared. In other words, for the introduction of a new declared field to be non-breaking, the new field's type constraint must be `::Any` and it may not feature a value-constraining or value-transforming assignment expression.
- An existing declared field's type or value constraints are tightened.
- An existing declared field is renamed.

If any of the above breaking changes are made to an existing schema version, instead of introducing a new schema version, subtle downstream breakage may occur. For example, if a new type/value-constrained field is declared, previously compliant tables containing a field with the same name might accidentally become non-compliant if existing values violate the new constraints. Similarly, downstream schema version extensions may have already declared a field with the same name, but with constraints that are incompatible with the new constraints.

One benefit of Legolas' approach is that multiple schema versions may be defined in the same codebase, e.g. there's nothing that prevents `@version(FooV1, ...)` and `@version(FooV2, ...)` from being defined and utilized simultaneously. The source code that defines any given Legolas schema version and/or consumes/produces Legolas tables is presumably already semantically versioned, such that consumer/producer packages can determine their compatibility with each other in the usual manner via interpreting major/minor/patch increments.

Note that it is preferable to avoid introducing new versions of an existing schema, if possible, in order to minimize code/data churn for downstream producers/consumers. Thus, authors should prefer conservative field type restrictions from the get-go. Remember: loosening a field type restriction is not a breaking change, but tightening one is.

## Important Expectations Regarding Custom Field Assignments

Schema authors should ensure that their `@version` declarations meet two important expectations so that generated record types behaves as intended:

1. Custom field assignments should preserve the [idempotency](https://en.wikipedia.org/wiki/Idempotence) of record type constructors.
2. Custom field assignments should not observe mutable non-local state.

Thus, given a Legolas-generated record type `R`, the following should hold for all valid values of `fields`:

```jl
R(R(fields)) == R(fields)
R(fields) == R(fields)
```
