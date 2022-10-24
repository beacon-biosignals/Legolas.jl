# Schema-Related Concepts/Conventions

!!! note

    If you're a newcomer to Legolas.jl, please familiarize yourself with the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/main/examples/tour.jl) before diving into this documentation.

## [Schema Version Identifiers](@id schema_version_identifier_specification)

Legolas defines "schema version identifiers" as strings of the form:

- `name@version` where:
    - `name` is a lowercase alphanumeric string and may include the special characters `.` and `-`.
    - `version` is a non-negative integer.
- or, `x>y` where `x` and `y` are valid schema version identifiers and `>` denotes "extends from".

A schema version identifier is said to be *fully qualified* if it includes the identifiers of all known ancestors of the particular schema version that it directly identifies.

Schema authors should follow the below conventions when choosing the name of a new schema:

1. Include a namespace. For example, assuming the schema is defined in a package Foo.jl, `foo.automobile` is good, `automobile` is bad.
2. Prefer singular over plural. For example, `foo.automobile` is good, `foo.automobiles` is bad.
3. Don't "overqualify" the schema name with ancestor-derived information. For example, `bar.automobile@1>foo.automobile@1` is good, `baz.supercar@1>bar.automobile@1` is good, `bar.foo.automobile@1>foo.automobile@1` is bad, `baz.automobile.supercar@1>bar.automobile@1` is bad.

## Schema Versioning: You Break It, You Bump It

While it is fairly established practice to [semantically version source code](https://semver.org/), the world of data/artifact versioning is a bit more varied. As presented in the tour, each `Legolas.SchemaVersion` carries a single version integer. The central rule that governs Legolas' schema versioning approach is:

**Do not introduce a change to an existing schema version that might cause existing compliant data to become non-compliant; instead, incorporate the intended change in a new schema version whose version number is one greater than the previous version number.**

For example, a schema author must introduce a new schema version for any of the following changes:

- A new type-restricted required field is added to the schema.
- An existing required field's type restriction is tightened.
- An existing required field is renamed.

One benefit of Legolas' approach is that multiple schema versions may be defined in the same codebase, e.g. there's nothing that prevents `@version("my-schema@1", ...)` and `@version("my-schema@2", ...)` from being defined and utilized simultaneously. The source code that defines any given Legolas schema version and/or consumes/produces Legolas tables is presumably already semantically versioned, such that consumer/producer packages can determine their compatibility with each other in the usual manner via interpreting major/minor/patch increments.

## Important Expectations Regarding Custom Field Assignments

Schema authors should ensure that their `@version` declarations meet two important expectations so that generated record types behaves as intended:

1. Custom field assignments should preserve the [idempotency](https://en.wikipedia.org/wiki/Idempotence) of record type constructors.
2. Custom field assignments should not observe mutable non-local state.

Thus, given a Legolas-generated record type `R`, the following should hold for all valid values of `fields`:

```jl
R(R(fields)) == R(fields)
R(fields) == R(fields)
```

## How to Avoid Breaking Schema Changes

It is preferable to avoid introducing a new version of an existing schema whenever possible to avoid code/data churn for consumers. Following the below guidelines should help make breaking changes less likely:

1. Prefer conservative field type restrictions from the get-go, to avoid needing to tighten them later.
2. Handle/enforce "potential deprecation paths" in a required field's RHS definition when possible. For example, imagine a schema that contains a required field `id::Union{UUID,String}` where `id` is either a `UUID`, or a `String` that may be parsed as a `UUID`. Now, let's imagine we decided we wanted to update the schema such that new tables ALWAYS normalize `id` to a proper `UUID`. In this case, it is preferable to simply update this required field to `id::Union{UUID,String} = UUID(id)` instead of `id::UUID`. The latter is a breaking change that requires introducing a new schema version, while the former achieves the same practical result without breaking consumers of old data. TODO update this guidance in light of `Legolas.accepted_field_type`.
