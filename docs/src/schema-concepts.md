# Schema-Related Concepts/Conventions

!!! note

    If you're a newcomer to Legolas.jl, please familiarize yourself with the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/main/examples/tour.jl) before diving into this documentation.

## Schema Identifiers

Legolas defines "schema identifiers" as strings of the form:

- `name@version` where:
    - `name` is a lowercase alphanumeric string and may include the special characters `.` and `-`.
    - `version` is a non-negative integer.
- or, `x>y` where `x` and `y` are valid schema identifiers and `>` denotes "extends from".

A schema identifier is said to be *fully qualified* if it includes the identifiers of all known ancestors of the particular schema that it directly identifies.

Schema authors should follow the below conventions when choosing the `name` part of a new schema's identifier:

1. Include a namespace. For example, assuming the schema is defined in a package Foo.jl, `foo.automobile` is good, `automobile` is bad.
2. Prefer singular over plural. For example, `foo.automobile` is good, `foo.automobiles` is bad.
3. Don't "overqualify" the schema name with ancestor-derived information. For example, `bar.automobile@1>foo.automobile@1` is good, `baz.supercar@1>bar.automobile@1` is good, `bar.foo.automobile@1>foo.automobile@1` is bad, `baz.automobile.supercar@1>bar.automobile@1` is bad.

## Schema Versioning: You Break It, You Bump It

While it is fairly established practice to [semantically version source code](https://semver.org/), the world of data/artifact versioning is a bit more varied. As presented in the tour, each `Legolas.Schema` has a single version integer. The central rule that governs Legolas' schema versioning approach is:

**If an update is made to a schema that potentially requires existing data to be rewritten in order to comply with the updated schema, then the version integer associated with that schema should be incremented.**

In other words: you break it, you bump it!

For example, a schema author must increment their existing schema's version integer if any of the following changes are made:

- A new non-`>:Missing` required field is added to the schema.
- An existing required field's type restriction is tightened.
- An existing required field is renamed.

One benefit of Legolas' approach is that multiple schema versions may be defined in the same codebase, e.g. there's nothing that prevents `@schema("my-schema@1", ...)` and `@schema("my-schema@2", ...)` from being defined and utilized simultaneously. The source code that defines any given Legolas schema and/or consumes/produces Legolas tables is presumably already semantically versioned, such that consumer/producer packages can determine their compatibility with each other in the usual manner via interpreting major/minor/patch increments.

## Important Expectations Regarding Custom Field Assignments

Schema authors should ensure that their schema declarations meet two important expectations so that Legolas' `row` function behaves as intended and inter-schema composability is preserved.

First, a schema's custom field assignments should preserve the [idempotency](https://en.wikipedia.org/wiki/Idempotence) of `row` invocations, such that the following holds for all valid values of `fields`:

```jl
row(schema, row(schema, fields)) == row(schema, fields)
```

Second, a schema's custom field assignments should not observe mutable non-local state, such that the following holds for all valid values of `fields`:

```jl
row(schema, fields) == row(schema, fields)
```

## How to Avoid Breaking Schema Changes

It is preferable to avoid incrementing a schema's version integer ("making a breaking change") whenever possible to avoid code/data churn for consumers. Following the below guidelines should help make breaking changes less likely:

1. Allow required fields to be `Missing` whenever reasonable.
2. Prefer conservative field type restrictions from the get-go, to avoid needing to tighten them later.
3. Handle/enforce "potential deprecation paths" in a required field's RHS definition when possible. For example, imagine a schema that contains a required field `id::Union{UUID,String} = id` where `id` is either a `UUID`, or a `String` that may be parsed as a `UUID`. Now, let's imagine we decided we wanted to update the schema such that new tables ALWAYS normalize `id` to a proper `UUID`. In this case, it is preferable to simply update this required field to `id::Union{UUID,String} = UUID(id)` instead of `id::UUID = id`. The latter is a breaking change that requires incrementing the schema's version integer, while the former achieves the same practical result without breaking consumers of old data.
