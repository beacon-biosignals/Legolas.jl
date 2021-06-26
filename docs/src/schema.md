# Tips for Schema Authors

If you're a newcomer to Legolas.jl, please familiarize yourself with via the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/master/examples/tour.jl) before diving into this documentation.

## Simple Integer Versioning: You Break It, You Bump It

While it is fairly established practice to [semantically version source code](https://semver.org/), the world of data/artifact versioning is a bit more varied. As presented in the tour, each `Legolas.Schema` has a single version integer. In this section, we'll discuss how to interpret this version integer.

We start with an assumption: source code that defines any given Legolas schema and/or consumes/produces Legolas tables is already semantically versioned, such that consumer/producer packages can determine their compatibility with each other in the usual manner via interpreting major/minor/patch increments.

With that in mind, here is the central guideline to obey as a Legolas schema author:

**If an update is made to a schema that potentially requires existing data to be rewritten in order to comply with the updated schema, then the version integer associated with that schema should be incremented.**

For example, you must increment the version integer if any of the following changes are made:

- A new non-`>:Missing` required field is added to the schema.
- An existing required field's type restriction is tightened.
- An existing required field is renamed.

## How to Avoid Breaking Changes

It is preferable to avoid incrementing a schema's version integer ("making a breaking change") whenever possible to avoid code/data churn for consumers. Following the below guidelines should help make breaking changes less likely:

1. Allow required fields to be `Missing` whenever reasonable.
2. Prefer conservative field type restrictions from the get-go, to avoid needing to tighten them later.
3. Handle/enforce "potential deprecation paths" in a required field's RHS definition when possible. For example, imagine a schema that contains a required field `id::Union{UUID,String} = id` where `id` is either a `UUID`, or a `String` that may be parsed as a `UUID`. Now, let's imagine we decided we wanted to update the schema such that new tables ALWAYS normalize `id` to a proper `UUID`. In this case, it is preferable to simply update this required field to `id::Union{UUID,String} = UUID(id)` instead of `id::UUID = id`. The latter is a breaking change that requires incrementing the schema's version integer, while the former achieves the same practical result without breaking consumers of old data.

When making deprecation/API decisions, keep in mind that multiple schema versions may be defined in the same codebase; there's nothing that prevents `@row("my-schema@1", ...)` and `@row("my-schema@2", ...)` from being defined/utilized simultaneously.

# Naming Conventions

1. Include a namespace. For example, assuming the schema is defined in a package Foo.jl, `foo.automobile` is good, `automobile` is bad.
2. Prefer singular over plural. For example, `foo.automobile` is good, `foo.automobiles` is bad.
3. Don't overqualify the schema name; that's what the qualified schema identifier is for! For example, `bar.automobile@1>foo.automobile@1` is good, `baz.supercar@1>bar.automobile@1` is good, `bar.foo.automobile@1>foo.automobile@1` is bad, `baz.automobile.supercar@1>bar.automobile@1` is bad.
4. When writing tables to files, use `*.<unqualified schema name>.arrow` as the file extension. For example, `filename.baz.supercar.arrow` is good, `filename.baz.supercar.bar.automobile.arrow` is bad, `baz.supercar.arrow` is bad.

# Other Tips For Composable Schemas

1. Prefer [idempotency](https://en.wikipedia.org/wiki/Idempotence) in required field's RHS definitions.
2. Prefer authoring child schemas such that they are [Liskov substitutable](https://en.wikipedia.org/wiki/Liskov_substitution_principle) for their parents. A less fancy way of stating the same thing: try to ensure that your child schema only adds additional fields to the parent and doesn't alter existing fields.
