# This file provides an introductory tour for Legolas.jl. Run lines in the REPL
# to inspect output at each step! Tests are littered throughout to demonstrate
# functionality in a concrete manner, and so that we can ensure examples stay
# current as the package evolves.

using Legolas, Arrow, Tables, Test

using Legolas: @schema, @alias, complies_with, validate, apply

#####
##### Introduction
#####
# Legolas provides mechanisms for constructing, reading, writing, and validating
# Tables.jl-compliant values against extensible, versioned, user-specified *schemas*.

# Each Legolas schema declaration must specify the declared schema's name, version,
# and the set of required fields that a given row value must contain in order to comply
# with that schema. Let's use the `@schema` macro to declare a new schema named `"foo"`
# at an initial version `1` with the given required fields:
@schema("foo@1",
        a::Real,
        b::String,
        c,
        d::AbstractVector)

# This `@schema` declaration automatically overloaded a bunch of methods with respect to
# a special type corresponding to the "foo@1` schema. We can create new instances of this
# type via `Legolas.Schema`:
@test Legolas.Schema("foo@1") isa Legolas.Schema{:foo,1}
@test Legolas.Schema("foo@1") == Legolas.Schema("foo", 1) # this constructor is also available

# Let's declare a convenience type alias `Foo{v}` that can be used to more succinctly
# construct `Legolas.Schema("foo", v)` instances:
@alias("foo", Foo)

# We can use `Legolas.complies_with` to check whether a given `Tables.Schema` (ref https://tables.juliadata.org/stable/#Tables.Schema)
# complies with the `foo@1` schema. For example, all of the following schemas comply with `foo@1`:
for s in [Tables.Schema((:a, :b, :c, :d), (Real, String, Any, AbstractVector)), # All required fields must be present...
          Tables.Schema((:a, :b, :c, :d), (Int, String, Float64, Vector)), # ...and have subtypes that match the schema's declared type constraints.
          Tables.Schema((:b, :a, :d, :c), (String, Int, Vector, Float64)), # Fields do not have to be in any particular order, as long as they are present.
          Tables.Schema((:a, :b, :d), (Int, String, Vector)), # Fields whose declared type constraints are `>:Missing` may be elided entirely.
          Tables.Schema((:a, :x, :b, :y, :d), (Int, Any, String, Any, Vector))] # Non-required fields may also be present.
    @test complies_with(s, Foo{1}())
    # `Legolas.validate` is similar to `Legolas.complies_with`, but throws a descriptive
    # error instead of returning `false` when it finds mismatching fields
    @test validate(s, Foo{1}()) isa Nothing
end

# ...while these schemas do not:
for s in [Tables.Schema((:a, :c, :d), (Int, Float64, Vector)), # The required non-`>:Missing` field `b::String` is not present.
          Tables.Schema((:a, :b, :c, :d), (Int, String, Float64, Any))] # The type of required field `d::AbstractVector` is not `<:AbstractVector`.
    @test !complies_with(s, Foo{1}())
    @test_throws ArgumentError validate(s, Foo{1}())
end

# The expectations that characterize Legolas' particular notion of "schematic compliance" - requiring the
# presence of pre-specified declared fields, assuming non-present fields to be implicitly `missing`, and allowing
# the presence of non-required fields - were chosen such that the question "Does the table `t` comply with the Legolas
# schema `s`?" is roughly equivalent to "Can a logical view be trivially constructed atop table `t` that contains only
# the required fields declared by `s`?" The ability to cleanly ask this question enables a weak notion of "subtyping"
# (see https://en.wikipedia.org/wiki/Duck_typing, https://en.wikipedia.org/wiki/Liskov_substitution_principle) that is
# core to Legolas' mechanisms for defining, extending, and versioning interfaces to tabular data.

#####
##### `Legolas.Schema` Application And Per-Field Assignment Operations
#####
# We can use `Legolas.apply` to produce a "canonicalized" schema-compliant row value from a given set
# of  fields, provided as either keyword arguments or as an `Tables.AbstractRow`-compliant value:
fields = (; a = 1.0, b = "hi", c = π, d = [1,2,3])
@test apply(Foo{1}(); fields...) == fields
@test apply(Foo{1}(), fields) == fields

# This may seem like a fairly trivial function in the preceding example, but the `apply` function has
# some useful and convenient properties. Specifically, input fields provided to `apply` may:
#
# - ...contain required fields in any order
# - ...elide required fields, in which case the constructor will assume them to be `missing`
# - ...contain any other fields in addition to the required fields
#
# ...while the output of `Legolas.apply` will always:
#
# - ...be a `Tables.AbstractRow`-compliant value
# - ...comply with the provided schema (otherwise an exception will be thrown)
# - ...contain all required fields ("missing" fields are explicitly presented with `missing` values)
# - ...contain all provided non-required fields
#
# ...or else an exception will be thrown. Demonstrating a few of these properties:

# Providing the additional non-required field `x` in the input, which is preserved in the output:
@test apply(Foo{1}(); fields..., x = "x") == (; fields..., x = "x")

# Eliding the required field `c`, which is assigned `missing` in the output:
@test isequal(apply(Foo{1}(); a = 1.0, b = "hi", d = [1,2,3]), (; a = 1.0, b = "hi", c = missing, d = [1,2,3]))

# Providing the non-compliantly-typed field `d::Int`, inducing a `MethodError`:
@test_throws MethodError apply(Foo{1}(); a = 1.0, b = "hi", d = 2)

# Implicitly providing the non-compliantly-typed field `d::Missing`, inducing a `MethodError`:
@test_throws MethodError apply(Foo{1}(); a = 1.0, b = "hi")


#=


# We can `apply` our `Schema` instance to a set of fields to construct a `Tables.AbstractRow`-compliant
# value (see https://tables.juliadata.org/stable/#Tables.AbstractRow-1 for details):
row = apply(MySchema{1}(); a=1.5, b="hello", c="goodbye", d=2, e=["anything"])

# By examining `row`'s fields, we can see how the assignment expressions from our original
# `@schema` declaration were applied in a simple linear fashion to the input arguments:
@test row.a == sin(1.5)
@test row.b == string(sin(1.5), "hello", "goodbye")
@test row.c == [sin(1.5), string(sin(1.5), "hello", "goodbye"), "goodbye"]
@test row.d == 2
@test row.e == ["anything"]

# In fact, the field assignment expressions provided to the `@schema` macro are interpolated
# nearly as-is into the underlying code generated by `@schema`. For example, the relevant code
# generated by our `@schema` declaration above looks roughly like:
#
#     function Legolas._apply(::typeof(Legolas.Schema("my-schema", 1));
#                             a=missing, b=missing, c=missing, d=missing, e=missing,
#                             other...)
#         a::Real = sin(a)
#         b::String = string(a, b, c)
#         c::Any = [a, b, c]
#         d::Int = d
#         e::Any = e
#         return (; a, b, c, d, e, other...)
#     end
#
# This `Legolas._apply` method is invoked at the core of the `Legolas.apply`. As you might have
# noticed, `Legolas.apply` has two interesting properties we haven't yet demonstrated.
#
# Here we demonstrate the first property - required fields have a `missing` value by default:
x = apply(MySchema{1}(), a=1.5, b="hello", c="goodbye", d=2)
y = apply(MySchema{1}(), (; a=1.5, b="hello", c="goodbye", d=2, e=missing))
@test isequal(x, y)

# ...so this correctly throws a `MethodError` when evaluating `d::Int = missing`:
@test_throws MethodError apply(MySchema{1}(), a=1.5, b="hello", c="goodbye")

# And here's a demonstration of the second property - callers may pass in any other fields in
# addition to the required fields:
row = apply(MySchema{1}(); a=1.5, b="hello", c="goodbye", my_other_field=":)", d=2, e=["anything"])
@test row.my_other_field == ":)"

# Finally, there's also an `apply` method that accepts any `Tables.AbstractRow`-compliant value,
# and extracts all input fields from that value. Here, we demonstrate with a `NamedTuple`:
@test row == apply(MySchema{1}(), (a=1.5, b="hello", c="goodbye", d=2, e=["anything"], my_other_field=":)"))

# To summarize:
#
# - Inputs to `Legolas.apply`...
#     - ...may be any `Tables.AbstractRow`-compliant value
#     - ...may contain required fields in any order
#     - ...may elide required fields, in which case the constructor will assume them to be `missing`
#     - ...may contain any other fields in addition to the required fields
# - The output of `Legolas.apply`...
#     - ...will be a `Tables.AbstractRow`-compliant value
#     - ...will comply with the given schema
#     - ...will contain all required fields ("missing" fields are explicitly presented with `missing` values)
#     - ...will contain all provided non-required fields

########################################################################################
########################################################################################
########################################################################################





# Each Legolas schema specifies its own name, version, and set of required fields. Any `Tables.AbstractRow`-compliant
# value (see https://tables.juliadata.org/stable/#Tables.AbstractRow-1) that contains these required fields is
# considered to be "compliant" with that schema.

# The


# Our `@schema` declaration automatically defined/overloaded a bunch of handy methods with respect
# to our `Legolas.Schema{:foo,1}` type.


# Now we can pass *instances* of this sche

########################################################################################
########################################################################################
########################################################################################

# Legolas defines a notion of a *schema*

# which callers can use
# define new [`Tables.AbstractRow`-compliant](https://tables.juliadata.org/stable/#Tables.AbstractRow-1)
# row types that exhibit some opinionated (but desirable!) properties w.r.t. composability/extensibility.
#
# These row type properties are fundamental to the wider data curation patterns that Legolas.jl seeks to
# facilitate, so let's explore them before we dig deeper into Legolas.jl's other table-centric utilities.




# We can `apply` our `Schema` instance to a set of fields to construct a `Tables.AbstractRow`-compliant
# value (see https://tables.juliadata.org/stable/#Tables.AbstractRow-1 for details):

`Tables.AbstractRow`-compliant](https://tables.juliadata.org/stable/#Tables.AbstractRow-1)

row value must have in order to
# compl

required fields and transformations associated


# Declare a `Legolas.Schema` with name `"my-schema"` at version `1` whose *required fields*
# `a`, `b`, `c`, `d`, and `e` are specified via the provided assignment expressions:
@schema("my-schema@1",
        a::Real = sin(a),
        b::String = string(a, b, c),
        c = [a, b, c],
        d::Int,
        e)



@test MySchema{1}() == Legolas.Schema("my-schema", 1)
@test MySchema{1}() == Legolas.Schema("my-schema@1")

#####
##### Applying Our New Legolas Schema
#####
# which callers can use
# define new [`Tables.AbstractRow`-compliant](https://tables.juliadata.org/stable/#Tables.AbstractRow-1)
# row types that exhibit some opinionated (but desirable!) properties w.r.t. composability/extensibility.
#
# These row type properties are fundamental to the wider data curation patterns that Legolas.jl seeks to
# facilitate, so let's explore them before we dig deeper into Legolas.jl's other table-centric utilities.


# We can `apply` our `Schema` instance to a set of fields to construct a `Tables.AbstractRow`-compliant
# value (see https://tables.juliadata.org/stable/#Tables.AbstractRow-1 for details):
row = apply(MySchema{1}(); a=1.5, b="hello", c="goodbye", d=2, e=["anything"])

# By examining `row`'s fields, we can see how the assignment expressions from our original
# `@schema` declaration were applied in a simple linear fashion to the input arguments:
@test row.a == sin(1.5)
@test row.b == string(sin(1.5), "hello", "goodbye")
@test row.c == [sin(1.5), string(sin(1.5), "hello", "goodbye"), "goodbye"]
@test row.d == 2
@test row.e == ["anything"]

# In fact, the field assignment expressions provided to the `@schema` macro are interpolated
# nearly as-is into the underlying code generated by `@schema`. For example, the relevant code
# generated by our `@schema` declaration above looks roughly like:
#
#     function Legolas._apply(::typeof(Legolas.Schema("my-schema", 1));
#                             a=missing, b=missing, c=missing, d=missing, e=missing,
#                             other...)
#         a::Real = sin(a)
#         b::String = string(a, b, c)
#         c::Any = [a, b, c]
#         d::Int = d
#         e::Any = e
#         return (; a, b, c, d, e, other...)
#     end
#
# This `Legolas._apply` method is invoked at the core of the `Legolas.apply`. As you might have
# noticed, `Legolas.apply` has two interesting properties we haven't yet demonstrated.
#
# Here we demonstrate the first property - required fields have a `missing` value by default:
x = apply(MySchema{1}(), a=1.5, b="hello", c="goodbye", d=2)
y = apply(MySchema{1}(), (; a=1.5, b="hello", c="goodbye", d=2, e=missing))
@test isequal(x, y)

# ...so this correctly throws a `MethodError` when evaluating `d::Int = missing`:
@test_throws MethodError apply(MySchema{1}(), a=1.5, b="hello", c="goodbye")

# And here's a demonstration of the second property - callers may pass in any other fields in
# addition to the required fields:
row = apply(MySchema{1}(); a=1.5, b="hello", c="goodbye", my_other_field=":)", d=2, e=["anything"])
@test row.my_other_field == ":)"

# Finally, there's also an `apply` method that accepts any `Tables.AbstractRow`-compliant value,
# and extracts all input fields from that value. Here, we demonstrate with a `NamedTuple`:
@test row == apply(MySchema{1}(), (a=1.5, b="hello", c="goodbye", d=2, e=["anything"], my_other_field=":)"))

# To summarize:
#
# - Inputs to `Legolas.apply`...
#     - ...may be any `Tables.AbstractRow`-compliant value
#     - ...may contain required fields in any order
#     - ...may elide required fields, in which case the constructor will assume them to be `missing`
#     - ...may contain any other fields in addition to the required fields
# - The output of `Legolas.apply`...
#     - ...is a `Tables.AbstractRow`-compliant value
#     - ...will contain all required fields ("missing" fields are explicitly presented with `missing` values)
#     - ...will contain all provided non-required fields

#####
##### Extending Existing Schemas
#####
# Schemas declared via `@schema` can inherit the fields and transformations specified by other schema declarations.
# Niftily, the properties of schema application that we demonstrated above enable this extension mechanism to be
# implemented via composition under the hood.

# Declare a row type whose schema is named `"my-child-schema"` at version `1` that inherits the fields of the
# `my-schema@1` schema that we defined in the previous section.
@schema("my-child-schema@1" > "my-schema@1",
        d, # "declaring" the underlying `my-schema@1` field here so that it
           # can be referenced in our definition for the `f` field.
        f::Int = f + d,
        g::String,
        c = last(c))

# The constructor for `MyChildRow` will first apply its parent's transformation before applying its own. The
# effect of this behavior can be seen clearly in the `c` field value in the following example:
input = (a=1.5, b="hello", c="goodbye", d=2, e=["anything"], f=3, g="foo")
child = apply(MyChildSchema{1}(), input)
@test child.a == sin(1.5)
@test child.b == string(sin(1.5), "hello", "goodbye")
@test child.c == "goodbye"
@test child.d == 2
@test child.e == ["anything"]
@test child.f == 5
@test child.g == "foo"

# Note that even though we didn't write down any constraints on `d` in our `my-child-schema@1` definition,
# that field still undergoes the parent transformation (defined by `my-schema@1`) where it is constrained
# to `d::Int`.
@test_throws InexactError MyChildRow(Tables.rowmerge(child; d=1.5))

# A note on syntax: You might ask "Why use `>` as the inheritance operator instead of `<:`?" There are actually three reasons. Firstly,
# `<:` is canonically a subtyping operator that implies the Liskov substition principle, but because Legolas allow arbitrary RHS code in
# required field declarations, a "child" row is not de facto substitutable for its parent. Secondly, `>` implies the actual ordering that
# row transformations are applied in; the parent transformation comes before the child transformation. Thirdly, the child row will usually
# (though technically not always) have a greater total number of required fields than the parent row.

#####
##### Validating/Writing/Reading `Arrow.Table`s with Legolas.jl
#####

# Legolas provides special methods for reading/writing/validating Arrow tables that utilize `Legolas.Schema`s. To
# start exploring these methods, we'll first construct a dummy table using `row::MyRow` from the previous section:
rows = [row,
        Tables.rowmerge(row; b="a different one"),
        Tables.rowmerge(row; e=:anything)]

# We can validate that `rows` is compliant w.r.t. `schema` via `Legolas.validate`, which will throw a descriptive
# error if the `Tables.schema(rows)` mismatches with the required columns/types specified by the `schema`.
schema = Schema("my-schema", 1)
@test schema === Schema("my-schema@1") # `Schema("my-schema", 1)` is an alternative to `Schema("my-schema@1")`
@test (Legolas.validate(rows, schema); true)
invalid = vcat(rows, Tables.rowmerge(row; a="this violates the schema's `a::Real` requirement"))
@test_throws ArgumentError("field `a` has unexpected type; expected <:Real, found Any") Legolas.validate(invalid, schema)

# This highlights two important properties regarding `Legolas.Schema` validation:
#
# - First, it's okay that the `e` field isn't present in this `Tables.Schema` because `my-schema` permits `e::Missing`.
# - Second, field ordering is unimportant and is not considered when determining whether a give `Tables.`
@test (Legolas.validate(Tables.Schema((:c, :d, :a, :b), Tuple{Vector,Int,Float64,String}), schema); true)

# Legolas also provides `Legolas.write(path_or_io, table, schema; kwargs...)`, which wraps `Arrow.write(path_or_io, table; kwargs...)`
# and performs two additional operations atop the usual operations performed by that function:
#
# - By default, the provided Tables.jl-compliant `table` is validated against `schema` via `Legolas.validate` before
#   it is actually written out. Note that this can be disabled by passing `validate=false` to `Legolas.write`.
#
# - `Legolas.write` ensures that the written-out Arrow table's metadata contains a `"legolas_schema_qualified"`
#   key whose value is `Legolas.schema_qualified_string(schema)`. This field enables consumers of the table to
#   perform automated (or manual) schema discovery/evolution/validation.

schema = Schema("my-child-schema", 1) # For this example, let's use a schema that has a parent
rows = [child,
        Tables.rowmerge(child; b="a different one"),
        Tables.rowmerge(child; e=:anything)]
io = IOBuffer()
Legolas.write(io, rows, schema)
t = Arrow.Table(seekstart(io))

table_isequal(a, b) = isequal(Legolas.materialize(a), Legolas.materialize(b))

@test Arrow.getmetadata(t) == Dict("legolas_schema_qualified" => "my-child-schema@1>my-schema@1")
@test table_isequal(t, Arrow.Table(Arrow.tobuffer(rows)))
@test table_isequal(t, Arrow.Table(Legolas.tobuffer(rows, schema))) # `Legolas.tobuffer` is analogous to `Arrow.tobuffer`

invalid = vcat(rows, Tables.rowmerge(child; a="this violates the schema's `a::Real` requirement"))
@test_throws ArgumentError("field `a` has unexpected type; expected <:Real, found Any") Legolas.tobuffer(invalid, schema)

# Similarly, Legolas provides `Legolas.read(path_or_io)`, which wraps `Arrow.Table(path_or_io)`
# and performs `Legolas.validate` on the resulting `Arrow.Table` before returning it.
@test table_isequal(Legolas.read(Legolas.tobuffer(rows, schema)), t)
msg = """
      could not extract valid `Legolas.Schema` from provided Arrow table;
      is it missing the expected custom metadata and/or the expected
      \"legolas_schema_qualified\" field?
      """
@test_throws ArgumentError(msg) Legolas.read(Arrow.tobuffer(rows))
invalid_but_has_schema_metadata = Arrow.tobuffer(invalid;
                                                 metadata = ("legolas_schema_qualified" => Legolas.schema_qualified_string(schema),))
@test_throws ArgumentError("field `a` has unexpected type; expected <:Real, found Union{Missing, Float64, String}") Legolas.read(invalid_but_has_schema_metadata)

# A note about one additional benefit of `Legolas.read`/`Legolas.write`: Unlike their Arrow.jl counterparts,
# these functions are relatively agnostic to the types of provided path arguments. Generally, as long as a
# given `path` supports `Base.read(path)::Vector{UInt8}` and `Base.write(path, bytes::Vector{UInt8})` then
# `path` will work as an argument to `Legolas.read`/`Legolas.write`. At some point, we'd like to make similar
# upstream improvements to Arrow.jl to render its API more path-type-agnostic.
=#
