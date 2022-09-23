#####
##### schema name validation
#####

const ALLOWED_SCHEMA_NAME_CHARACTERS = Char['-', '.', 'a':'z'..., '0':'9'...]

"""
    Legolas.is_valid_schema_name(x::AbstractString)

Return `true` if `x` is a valid schema name, return `false` otherwise.

Valid schema names are lowercase, alphanumeric, and may contain hyphens or periods.
"""
is_valid_schema_name(x::AbstractString) = all(i -> i in ALLOWED_SCHEMA_NAME_CHARACTERS, x)

#####
##### `Schema`
#####

"""
    Legolas.Schema{name,version}

A type representing the schema of a [`Legolas.Row`](@ref). The `name` (a `Symbol`) and `version` (an `Integer`)
are surfaced as type parameters, allowing them to be utilized for dispatch.

For more details and examples, please see `Legolas.jl/examples/tour.jl` and the "Tips for Schema Authors"
section of the Legolas.jl documentation.

See also: [`schema_name`](@ref), [`schema_version`](@ref), [`schema_parent`](@ref)
"""
struct Schema{name,version} end

Schema(schema::Schema) = schema

"""
    Legolas.Schema(name::AbstractString, version::Integer)

Return `Legolas.Schema{Symbol(name),version}()`. This constructor will throw an `ArgumentError` if `name` is
not a valid schema name.

Prefer using this constructor over `Legolas.Schema{Symbol(name),version}()` directly.
"""
function Schema(name::AbstractString, version::Integer)
    version >= 0 || throw(ArgumentError("`Legolas.Schema` version must be non-negative, recieved: $version"))
    is_valid_schema_name(name) || throw(ArgumentError("argument is not a valid `Legolas.Schema` name: \"$name\""))
    return Schema{Symbol(name),version}()
end

"""
    Legolas.Schema(s::AbstractString)

Return `Legolas.Schema(name, n)` where `s` is a valid schema identifier of the form `"name@n"`.

`s` may also be a fully qualified schema identifier of the form `"name@n>...>..."`.
"""
function Schema(s::AbstractString)
    x = split(first(split(s, '>', limit=2)), '@')
    if length(x) == 2
        name, version = x
        version = tryparse(Int, version)
        version isa Int && return Schema(name, version)
    end
    throw(ArgumentError("argument is not a valid `Legolas.Schema` string: \"$s\""))
end

#####
##### `@alias`
#####

macro alias(schema_name, T)
    schema_name isa String || throw(ArgumentError("`schema_name` must be a `String`"))
    version_symbol = esc(:v)
    return quote
        const $(esc(T)){$version_symbol} = Legolas.Schema{Symbol($schema_name),$version_symbol}
        $(esc(T))() = error("TODO")
    end
end

#####
##### `UnknownSchemaError`
#####

struct UnknownSchemaError <: Exception
    schema::Legolas.Schema
end

function Base.showerror(io::IO, e::UnknownSchemaError)
    print(io, """
              encountered unknown `Legolas.Schema` type: $(e.schema)

              This generally indicates that this schema has not been declared (i.e.
              the schema's corresponding `@schema` statement has not been executed) in
              the current Julia session.

              In practice, this can arise if you try to read a Legolas table with a
              prescribed schema, but haven't actually loaded the schema definition
              (or commonly, haven't loaded the dependency that contains the schema
              definition - check the versions of loaded packages/modules to confirm
              your environment is as expected).

              Note that if you're in this particular situation, you can still load the raw
              table as-is without Legolas (e.g. via `Arrow.Table(path_to_table)`).
              """)
    return nothing
end

#####
##### `Schema` property accessors/generators
#####

"""
    schema_name(::Legolas.Schema{name})

Return `name`.
"""
@inline schema_name(::Schema{name}) where {name} = name

"""
    schema_version(::Legolas.Schema{name,version})

Return `version`.
"""
@inline schema_version(::Schema{name,version}) where {name,version} = version

"""
    schema_parent(::Legolas.Schema)

Return the `Legolas.Schema` instance that corresponds to the parent of the given `Legolas.Schema`.
"""
@inline schema_parent(::Schema) = nothing

"""
    schema_qualified_string(::Legolas.Schema)

Return this `Legolas.Schema`'s fully qualified schema identifier string. This string is
serialized as the `\"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\"` field value in table
metadata for table written via [`Legolas.write`](@ref).
"""
schema_qualified_string(schema::Legolas.Schema) = throw(UnknownSchemaError(schema))

"""
TODO

- returns NamedTuple of DataTypes
- typeintersect to combine
"""
schema_fields(schema::Legolas.Schema) = throw(UnknownSchemaError(schema))

"""
TODO

- returns a `Dict{Symbol,Expr}` containing each required field statement for interactive discovery usage (including parent fields)
- only one entry per field; merge "subsequent" field declarations / constraints
"""
schema_field_statements(schema::Legolas.Schema) = throw(UnknownSchemaError(schema))

#####
##### `Schema` printing
#####

# TODO this should be compact version; full version should print `schema_field_statements`
Base.show(io::IO, schema::Schema) = print(io, "Schema(\"$(schema_name(schema))@$(schema_version(schema))\")")

#####
##### `Schema` Arrow (de)serialization
#####

# support (de)serialization of Schemas to Arrow
const LEGOLAS_SCHEMA_ARROW_NAME = Symbol("JuliaLang.Legolas.Schema")
Arrow.ArrowTypes.arrowname(::Type{<:Schema}) = LEGOLAS_SCHEMA_ARROW_NAME
Arrow.ArrowTypes.ArrowType(::Type{<:Schema}) = String
Arrow.ArrowTypes.toarrow(schema::Schema) = schema_qualified_string(schema)
Arrow.ArrowTypes.JuliaType(::Val{LEGOLAS_SCHEMA_ARROW_NAME}, ::Any) = Schema
Arrow.ArrowTypes.fromarrow(::Type{<:Schema}, qualified_string) = Schema(qualified_string)

#####
##### `Tables.Schema` validation
#####

#=
function validate(tables_schema, legolas_schema)
    result = find_violation(tables_schema, legolas_schema)
    isnothing(result) && return nothing
    field, violation = result
    ismissing(violation) || throw(ArgumentError("could not find expected field `$field::$T` in $tables_schema"))
    throw(ArgumentError("field `$field` has unexpected type; expected <:$T, found $violation"))
end

"""
    Legolas.validate(tables_schema::Tables.Schema, legolas_schema::Legolas.Schema)

Throws an `ArgumentError` if `tables_schema` does not comply with `legolas_schema`, otherwise
returns `nothing`.

Specifically, `tables_schema` is considered to comply with `legolas_schema` if:

- every non-`>:Missing` field required by `legolas_schema` is present in `tables_schema`.
- `T <: S` for each field `f::T` in `tables_schema` that matches a required `legolas_schema` field `f::S`.
"""
validate(::Tables.Schema, s::Legolas.Schema) = throw(UnknownSchemaError(s))

function _validate end

"""
TODO
"""
find_violation(::Tables.Schema, s::Legolas.Schema) = throw(UnknownSchemaError(s))

function _complies_with end

function _check_expected_field(schema::Tables.Schema, name::Symbol, ::Type{T}) where {T}
    i = findfirst(==(name), schema.names)
    if isnothing(i)
        Missing <: T || return missing
    else
        schema.types[i] <: T || return schema.types[i]
    end
    return nothing
end

find_violation()

function validate_expected_field(schema, name, T)
    result = _check_expected_field(schema, name, T)
    isnothing(result) && return nothing
    ismissing(result) || throw(ArgumentError("could not find expected field `$name::$T` in $schema"))
    throw(ArgumentError("field `$name` has unexpected type; expected <:$T, found $result"))
end

complies_with_expected_field(schema, name, T) = isnothing(_check_expected_field(schema, name, T))

#####
##### `row`
#####
# TODO more heavily encourage idempotency in documentation/examples

row(s::Legolas.Schema; fields...) = throw(UnknownSchemaError(s))
row(schema::Schema, fields::NamedTuple) = row(schema; fields...)
row(schema::Schema, fields) = row(schema, NamedTuple(Tables.Row(fields)))

function _row end

=#

#####
##### `@schema`
#####
# Note that there exist very clean generic implementations of `apply`/`validate`/etc.:
#
#    function apply(schema::Schema; fields...)
#        parent = schema_parent(schema)
#        parent isa Schema && (fields = apply(parent; fields...))
#        return _apply(schema; fields...)
#    end
#
#    function validate(tables_schema::Tables.Schema, legolas_schema::Schema)
#        parent = schema_parent(legolas_schema)
#        parent isa Schema && validate(parent, tables_schema)
#        _validate(tables_schema, legolas_schema)
#        return nothing
#    end
#
# However, basic benchmarking demonstrates that the above versions can allocate
# unnecessarily for schemas with a few ancestors, while the "hardcoded" versions
# generated by the current implementation of the `@schema` macro (see below) do not.

struct SchemaDeclarationError <: Exception
    message::String
end

Base.showerror(io::IO, e::SchemaDeclarationError) = print(io, e.message)

function _parse_schema_expr(x)
    if x isa Expr && x.head == :call && x.args[1] == :> && length(x.args) == 3
        child, _ = _parse_schema_expr(x.args[2])
        parent, _ = _parse_schema_expr(x.args[3])
        return child, parent
    end
    return nothing, nothing
end

_parse_schema_expr(str::AbstractString) = Schema(str), nothing

# xref https://github.com/JuliaLang/julia/pull/36291#issuecomment-1229396243
function _merge_named_tuple_with_typeintersect(a::NamedTuple, b::NamedTuple)
    names = Base.merge_names(keys(a), keys(b))
    return NamedTuple{names}(ntuple(nfields(names)) do i
        n = getfield(names, i)
        if haskey(a, n)
            haskey(b, n) && return typeintersect(getfield(a, n), getfield(b, n))
            return getfield(a, n)
        else
            return getfield(b, n)
        end
    end)
end

function _child_fields_obey_parent_fields(child_fields::NamedTuple, parent_fields::NamedTuple)
    for (name, child_type) in pairs(child_fields)
        if haskey(parent_fields, name)
            child_type <: parent_fields[name] || return false
        end
    end
    return true
end

macro schema(schema_expr, field_exprs...)
    schema, parent = _parse_schema_expr(schema_expr)
    isnothing(schema) && throw(SchemaDeclarationError("`@schema` schema argument must be of the form `\"name@X\"` or `\"name@X\" > \"parent@Y\"`. Received: $schema_expr"))
    field_statements = map(field_exprs) do f
        original_f = f
        f isa Symbol && (f = Expr(:(::), f, :Any))
        f.head == :(::) && (f = Expr(:(=), f, f.args[1]))
        f.head == :(=) && f.args[1] isa Symbol && (f.args[1] = Expr(:(::), f.args[1], :Any))
        f.head == :(=) && f.args[1].head == :(::) || throw(SchemaDeclarationError("malformed `@schema` field expression: $original_f"))
        return f
    end
    fields = [f.args[1].args[1] => f.args[1].args[2] for f in field_statements]
    allunique(n for (n, _) in fields) || throw(SchemaDeclarationError("TODO"))


    if isnothing(parent)
        return quote
            TODO
        end
    else
        schema_type = Base.Meta.quot(typeof(schema))
        quoted_parent = Base.Meta.quot(parent)
        fields_named_tuple = Expr(:tuple, (:($n = $t) for (n, t) in fields)...)
        return quote
            if Legolas.schema_registered($quoted_parent)
                if Legolas._child_fields_obey_parent_fields($fields_named_tuple, Legolas.schema_fields($quoted_parent))
                    TODO
                else
                    throw(SchemaDeclarationError("TODO"))
                end
            else
                throw(SchemaDeclarationError("TODO"))
            end
        end
    end
end

#=
"""
    @schema("name@version", field_expressions...)
    @schema("name@version" > "parent_name@parent_version", field_expressions...)

Register a new `Legolas.Schema{name,version}` whose required fields are specified by `field_expressions`.

Each element of `field_expression` defines a required field for `Legolas.Schema{name,version}`, and is
an expression of the form `field::F = rhs` where:

- `field` is the corresponding field's name
- `::F` denotes the field's type constraint (if elided, defaults to `::Any`).
- `rhs` is the expression which produces `field::F` (if elided, defaults to `field`).

As implied above, the following alternative forms are also allowed:

- `field::F` (interpreted as `field::F = field`)
- `field = rhs` (interpreted as `field::Any = rhs`)
- `field` (interpreted as `field::Any = field`)

For more details and examples, please see `Legolas.jl/examples/tour.jl` and the "Tips for Schema Authors"
section of the Legolas.jl documentation.
"""
macro schema(schema_expr, fields...)
    schema_stmt, parent_stmt = _parse_schema_expr(schema_expr)
    isnothing(schema_stmt) && throw(ArgumentError("`@schema` schema argument must be of the form `\"name@X\"` or `\"name@X\" > \"parent@Y\"`. Received: $schema_expr"))
    fields = map(fields) do f
        original_f = f
        f isa Symbol && (f = Expr(:(::), f, :Any))
        f.head == :(::) && (f = Expr(:(=), f, f.args[1]))
        f.head == :(=) && f.args[1] isa Symbol && (f.args[1] = Expr(:(::), f.args[1], :Any))
        f.head == :(=) && f.args[1].head == :(::) || throw(ArgumentError("malformed `@schema` field expression: $original_f"))
        return f
    end
    # field_check_statements = fn -> begin
    #     return map(fields) do fld
    #         name, type = fld.args[1].args
    #         return :($fn(tables_schema, $(Base.Meta.quot(name)), $(esc(type))))
    #     end
    # end
    # field_names = [esc(f.args[1].args[1]) for f in fields]

    # schema_type = Base.Meta.quot(typeof(schema))
    # quoted_parent = Base.Meta.quot(parent)
    # schema_qualified_string = string(schema_name(schema), '@', schema_version(schema))
    # parent_apply = nothing
    # parent_validate = nothing
    # parent_complies_with = nothing
    # if !isnothing(parent_stmt)
    #     schema_qualified_string = :(string($schema_qualified_string, '>', Legolas.schema_qualified_string($quoted_parent)))
    #     schema_field_names = :(($schema_field_names..., Legolas.schema_field_names($quoted_parent)...))
    #     schema_field_types = :(($schema_field_types..., Legolas.schema_field_types($quoted_parent)...))
    #     parent_apply = :(fields = apply($quoted_parent; fields...))
    #     parent_validate = :(validate(tables_schema, $quoted_parent))
    #     parent_complies_with = :(complies_with(tables_schema, $quoted_parent))
    # end
    if isnothing(parent_stmt)
        return quote
            if Legolas.schema_registered(::$schema_type) && $todo

            else
                throw(SchemaDefinitionError("TODO"))
            end
        end
    else
        return different
    end

    quote
        if Legolas.schema_registered(::$schema_type) && $todo
            # compute stuff here

            # eval the below:


            Legolas.schema_qualified_string(::$schema_type) = $schema_qualified_string

            Legolas.schema_parent(::Type{<:$schema_type}) = $quoted_parent

            function Legolas._apply(::$schema_type; $([Expr(:kw, f, :missing) for f in field_names]...), other...)
                $(map(esc, fields)...)
                return (; $([Expr(:kw, f, f) for f in field_names]...), other...)
            end

            function Legolas._validate(tables_schema::Tables.Schema, legolas_schema::$schema_type)
                $(field_check_statements(:validate_expected_field)...)
                return nothing
            end

            function Legolas._complies_with(tables_schema::Tables.Schema, legolas_schema::$schema_type)
                return $(foldr((a, b) -> :($a && $b), field_check_statements(:complies_with_expected_field)))
            end

            function Legolas.apply(schema::$schema_type; fields...)
                $parent_apply
                return _apply(schema; fields...)
            end

            function Legolas.validate(tables_schema::Tables.Schema, legolas_schema::$schema_type)
                $parent_validate
                return _validate(tables_schema, legolas_schema)
            end

            function Legolas.complies_with(tables_schema::Tables.Schema, legolas_schema::$schema_type)
                $parent_complies_with
                return _complies_with(tables_schema, legolas_schema)
            end
        else
            throw(SchemaDefinitionError("TODO"))
        end
        nothing
    end
end
=#
