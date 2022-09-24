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
    TODO
"""
@inline schema_registered(::Schema) = false

"""
    schema_qualified_string(::Legolas.Schema)

Return this `Legolas.Schema`'s fully qualified schema identifier string. This string is
serialized as the `\"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\"` field value in table
metadata for table written via [`Legolas.write`](@ref).
"""
schema_qualified_string(schema::Schema) = throw(UnknownSchemaError(schema))

"""
TODO

- returns NamedTuple of DataTypes
- typeintersect to combine
"""
schema_fields(schema::Schema) = throw(UnknownSchemaError(schema))

"""
TODO

- returns a `Dict{Symbol,Expr}` containing each required field statement for interactive discovery usage (including parent fields)
- only one entry per field; merge "subsequent" field declarations / constraints
"""
schema_field_statements(schema::Schema) = throw(UnknownSchemaError(schema))

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
=#

#####
##### `row`
#####
# TODO more heavily encourage idempotency in documentation/examples

row(s::Legolas.Schema; fields...) = throw(UnknownSchemaError(s))
row(schema::Schema, fields::NamedTuple) = row(schema; fields...)
row(schema::Schema, fields) = row(schema, NamedTuple(Tables.Row(fields)))

function _row end

#####
##### `@schema`
#####

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

function _do_child_fields_subtype_parent_fields(child_fields::NamedTuple, parent_fields::NamedTuple)
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
    fields_named_tuple = Expr(:tuple, (:($n = $t) for (n, t) in fields)...)
    schema_qualified_string = string(schema_name(schema), '@', schema_version(schema))
    quoted_schema = Base.Meta.quot(schema)
    quoted_schema_type = Base.Meta.quot(typeof(schema))
    quoted_parent = Base.Meta.quot(parent)
    quoted_field_statements = Base.Meta.quot(field_statements)
    check_parent_statements = isnothing(parent) ? nothing : quote
        Legolas.schema_registered($quoted_parent) || throw(SchemaDeclarationError("TODO"))
        Legolas._do_child_fields_subtype_parent_fields($fields_named_tuple, Legolas.schema_fields($quoted_parent)) || throw(SchemaDeclarationError("TODO"))
    end
    matches_existing_schema_definition = "TODO"
    return quote
        begin
            if Legolas.schema_registered($quoted_schema)
                $matches_existing_schema_definition || throw(SchemaDeclarationError("TODO"))
            else
                $check_parent_statements
                eval(Legolas._generate_schema_overload_statements($quoted_schema, $quoted_parent, $quoted_field_statements))
            end
        end
    end
end

# Note that there exists a clean generic approach for implementing `row` that is applicable
# to the other Legolas functions that similarly compose child/parent schema behaviors:
#
#    function _row(schema; fields...)
#        # perform only the transformations directly specified by the target
         # schema declaration (i.e. do not include parent transformations)
#    end
#
#    function row(schema::Schema; fields...)
#        parent = schema_parent(schema)
#        parent isa Schema && (fields = row(parent; fields...))
#        return _row(schema; fields...)
#    end
#
# However, basic benchmarking demonstrates that the above approach can induce unnecessary
# allocations for schemas with a few ancestors, while the "hardcoded" approach generated
# below does not. Futhermore, this approach results in unintuitive field ordering compared
# the hardcoded approach, ref https://github.com/beacon-biosignals/Legolas.jl/issues/12.

function _generate_schema_overload_statements(schema::Schema, parent::Schema,
                                              field_statements::Vector{Expr})
    qualified_string = string(schema_name(schema), '@', schema_version(schema))
    qualified_string = string(qualified_string, '>', schema_qualified_string(parent))
    fields =


    # TODO check if this works even if `Legolas` is not bound in evalutation context
    return quote
        $(Base.Meta.quot(Legolas)).schema_qualified_string(::$schema_type) = $qualified_string

        $(Base.Meta.quot(Legolas)).schema_parent(::$schema_type) = $(Base.Meta.quot(parent))

        function Legolas.row(::$schema_type; $([Expr(:kw, f, :missing) for f in field_names]...), other...)
            $(Legolas.)
            $(field_statements...)
            return (; $([Expr(:kw, f, f) for f in field_names]...), other...)
        end

        # function Legolas._apply(::$schema_type; $([Expr(:kw, f, :missing) for f in field_names]...), other...)
        #     $(map(esc, fields)...)
        #     return (; $([Expr(:kw, f, f) for f in field_names]...), other...)
        # end

        # function Legolas.apply(schema::$schema_type; fields...)
        #     $parent_apply
        #     return _apply(schema; fields...)
        # end
    end
end
