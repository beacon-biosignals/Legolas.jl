#####
##### Schema
#####

const ALLOWED_SCHEMA_NAME_CHARACTERS = Char['-', '.', 'a':'z'..., '0':'9'...]

"""
    Legolas.is_valid_schema_name(x::AbstractString)

Return `true` if `x` is a valid schema name, return `false` otherwise.

Valid schema names are lowercase, alphanumeric, and may contain hyphens or periods.
"""
is_valid_schema_name(x::AbstractString) = all(i -> i in ALLOWED_SCHEMA_NAME_CHARACTERS, x)

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

# support (de)serialization of Schemas to Arrow
const LEGOLAS_SCHEMA_ARROW_NAME = Symbol("JuliaLang.Legolas.Schema")
Arrow.ArrowTypes.arrowname(::Type{<:Schema}) = LEGOLAS_SCHEMA_ARROW_NAME
Arrow.ArrowTypes.ArrowType(::Type{<:Schema}) = String
Arrow.ArrowTypes.toarrow(schema::Schema) = schema_qualified_string(schema)
Arrow.ArrowTypes.JuliaType(::Val{LEGOLAS_SCHEMA_ARROW_NAME}, ::Any) = Schema
Arrow.ArrowTypes.fromarrow(::Type{<:Schema}, qualified_string) = Schema(qualified_string)

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

"""
    schema_name(::Type{<:Legolas.Schema{name}})
    schema_name(::Legolas.Schema{name})

Return `name`.
"""
@inline schema_name(::Type{<:Schema{name}}) where {name} = name
@inline schema_name(schema::Schema) = schema_name(typeof(schema))

"""
    schema_version(::Type{Legolas.Schema{name,version}})
    schema_version(::Legolas.Schema{name,version})

Return `version`.
"""
@inline schema_version(::Type{<:Schema{name,version}}) where {name,version} = version
@inline schema_version(schema::Schema) = schema_version(typeof(schema))

"""
    schema_parent(::Type{Legolas.Schema{name,version}})
    schema_parent(::Legolas.Schema{name,version})

Return the `Legolas.Schema` instance that corresponds to the parent of the given `Legolas.Schema`.
"""
@inline schema_parent(::Type{<:Schema}) = nothing
@inline schema_parent(schema::Schema) = schema_parent(typeof(schema))

Base.show(io::IO, schema::Schema) = print(io, "Schema(\"$(schema_name(schema))@$(schema_version(schema))\")")

#####
##### methods overloaded by `@row`
#####

struct UnknownSchemaError <: Exception
    schema::Legolas.Schema
end

function Base.showerror(io::IO, e::UnknownSchemaError)
    print(io, """
              encountered unknown `Legolas.Schema` type: $(e.schema)

              This generally indicates that this schema has not been defined (i.e.
              the schema's corresponding `@row` statement has not been executed) in
              the current Julia session.

              In practice, this can arise if you try to read a Legolas table with a
              prescribed schema, but haven't actually loaded the schema definition
              (or commonly, haven't loaded the dependency that contains the schema
              definition - check the versions of loaded packages/modules to confirm
              your environment is as expected).

              Note that if you're in this particular situation, you can still load
              the raw table as-is without Legolas; e.g., to load an Arrow table, call `Arrow.Table(path)`.
              """)
    return nothing
end

# Note that there exist very clean generic implementations of `transform`/`validate`:
#
#    function transform(schema::Schema; fields...)
#        parent = schema_parent(schema)
#        parent isa Schema && (fields = transform(parent; fields...))
#        return _transform(schema; fields...)
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
# generated by the current implementation of the `@row` macro (see below) do not.

transform(s::Legolas.Schema; fields...) = throw(UnknownSchemaError(s))

function _transform end

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

function validate_expected_field(schema::Tables.Schema, name::Symbol, ::Type{T}) where {T}
    i = findfirst(==(name), schema.names)
    if isnothing(i)
        Missing <: T || throw(ArgumentError("could not find expected field `$name::$T` in $schema"))
    else
        schema.types[i] <: T || throw(ArgumentError("field `$name` has unexpected type; expected <:$T, found $(schema.types[i])"))
    end
    return nothing
end

"""
    schema_qualified_string(::Legolas.Schema{name,version})

Return this `Legolas.Schema`'s fully qualified schema identifier string. This string is
serialized as the `\"$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY\"` field value in table
metadata for table written via [`Legolas.write`](@ref).
"""
schema_qualified_string(s::Legolas.Schema) = throw(UnknownSchemaError(s))

#####
##### Row
#####

"""
    Legolas.Row(schema::Schema; fields...)
    Legolas.Row(schema::Schema, row)

Return a `Legolas.Row <: Tables.AbstractRow` instance whose fields are the provided `fields`
(or the fields of `row`) validated/transformed in accordance with provided `schema`.

For more details and examples, please see `Legolas.jl/examples/tour.jl`.
"""
struct Row{S<:Schema,F} <: Tables.AbstractRow
    schema::S
    fields::F
    function Row(schema::Schema; fields...)
        fields = transform(schema; fields...)
        return new{typeof(schema),typeof(fields)}(schema, fields)
    end
end

Row{S}(args...; kwargs...) where {S} = Row(S(), args...; kwargs...)

Row(schema::Schema, fields) = Row(schema, NamedTuple(Tables.Row(fields)))
Row(schema::Schema, fields::Row) = Row(schema, getfield(fields, :fields))
Row(schema::Schema, fields::NamedTuple) = Row(schema; fields...)

Base.propertynames(row::Row) = propertynames(getfield(row, :fields))
Base.getproperty(row::Row, name::Symbol) = getproperty(getfield(row, :fields), name)

Tables.getcolumn(row::Row, i::Int) = Tables.getcolumn(getfield(row, :fields), i)
Tables.getcolumn(row::Row, nm::Symbol) = Tables.getcolumn(getfield(row, :fields), nm)
Tables.columnnames(row::Row) = Tables.columnnames(getfield(row, :fields))

Base.:(==)(a::Row, b::Row) = getfield(a, :schema) == getfield(b, :schema) && getfield(a, :fields) == getfield(b, :fields)
Base.isequal(a::Row, b::Row) = isequal(getfield(a, :schema), getfield(b, :schema)) && isequal(getfield(a, :fields), getfield(b, :fields))
Base.hash(a::Row, h::UInt) = hash(Row, hash(getfield(a, :schema), hash(getfield(a, :fields), h)))

function Base.show(io::IO, row::Row)
    print(io, "Row($(getfield(row, :schema)), ")
    show(io, getfield(row, :fields))
    print(io, ")")
    return nothing
end

"""
    schema_field_names(::Type{<:Legolas.Schema})

Get a tuple with the names of the fields of this `Legolas.Schema`, including names that
have been inherited from this `Legolas.Schema`'s parent schema.
"""
schema_field_names(s::Legolas.Schema) = schema_field_names(typeof(s))
schema_field_names(::Legolas.Row{S}) where {S} = schema_field_names(S)
schema_field_names(::Type{<:Legolas.Row{S}}) where {S} = schema_field_names(S)

"""
    schema_field_types(::Legolas.Schema{name,version})

Get a tuple with the types of the fields of this `Legolas.Schema`, including types of fields that
have been inherited from this `Legolas.Schema`'s parent schema.
"""
schema_field_types(s::Legolas.Schema) = schema_field_types(typeof(s))
schema_field_types(::Legolas.Row{S}) where {S} = schema_field_types(S)
schema_field_types(::Type{<:Legolas.Row{S}}) where {S} = schema_field_types(S)

function _parse_schema_expr(x)
    if x isa Expr && x.head == :call && x.args[1] == :> && length(x.args) == 3
        child, _ = _parse_schema_expr(x.args[2])
        parent, _ = _parse_schema_expr(x.args[3])
        return child, parent
    end
    return nothing, nothing
end

_parse_schema_expr(str::AbstractString) = Schema(str), nothing

"""
    @row("name@version", field_expressions...)
    @row("name@version" > "parent_name@parent_version", field_expressions...)

Define a new `Legolas.Schema{name,version}` whose required fields are specified by `field_expressions`.
Returns `Legolas.Row{Legolas.Schema{name,version}}` which can be conveniently aliased to the caller's
preferred binding for a row constructor associated with `Legolas.Schema{name,version}`.

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
macro row(schema_expr, fields...)
    schema, parent = _parse_schema_expr(schema_expr)
    isnothing(schema) && throw(ArgumentError("`@row` schema argument must be of the form `\"name@X\"` or `\"name@X\" > \"parent@Y\"`. Received: $schema_expr"))
    fields = map(fields) do f
        original_f = f
        f isa Symbol && (f = Expr(:(::), f, :Any))
        f.head == :(::) && (f = Expr(:(=), f, f.args[1]))
        f.head == :(=) && f.args[1] isa Symbol && (f.args[1] = Expr(:(::), f.args[1], :Any))
        f.head == :(=) && f.args[1].head == :(::) || throw(ArgumentError("malformed `@row` field expression: $original_f"))
        return f
    end
    validate_fields = map(fields) do f
        name, type = f.args[1].args
        return :(validate_expected_field(tables_schema, $(Base.Meta.quot(name)), $(esc(type))))
    end
    field_exprs = [f.args[1] for f in fields]
    field_names = [e.args[1] for e in field_exprs]
    field_types = [e.args[2] for e in field_exprs]
    escaped_field_names = map(esc, field_names)
    schema_type = Base.Meta.quot(typeof(schema))
    quoted_parent = Base.Meta.quot(parent)
    schema_qualified_string = string(schema_name(schema), '@', schema_version(schema))
    schema_field_names = Expr(:tuple, map(QuoteNode, field_names)...)
    schema_field_types = Expr(:tuple, field_types...)
    parent_transform = nothing
    parent_validate = nothing
    if !isnothing(parent)
        schema_qualified_string = :(string($schema_qualified_string, '>', Legolas.schema_qualified_string($quoted_parent)))
        schema_field_names = :(($schema_field_names..., Legolas.schema_field_names($quoted_parent)...))
        schema_field_types = :(($schema_field_types..., Legolas.schema_field_types($quoted_parent)...))
        parent_transform = :(fields = transform($quoted_parent; fields...))
        parent_validate = :(validate(tables_schema, $quoted_parent))
    end

    legolas_row_arrow_name = :(Symbol("JuliaLang.", $schema_qualified_string))
    return quote
        Legolas.schema_qualified_string(::$schema_type) = $schema_qualified_string
        Legolas.schema_field_names(::Type{$schema_type}) = $schema_field_names
        Legolas.schema_field_types(::Type{$schema_type}) = $schema_field_types

        Legolas.schema_parent(::Type{<:$schema_type}) = $quoted_parent

        function Legolas._transform(::$schema_type; $([Expr(:kw, f, :missing) for f in escaped_field_names]...), other...)
            $(map(esc, fields)...)
            return (; $([Expr(:kw, f, f) for f in escaped_field_names]...), other...)
        end

        function Legolas._validate(tables_schema::Tables.Schema, legolas_schema::$schema_type)
            $(validate_fields...)
            return nothing
        end

        function Legolas.transform(schema::$schema_type; fields...)
            $parent_transform
            return _transform(schema; fields...)
        end

        function Legolas.validate(tables_schema::Tables.Schema, legolas_schema::$schema_type)
            $parent_validate
            return _validate(tables_schema, legolas_schema)
        end


        # Support (de)serialization as an Arrow column value via Arrow.ArrowTypes overloads.
        #
        # Note that this only really works in relatively simple cases; rely on this at your own peril.
        # See https://github.com/JuliaData/Arrow.jl/issues/230 for more details.
        #
        # Note also that the limited support here that DOES work participates in SemVer,
        # e.g. if we break this in future Legolas versions we should treat it as a breaking
        # change and bump version numbers accordingly.

        # We serialize as a triple of schema name, schema version, and fields.
        # This is for backwards compatibility. With this approach, defining methods per-Row type,
        # we could just serialize the fields alone.
        # This approach allows nested arrow serialization to work, ref <https://github.com/beacon-biosignals/Legolas.jl/issues/39>.
        Arrow.ArrowTypes.arrowname(::Type{<:Legolas.Row{$schema_type}}) = $legolas_row_arrow_name
        Arrow.ArrowTypes.ArrowType(::Type{Legolas.Row{$schema_type,F}}) where {F} = Tuple{String,Int,F}
        Arrow.ArrowTypes.toarrow(row::Legolas.Row{$schema_type}) = (String(Legolas.schema_name($schema_type)), Legolas.schema_version($schema_type), getfield(row, :fields))
        Arrow.ArrowTypes.JuliaType(::Val{$legolas_row_arrow_name}, ::Any) = Legolas.Row{$schema_type}
        Arrow.ArrowTypes.fromarrow(::Type{<:Legolas.Row{$schema_type}}, name, version, fields) = Legolas.Row{$schema_type}(fields)


        Legolas.Row{$schema_type}
    end
end

# More Arrow serialization: here we provide backwards compatibility for `JuliaLang.Legolas.Row`
# serialized tables.
const LEGOLAS_ROW_ARROW_NAME = Symbol("JuliaLang.Legolas.Row")
Arrow.ArrowTypes.arrowname(::Type{<:Legolas.Row}) = LEGOLAS_ROW_ARROW_NAME
Arrow.ArrowTypes.ArrowType(::Type{Legolas.Row{_,F}}) where {_,F} = Tuple{String,Int,F}
Arrow.ArrowTypes.toarrow(row::Legolas.Row{S}) where {S} = (String(Legolas.schema_name(S)), Legolas.schema_version(S), getfield(row, :fields))
Arrow.ArrowTypes.JuliaType(::Val{LEGOLAS_ROW_ARROW_NAME}, ::Any) = Legolas.Row
Arrow.ArrowTypes.fromarrow(::Type{<:Legolas.Row}, name, version, fields) = Legolas.Row(Legolas.Schema(name, version), fields)
