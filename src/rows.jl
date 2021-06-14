#####
##### Schema
#####

const ALLOWED_SCHEMA_NAME_CHARACTERS = Char['-', '.', 'a':'z'..., '0':'9'...]

"""
    TODO
"""
is_valid_schema_name(x::AbstractString) = all(i -> i in ALLOWED_SCHEMA_NAME_CHARACTERS, x)

"""
    TODO
"""
struct Schema{name,version} end

"""
    TODO
"""
function Schema(name::AbstractString, version::Integer)
    is_valid_schema_name(name) || throw(ArgumentError("TODO"))
    return Schema{Symbol(name),version}()
end

"""
    TODO
"""
function Schema(str::AbstractString)
    x = split(first(split(str, '>', limit=2)), '@')
    if length(x) == 2
        name, version = x
        version = tryparse(Int, version)
        version isa Int && return Schema(name, version)
    end
    throw(ArgumentError("TODO"))
end

"""
    TODO
"""
@inline schema_version(::Type{<:Schema{name,version}}) where {name,version} = version
@inline schema_version(schema::Schema) = schema_version(typeof(schema))

"""
    TODO
"""
@inline schema_name(::Type{<:Schema{name}}) where {name} = name
@inline schema_name(schema::Schema) = schema_name(typeof(schema))

"""
    TODO
"""
@inline schema_parent(::Type{<:Schema}) = nothing
@inline schema_parent(schema::Schema) = schema_parent(typeof(schema))

"""
    TODO
"""
function schema_qualified_string end

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

"""
    TODO
"""
function transform end

function _transform end

"""
    TODO
"""
function validate end

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

Base.show(io::IO, schema::Schema) = print(io, "Schema(\"$(schema_name(schema))@$(schema_version(schema))\")")

#####
##### Row
#####

"""
    TODO
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

Base.show(io::IO, row::Row) = print(io, "Row($(getfield(row, :schema)), $(getfield(row, :fields)))")

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
    TODO
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
    field_names = [esc(f.args[1].args[1]) for f in fields]
    schema_type = Base.Meta.quot(typeof(schema))
    quoted_parent = Base.Meta.quot(parent)
    schema_qualified_string = string(schema_name(schema), '@', schema_version(schema))
    parent_transform = nothing
    parent_validate = nothing
    if !isnothing(parent)
        schema_qualified_string = :(string($schema_qualified_string, '>', Legolas.schema_qualified_string($quoted_parent)))
        parent_transform = :(fields = transform($quoted_parent; fields...))
        parent_validate = :(validate(tables_schema, $quoted_parent))
    end
    return quote
        Legolas.schema_qualified_string(::$schema_type) = $schema_qualified_string

        Legolas.schema_parent(::Type{<:$schema_type}) = $quoted_parent

        function Legolas._transform(::$schema_type; $([Expr(:kw, f, :missing) for f in field_names]...), other...)
            $(map(esc, fields)...)
            return (; $([Expr(:kw, f, f) for f in field_names]...), other...)
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

        Legolas.Row{$schema_type}
    end
end
