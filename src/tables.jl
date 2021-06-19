function _columns(table)
    try
        return Tables.columns(table)
    catch
        @warn "encountered error during internal invocation of `Tables.columns`; provided table might not be Tables.jl-compliant"
        rethrow()
    end
end

#####
##### validate tables
#####

"""
    TODO
"""
function validate(table, legolas_schema::Schema)
    columns = _columns(table)
    Tables.rowcount(columns) > 0 || return nothing
    tables_schema = Tables.schema(columns)
    if tables_schema isa Tables.Schema
        try
            validate(tables_schema, legolas_schema)
        catch
            @warn "provided table's `Tables.Schema` does not appear to match provided `Legolas.Schema`. Run `[Legolas.Row($legolas_schema, r) for r in Tables.rows(t)]` to try converting the table `t` to a compatible representation."
            rethrow()
        end
    else
        @warn "could not determine `Tables.Schema` from provided table; skipping schema validation"
    end
    return nothing
end

"""
    TODO
"""
function validate(table)
    metadata = Arrow.getmetadata(table)
    (metadata isa Dict && haskey(metadata, LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY)) || throw(ArgumentError("`$LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY` field not found in Arrow table metadata"))
    validate(table, Schema(metadata[LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY]))
    return nothing
end

#####
##### read/write tables
#####

"""
    TODO
"""
function read(path; validate::Bool=true)
    table = read_arrow(path)
    validate && Legolas.validate(table)
    return table
end

"""
    TODO
"""
function write(io_or_path, table, schema::Schema; validate::Bool=true, kwargs...)
    # This `_columns` call is unfortunately necessary; ref https://github.com/JuliaData/Arrow.jl/issues/211
    # It is also the case that `Tables.schema(Tables.columns(table))` is more likely to return a `Tables.Schema`
    # (rather than `nothing`) than a bare `table`, especially if `table::Vector`. We should probably fix/improve
    # these upstream at some point.
    columns = _columns(table)
    validate && Legolas.validate(columns, schema)
    assign_to_table_metadata!(columns, (LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => schema_qualified_string(schema),))
    write_arrow(io_or_path, columns; kwargs...)
    return table
end

"""
    TODO
"""
function tobuffer(args...; kwargs...)
    io = IOBuffer()
    Legolas.write(io, args...; kwargs...)
    seekstart(io)
    return io
end

#####
##### read/write Arrow content to generic path types
#####
# It would be better if Arrow.jl supported a generic API for nonstandard path-like types so that
# we can avoid potential intermediate copies here, but its documentation is explicit that it only
# supports `Union{IO,String}`.
#
# TODO: upstream improvements to Arrow.jl to obviate these?

write_full_path(path::AbstractString, bytes) = (mkpath(dirname(path)); Base.write(path, bytes))
write_full_path(path, bytes) = Base.write(path, bytes)

read_arrow(io_or_path::Union{IO,String,Vector{UInt8}}) = Arrow.Table(io_or_path)
read_arrow(path) = read_arrow(Base.read(path))

write_arrow(path::String, table; kwargs...) = Arrow.write(path, table; kwargs...)
write_arrow(io::IO, table; kwargs...) = Arrow.write(io, table; file=get(kwargs, :file, true), kwargs...)
write_arrow(path, table; kwargs...) = (io = IOBuffer(); write_arrow(io, table; kwargs...); write_full_path(path, take!(io)))

#####
##### Arrow.Table metadata manipulation
#####
# TODO: upstream to Arrow.jl?

"""
    TODO

Note that we intend to eventually migrate this function from Legolas.jl to a more appropriate package.
"""
function assign_to_table_metadata!(table, pairs)
    m = Arrow.getmetadata(table)
    if !(m isa Dict)
        m = Dict{String,String}()
        Arrow.setmetadata!(table, m)
    end
    for (k, v) in pairs
        m[k] = v
    end
    return m
end

#####
##### Tables.jl operations
#####
# TODO: Upstream these to a more appropriate location?

function locations(collections::T) where {T<:Tuple}
    N = fieldcount(T)
    K = promote_type(eltype.(collections)...)
    results = Dict{K,NTuple{N,Vector{Int}}}()
    for (c, collection) in enumerate(collections)
        for (i, item) in enumerate(collection)
            push!(get!(() -> ntuple(_ -> Int[], N), results, item)[c], i)
        end
    end
    return results
end

function _iterator_for_column(table, c)
    Tables.columnaccess(table) && return Tables.getcolumn(_columns(table), c)
    # there's not really a need to actually materialize this iterable
    # for the caller, but doing so allows the caller to more usefully
    # employ `eltype` on this function's output (since e.g. a generator
    # would just return `Any` for the eltype)
    return [Tables.getcolumn(r, c) for r in Tables.rows(table)]
end

"""
    gather(column_name, tables...; extract=((table, idxs) -> view(table, idxs, :)))

Gather rows from `tables` into a unified cross-table index along `column_name`. Returns
a `Dict` whose keys are the unique values of `column_name` across `tables`, and whose
values are tuples of the form:

    (rows_matching_key_in_table_1, rows_matching_key_in_table_2, ...)

The provided `extract` function is used to extract rows from each table; it takes
as input a table and a `Vector{Int}` of row indices, and returns the corresponding
subtable. The default definition is sufficient for `DataFrames` tables.

Note that this function may internally call `Tables.columns` on each input table, so
it may be slower and/or require more memory if `any(!Tables.columnaccess, tables)`.

Note that we intend to eventually migrate this function from Legolas.jl to a more appropriate package.
"""
function gather(column_name, tables::Vararg{Any,N};
                extract=((cols, idxs) -> view(cols, idxs, :))) where {N}
    iters = ntuple(i -> _iterator_for_column(tables[i], column_name), N)
    return Dict(id => ntuple(i -> extract(tables[i], locs[i]), N) for (id, locs) in locations(iters))
end

"""
    materialize(table)

Return a fully deserialized copy of `table`.

This function is useful when `table` has built-in deserialize-on-access or
conversion-on-access behavior (like `Arrow.Table`) and you'd like to pay
such access costs upfront before repeatedly accessing the table. For example:

TODO: make this example runnable

```
julia> items = read_table(path_to_file);

# iterate through all elements of `items.nested_structures`
julia> @time foreach(identity, (nested_structure for nested_structure in items.nested_structures));
0.000126 seconds (306 allocations: 6.688 KiB)

julia> materialized = Onda.materialize(items);

julia> @time foreach(identity, (nested_structure for nested_structure in materialized.nested_structures));
  0.000014 seconds (2 allocations: 80 bytes)
```

Note that we intend to eventually migrate this function from Legolas.jl to a more appropriate package.
"""
materialize(table) = map(collect, Tables.columntable(table))
