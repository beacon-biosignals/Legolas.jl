const SCHEMA_METADATA_KEY = "legolas_qualified_schema"

function validate(schema::Schema, table)
    tables_schema = Tables.schema(table)
    if tables_schema isa Tables.Schema
        validate(tables_schema, schema)
    else
        @warn "could not determine `Tables.Schema` from given table; skipping schema validation"
    end
    return nothing
end

function validate(table)
    metadata = Arrow.getmetadata(table)
    (metadata isa Dict && haskey(metadata, SCHEMA_METADATA_KEY)) || throw(ArgumentError("`$SCHEMA_METADATA_KEY` field not found in Arrow table metadata"))
    validate(Schema(metadata[SCHEMA_METADATA_KEY]), table)
    return nothing
end