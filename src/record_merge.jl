"""
    record_merge(record::AbstractRecord; fields_to_merge...)

Return a new `AbstractRecord` with the same schema version as `record`, whose fields
are computed via `Tables.rowmerge(record; fields_to_merge...)`. The returned record is 
constructed by passing these merged fields to the `AbstractRecord` constructor 
that matches the type of the input `record`.
"""
function record_merge(record::AbstractRecord; fields_to_merge...)
    # Avoid using `typeof(record)` as can cause constructor failures with parameterized 
    # record types.
    R = record_type(schema_version_from_record(record))
    return R(Tables.rowmerge(record; fields_to_merge...))
end
