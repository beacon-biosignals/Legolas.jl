# Legolas Table Specification

Legolas.jl's target (de)serialization format, Arrow, already features wide cross-language adoption, enabling serialized "Legolas tables" to be seamlessly read into many non-Julia environments. This brief specification defines the requirements for any given serialized Arrow table to be considered a "valid Legolas table" agnostic to any particular implementation.

Currently, there is only a single requirement: the presence of a `legolas_schema_qualified` field in the Arrow table's metadata.

The `legolas_schema_qualified` field's value should be either:

1. `name@version` where:
    - `name` is a lowercase alphanumeric string and may include the special characters `.` and `-`
    - `version` is a non-negative integer
2. `x>y` where `x` and `y` are valid `legolas_schema_qualified` strings

This field may be employed by consumers to match deserialized Legolas tables to application-layer schema definitions.