# API Documentation

!!! note

    If you're a newcomer to Legolas.jl, please familiarize yourself with the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/main/examples/tour.jl) before diving into this documentation.

```@meta
CurrentModule = Legolas
```

## Legolas `Schema`s

```@docs
Legolas.@schema
Legolas.Schema
Legolas.is_valid_schema_name
Legolas.schema_name
Legolas.schema_version
Legolas.schema_identifier
Legolas.schema_parent
Legolas.schema_fields
Legolas.schema_declaration
Legolas.parse_schema_identifier
Legolas.row
```

## Validating/Writing/Reading Legolas Tables

```@docs
Legolas.extract_legolas_schema
Legolas.write
Legolas.read
```

## Utilities

```@docs
Legolas.lift
Legolas.construct
Legolas.guess_schema
Legolas.assign_to_table_metadata!
Legolas.gather
Legolas.locations
Legolas.materialize
```
