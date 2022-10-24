# API Documentation

!!! note

    If you're a newcomer to Legolas.jl, please familiarize yourself with the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/main/examples/tour.jl) before diving into this documentation.

```@meta
CurrentModule = Legolas
```

## Legolas `Schema`s

```@docs
Legolas.SchemaVersion
Legolas.@schema
Legolas.@version
Legolas.is_valid_schema_name
Legolas.parse_identifier
Legolas.name
Legolas.version
Legolas.identifier
Legolas.parent
Legolas.fields
Legolas.declaration
Legolas.declared
Legolas.row
```

## Validating/Writing/Reading Legolas Tables

```@docs
Legolas.extract_schema_version
Legolas.write
Legolas.read
```

## Utilities

```@docs
Legolas.lift
Legolas.construct
Legolas.assign_to_table_metadata!
Legolas.gather
Legolas.locations
Legolas.materialize
```
