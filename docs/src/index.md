# API Documentation

If you're a newcomer to Legolas.jl, please familiarize yourself with via the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/master/examples/tour.jl) before diving into this documentation.

```@meta
CurrentModule = Legolas
```

## Legolas `Schema`s and `Row`s

```@docs
Legolas.@row
Legolas.Row
Legolas.Schema
Legolas.is_valid_schema_name
Legolas.schema_name
Legolas.schema_version
Legolas.schema_qualified_string
Legolas.schema_parent
```

## Validating/Writing/Reading Legolas Tables

```@docs
Legolas.validate
Legolas.write
Legolas.read
```

## Utilities

```@docs
Legolas.lift
Legolas.assign_to_table_metadata!
Legolas.gather
Legolas.materialize
```
