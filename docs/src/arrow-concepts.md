# Arrow-Related Concepts/Conventions

!!! note

    If you're a newcomer to Legolas.jl, please familiarize yourself with the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/main/examples/tour.jl) before diving into this documentation.

Legolas.jl's target (de)serialization format, [Arrow](https://arrow.apache.org/), already features wide cross-language adoption, enabling Legolas-serialized tables to be seamlessly read into many non-Julia environments. This documentation section contains conventions related to Legolas-serialized Arrow tables that may be observable by generic Legolas-unaware Arrow consumers.

## Supporting Legolas Schema Discovery In Arrow Tables

Legolas defines a special field `legolas_schema_qualified` that Legolas-aware Arrow writers may include in an Arrow table's table-level metadata to indicate a particular Legolas schema with which the table complies.

Arrow tables which include this field are considered to "support Legolas schema discovery" and are referred to as "Legolas-discoverable", since Legolas consumers may employ this field to automatically match the table against available application-layer Legolas schema definitions.

If present, the `legolas_schema_qualified` field's value must be a [fully qualified schema identifier](TODO).

## Arrow File Naming Conventions

When writing a Legolas-discoverable Arrow table to a file, prefer using the file extension `*.<unqualified schema name>.arrow`. For example, if the file's table's Legolas schema is `baz.supercar@1>bar.automobile@1`,
use the file extension `*.baz.supercar.arrow`.
