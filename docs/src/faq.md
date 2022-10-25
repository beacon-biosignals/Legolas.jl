# FAQ

## What is the point of Legolas.jl? Who benefits from using it?

At its core, Legolas.jl provides a lightweight, expressive set of mechanisms/patterns for wrangling Tables.jl-compliant values in a manner that enables schema composability, extensibility and a few nice utilties on top.

The package originated from code developed internally at Beacon to wrangling heterogeneous Arrow datasets, and is thus probably mostly useful for folks in a similar situation. If you're curating tabular datasets and you'd like to build shared Julia tools atop the schemas therein, then Legolas.jl may be worth checking out.

## Why does Legolas.jl support Arrow as a (de)serialization target, but not, say, JSON?

Technically, Legolas.jl's core `@schema`/`@version` functionality is agnostic to (de)serialization and could be useful for anybody who wants to wrangle Tables.jl-compliant values.

Otherwise, with regards to (de)serialization-specific functionality, Beacon has put effort into ensuring Legolas.jl works well with [Arrow.jl](https://github.com/JuliaData/Arrow.jl) "by default" simply because we're heavy users of the Arrow format. There's nothing stopping users from composing the package with [JSON3.jl](https://github.com/quinnj/JSON3.jl) or other packages.
