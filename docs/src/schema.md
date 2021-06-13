# Tips for Schema Authors

If you're a newcomer to Legolas.jl, please familiarize yourself with via the [tour](https://github.com/beacon-biosignals/Legolas.jl/blob/master/examples/tour.jl) before diving into this documentation.

TODO: cover the following items:

- Legolas.jl's Simple Integer Versioning: You Break It, You Bump It
- forward/backward compatibility via allowing `missing` columns when possible
- avoid bumping schema versions by handling the deprecation path in the constructor
- prefer idempotency in field expressions when possible
- prefer Liskov substitutability when possible

