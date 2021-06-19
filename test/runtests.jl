using Legolas, Test, DataFrames, Arrow

using Legolas: Schema, @row, Row

include(joinpath(dirname(@__DIR__), "examples", "tour.jl"))

@testset "Legolas.lift" begin
    @test ismissing(Legolas.lift(sin, nothing))
    @test ismissing(Legolas.lift(sin, missing))
    @test Legolas.lift(sin, 1.0) == sin(1.0)
end

@testset "Legolas.gather" begin
    a = [(x=1, y="a", z="k"),
         (x=2, y="b", z="j"),
         (x=4, y="c", z="i"),
         (x=4, y="d", z="h"),
         (x=2, y="e", z="g"),
         (x=5, y="f", z="f"),
         (x=4, y="g", z="e"),
         (x=3, y="h", z="d"),
         (x=1, y="i", z="c"),
         (x=5, y="j", z="b"),
         (x=4, y="k", z="a")]
    b = [(x=1, m=1),
         (x=2, m=2),
         (x=2, m=5),
         (x=5, m=4),
         (x=4, m=6)]
    c = [(test="a", x=1, z=1.0),
         (test="b", x=2, z=1.0),
         (test="d", x=4, z=1.0),
         (test="e", x="gotcha", z=1.0),
         (test="f", x=5, z=1.0),
         (test="h", x=3, z=1.0),
         (test="i", x=1, z=1.0),
         (test="j", x=5, z=1.0),
         (test="k", x=4, z=1.0)]
    dfa, dfb, dfc = DataFrame(a), DataFrame(b), DataFrame(c)
    g = Legolas.gather(:x, a, b, c; extract=(t, i) -> t[i])
    dfg = Legolas.gather(:x, dfa, dfb, dfc)
    expected = Dict(1 => ([(x=1, y="a", z="k"), (x=1, y="i", z="c")],
                          [(x=1, m=1)],
                          [(test="a", x=1, z=1.0), (test="i", x=1, z=1.0)]),
                    2 => ([(x=2, y="b", z="j"), (x=2, y="e", z="g")],
                          [(x=2, m=2), (x=2, m=5)],
                          [(test="b", x=2, z=1.0)]),
                    3 => ([(x=3, y="h", z="d")],
                          NamedTuple{(:x, :m),Tuple{Int64,Int64}}[],
                          [(test="h", x=3, z=1.0)]),
                    4 => ([(x=4, y="c", z="i"), (x=4, y="d", z="h"), (x=4, y="g", z="e"), (x=4, y="k", z="a")],
                          [(x=4, m=6)],
                          [(test="d", x=4, z=1.0), (test="k", x=4, z=1.0)]),
                    5 => ([(x=5, y="f", z="f"), (x=5, y="j", z="b")],
                          [(x=5, m=4)],
                          [(test="f", x=5, z=1.0), (test="j", x=5, z=1.0)]),
                    "gotcha" => (NamedTuple{(:x, :y, :z),NTuple{3,Any}}[],
                                 NamedTuple{(:x, :m),NTuple{2,Any}}[],
                                 [(test="e", x="gotcha", z=1.0)]))
    @test g == expected
    @test keys(dfg) == keys(expected)
    @test all(all(dfg[k] .== DataFrame.(expected[k])) for k in keys(dfg))

    # test both the fast path + fallback path for Legolas._iterator_for_column
    @test Legolas._iterator_for_column(dfa, :x) === dfa.x
    @test Legolas._iterator_for_column(a, :x) == dfa.x
end

@testset "Legolas.assign_to_table_metadata!" begin
    struct Foo
        meta
    end
    Legolas.Arrow.getmetadata(foo::Foo) = foo.meta
    foo = Foo(Dict("a" => "b", "b" => "b"))
    @test foo.meta === Legolas.assign_to_table_metadata!(foo, ("b" => "c", "d" => "e"))
    @test foo.meta == Dict("a" => "b", "b" => "c", "d" => "e")
end

@testset "miscellaneous Legolas.Schema / Legolas.Row tests" begin
    @test_throws ArgumentError("argument is not a valid `Legolas.Schema` name: \"bad_name?\"") Schema("bad_name?", 1)
    @test_throws ArgumentError("argument is not a valid `Legolas.Schema` string: \"bad_name>?@1\"") Schema("bad_name>?@1")

    @row("foo@1", x, y)
    @row("bar@1" > "foo@1", z)
    @test Legolas.schema_parent(Schema("bar", 1)) == Schema("foo", 1)

    r = Row(Schema("bar", 1), (x=1, y=2, z=3))

    @test propertynames(r) == (:z, :x, :y)
    @test r === Row(Schema("bar", 1), r)
    @test r === Row(Schema("bar", 1); x=1, y=2, z=3)
    @test r === Row(Schema("bar", 1), first(Tables.rows(Arrow.Table(Arrow.tobuffer((x=[1],y=[2],z=[3]))))))
    @test r[1] === 3
    @test string(r) == "Row(Schema(\"bar@1\"), (z = 3, x = 1, y = 2))"
end

@testset "generic path types" begin
    struct MyPath
        x::String
    end
    Base.read(p::MyPath) = Base.read(p.x)
    Base.write(p::MyPath, bytes) = Base.write(p.x, bytes)
    root = mktempdir()
    path = MyPath(joinpath(root, "baz.arrow"))
    Baz = @row("baz@1", a, b)
    t = [Baz(a=1, b=2), Baz(a=3, b=4)]
    Legolas.write(path, t, Schema("baz", 1))
    @test t == Baz.(Tables.rows(Legolas.read(path)))
end