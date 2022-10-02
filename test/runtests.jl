using Legolas, Test, DataFrames, Arrow

using Legolas: Schema, @schema, @alias, row, SchemaDeclarationError

include(joinpath(dirname(@__DIR__), "examples", "tour.jl"))

@testset "Legolas.lift" begin
    @test ismissing(Legolas.lift(sin, nothing))
    @test ismissing(Legolas.lift(sin, missing))
    @test Legolas.lift(sin, 1.0) == sin(1.0)
    @test Legolas.lift(Some, Some(1)) == Some(Some(1))

    @test ismissing(Legolas.lift(sin)(nothing))
    @test ismissing(Legolas.lift(sin)(missing))
    @test Legolas.lift(sin)(1.0) == sin(1.0)
    @test Legolas.lift(Some)(Some(1)) == Some(Some(1))
end

@testset "Legolas.construct" begin
    @test Legolas.construct(Int, 0x00) === 0
    @test Legolas.construct(Some, Some(1)) === Some(1)

    @test Legolas.construct(Int)(0x00) === 0
    @test Legolas.construct(Some)(Some(1)) === Some(1)

    # Restrict `construct` to types only
    @test_throws MethodError Legolas.construct(sin, 1.0)
    @test_throws MethodError Legolas.construct(sin)

    @testset "undefined identity constructor" begin
        mutable struct PR45
            x::Int
        end
        Foo = PR45  # Alias to unique struct name

        x = Foo(1)
        @test x !== Foo(1)
        @test_throws MethodError Foo(x)  # Type does not define an identity constructor
        @test Legolas.construct(Foo, x) === x
    end
end

@testset "Legolas.location" begin
    collections = (['a', 'b', 'c', 'f', 'b'],
                   ['d', 'c', 'e', 'b'],
                   ['f', 'a', 'f'])
    expected = Dict('f' => ([4], [], [1, 3]),
                    'a' => ([1], [], [2]),
                    'c' => ([3], [2], []),
                    'd' => ([], [1], []),
                    'e' => ([], [3], []),
                    'b' => ([2, 5], [4], []))
    @test Legolas.locations(collections) == expected
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

bad_id_message(x) = "failed to parse seemingly invalid/malformed schema identifier string: \"$x\""
bad_name_message(x) = "argument is not a valid `Legolas.Schema` name: \"$x\""
bad_version_message(x) = "`version` in `Schema{_,version}` must be a non-negative integer, received: $x"

function bad_schema_declaration_message(id, err)
    return "Error encountered attempting to parse schema identifier.\n" *
           "Received: \"$id\"\n" *
           "Encountered: ArgumentError: " * err
end

@testset "Legolas.parse_schema_identifier and related code" begin
    good_schema_names = ("foo", "test.foo", "test.foo-bar", ".-technically-allowed-.")
    good_versions = (0, 1, 2, 3)
    bad_schema_names = ("has_underscore", "caPitaLs",  "has a space", "illegal?chars*")
    bad_versions = (-1, -2, -3)

    for n in good_schema_names, v in good_versions
        @test Legolas.parse_schema_identifier("$n@$v") == [Schema(n, v)]
        @test Schema(n, v) == Schema{Symbol(n),v}()
        @test Legolas.schema_name(Schema(n, v)) == Symbol(n)
        @test Legolas.schema_version(Schema(n, v)) == v
    end

    for n in good_schema_names, v in bad_versions
        @test_throws ArgumentError(bad_version_message(v)) Schema(n, v)
        id = "$n@$v"
        @test_throws ArgumentError(bad_version_message(v)) Legolas.parse_schema_identifier(id)
        @test_throws SchemaDeclarationError(bad_schema_declaration_message(id, bad_version_message(v))) @eval(@schema($id))
    end

    for n in bad_schema_names, v in Iterators.flatten((bad_versions, good_versions))
        @test_throws ArgumentError(bad_name_message(n)) Schema(n, v)
        id = "$n@$v"
        @test_throws ArgumentError(bad_name_message(n)) Legolas.parse_schema_identifier(id)
        @test_throws SchemaDeclarationError(bad_schema_declaration_message(id, bad_name_message(n))) @eval(@schema($id))
    end

    for n in good_schema_names, m in good_schema_names
        @test Legolas.parse_schema_identifier("$n@3>$m@2") == [Schema(n, 3), Schema(m, 2)]
        @test Legolas.parse_schema_identifier("$n@1>$m@0>bob@3") == [Schema(n, 1), Schema(m, 0), Schema("bob", 3)]
        @test Legolas.parse_schema_identifier("$n@1 >$m@0 > bob@3 ") == [Schema(n, 1), Schema(m, 0), Schema("bob", 3)]
        id = "$n@1 >$m @0 > bob@3 "
        @test_throws ArgumentError(bad_name_message("$m ")) Legolas.parse_schema_identifier(id)
        @test_throws SchemaDeclarationError(bad_schema_declaration_message(id, bad_name_message("$m "))) @eval(@schema($id))
        for bad_id in ("$n>$m@1",
                       "$n@1>$m",
                       "$n@>$m@",
                       "$n>$m@",
                       "$n@>$m",
                       "$n>$m")
            @test_throws ArgumentError(bad_id_message(bad_id)) Legolas.parse_schema_identifier(bad_id)
            @test_throws SchemaDeclarationError(bad_schema_declaration_message(bad_id, bad_id_message(bad_id))) @eval(@schema($bad_id))
        end
        for bad_separator in ("<", "<:", ":", "=")
            id = "$n@1" * bad_separator * "$m@0"
            @test_throws ArgumentError(bad_id_message(id)) Legolas.parse_schema_identifier(id)
            @test_throws SchemaDeclarationError(bad_schema_declaration_message(id, bad_id_message(id))) @eval(@schema($id))
        end
    end

    for good in good_schema_names, bad in bad_schema_names
        for id in ("$good@3>$bad@2",
                   "$bad@1>bob@0>$good@3",
                   "bob@1>$bad@0>$good@3",
                   "$good@1>bob@0>$bad@3")
            @test_throws ArgumentError(bad_name_message(bad)) Legolas.parse_schema_identifier(id)
            @test_throws SchemaDeclarationError(bad_schema_declaration_message(id, bad_name_message(bad))) @eval(@schema($id))
        end
    end
end

@alias("bob", B)

@testset "Legolas.@alias" begin
    @test B{3} == Schema{:bob,3}
    @test B(3) == Schema{:bob,3}()
    @test_throws ErrorException("invocations of `B` must specify a schema version integer; instead of `B()`, "*
                                "try `B(v)` where `v` is an appropriate schema version integer") B()
    @test_throws ErrorException("invalid redefinition of constant B") @eval(@alias("joe", B))
    joe = "joe"
    @test_throws ArgumentError("`schema_name` provided to `@alias` must be a string literal") @alias(joe, J)
    @test_throws ArgumentError("`schema_name` provided to `@alias` should not include an `@` version clause") @alias("joe@1", J)
    @test_throws ArgumentError("`schema_name` provided to `@alias` is not a valid `Legolas.Schema` name: \"joe?\"") @alias("joe?", J)
end

@schema("test.parent@1", x::Vector, y::AbstractString)
@alias("test.parent", Parent)

@schema("test.child@1 > test.parent@1", z)
@alias("test.child", Child)

@schema("test.grandchild@1 > test.child@1", a::Int32 = round(Int32, a), y::String = string(y[1:2]))
@alias("test.grandchild", Grandchild)

@testset "`Legolas.@schema` and associated utilities for declared `Legolas.Schema`s" begin
    @testset "Legolas.SchemaDeclarationError" begin
        @test_throws SchemaDeclarationError("no required fields declared") @schema("name@1")
        @test_throws SchemaDeclarationError("schema identifier must be a string literal") @schema(id, x)
        @test_throws SchemaDeclarationError("schema identifier should specify at most one parent, found multiple: " *
                                            "Schema[Schema(\"bob\", 1), Schema(\"dave\", 1), Schema(\"joe\", 1)]") @schema("bob@1 > dave@1 > joe@1", x)
        @test_throws SchemaDeclarationError("cannot have duplicate field names in `@schema` declaration; recieved: [:x, :y, :x, :z]") @schema("name@1", x, y, x, z)
        @test_throws SchemaDeclarationError("parent schema cannot be used before it has been declared: Schema(\"test.parent\", 2)") @schema("test.child@2 > test.parent@2", x)
        @test_throws SchemaDeclarationError("declared field types violate parent schema's field types") @schema("new@1 > test.parent@1", y::Int)
        @test_throws SchemaDeclarationError("declared field types violate parent schema's field types") @schema("new@1 > test.child@1", y::Int)
        @test_throws SchemaDeclarationError("invalid redeclaration of existing schema; all `@schema` redeclarations must exactly match previous declarations") @schema("test.parent@1", x, y)
        @test_throws SchemaDeclarationError("malformed `@schema` field expression: f()") @schema("test.child@2", f())
    end

    undeclared = Schema("undeclared", 3)

    @testset "Legolas.schema_declared" begin
        @test !Legolas.schema_declared(undeclared)
        @test !Legolas.schema_declared(Child(3))
        @test all(Legolas.schema_declared, (Parent(1), Child(1), Grandchild(1)))
    end

    @testset "Legolas.schema_parent" begin
        @test isnothing(Legolas.schema_parent(undeclared))
        @test isnothing(Legolas.schema_parent(Child(3)))
        @test Legolas.schema_parent(Child(1)) == Parent(1)
        @test Legolas.schema_parent(Grandchild(1)) == Child(1)
    end

    @testset "Legolas.schema_identifier" begin
        @test_throws Legolas.UnknownSchemaError(undeclared) Legolas.schema_identifier(undeclared)
        @test_throws Legolas.UnknownSchemaError(Child(3)) Legolas.schema_identifier(Child(3))
        @test Legolas.schema_identifier(Child(1)) == "test.child@1>test.parent@1"
        @test Legolas.schema_identifier(Grandchild(1)) == "test.grandchild@1>test.child@1>test.parent@1"
    end

    @testset "Legolas.schema_fields" begin
        @test_throws Legolas.UnknownSchemaError(undeclared) Legolas.schema_fields(undeclared)
        @test_throws Legolas.UnknownSchemaError(Child(3)) Legolas.schema_fields(Child(3))
        @test Legolas.schema_fields(Parent(1)) == (x=Vector, y=AbstractString)
        @test Legolas.schema_fields(Child(1)) == (x=Vector, y=AbstractString, z=Any)
        @test Legolas.schema_fields(Grandchild(1)) == (x=Vector, y=String, z=Any, a=Int32)
    end

    @testset "Legolas.find_violation + Legolas.complies_with + Legolas.validate" begin
        @test_throws Legolas.UnknownSchemaError(undeclared) Legolas.validate(Tables.Schema((:a, :b), (Int, Int)), undeclared)
        @test_throws Legolas.UnknownSchemaError(Child(3)) Legolas.validate(Tables.Schema((:a, :b), (Int, Int)), Child(3))

        @test_throws Legolas.UnknownSchemaError(undeclared) Legolas.complies_with(Tables.Schema((:a, :b), (Int, Int)), undeclared)
        @test_throws Legolas.UnknownSchemaError(Child(3)) Legolas.complies_with(Tables.Schema((:a, :b), (Int, Int)), Child(3))

        @test_throws Legolas.UnknownSchemaError(undeclared) Legolas.find_violation(Tables.Schema((:a, :b), (Int, Int)), undeclared)
        @test_throws Legolas.UnknownSchemaError(Child(3)) Legolas.find_violation(Tables.Schema((:a, :b), (Int, Int)), Child(3))

        # Note that many of the basic properties of `find_violation`/`complies_with`/`validate`
        # are unit-tested in `examples/tour.jl`; thus, we focus here on testing that these
        # functions work as expected w.r.t. schema extension in particular.

        t = Tables.Schema((:a, :y, :z), (Int32, String, Any))
        for s in (Grandchild(1), Child(1), Parent(1))
            @test_throws ArgumentError("could not find expected field `x` in $t") Legolas.validate(t, s)
            @test !Legolas.complies_with(t, s)
            @test isequal(Legolas.find_violation(t, s), :x => missing)
        end

        t = Tables.Schema((:x, :a, :y), (ComplexF64, Int32, String))
        for s in (Grandchild(1), Child(1), Parent(1))
            @test_throws ArgumentError("field `x` has unexpected type; expected <:Vector{T} where T, found ComplexF64") Legolas.validate(t, s)
            @test !Legolas.complies_with(t, s)
            @test isequal(Legolas.find_violation(t, s), :x => ComplexF64)
        end

        t = Tables.Schema((:x, :a, :y), (Vector, Int32, String))
        for s in (Grandchild(1), Child(1), Parent(1))
            @test isnothing(Legolas.validate(t, s))
            @test Legolas.complies_with(t, s)
            @test isnothing(Legolas.find_violation(t, s))
        end
    end

    @testset "Legolas.schema_declaration" begin
        @test_throws Legolas.UnknownSchemaError(undeclared) Legolas.schema_declaration(undeclared)
        @test_throws Legolas.UnknownSchemaError(Child(3)) Legolas.schema_declaration(Child(3))
        @test Legolas.schema_declaration(Parent(1)) == ("test.parent@1" => Dict(:y => :(y::AbstractString = y), :x => :(x::Vector = x)))
        @test Legolas.schema_declaration(Child(1)) == ("test.child@1>test.parent@1" => Dict(:z => :(z::Any = z)))
        @test Legolas.schema_declaration(Grandchild(1)) == ("test.grandchild@1>test.child@1" => Dict(:a => :(a::Int32 = round(Int32, a)), :y => :(y::String = string(y[1:2]))))
    end

    @test_throws Legolas.UnknownSchemaError(undeclared) row(undeclared; a=1, b=2)
    @test_throws Legolas.UnknownSchemaError(Child(3)) row(Child(3); a=1, b=2)
    r0 = (x=[42], y="foo", z=:three, a=1.3)
    r0_arrow = first(Tables.rows(Arrow.Table(Arrow.tobuffer([r0]))))
    for s in (Child(1), Parent(1))
        @test Set(pairs(row(s, r0))) == Set(pairs(r0))
        @test Set(pairs(row(s, r0_arrow))) == Set(pairs(r0))
        @test Set(pairs(row(s; r0...))) == Set(pairs(r0))
    end
    r1 = (x=[42], y="fo", z=:three, a=1)
    @test Set(pairs(row(Grandchild(1), r0))) == Set(pairs(r1))
    @test Set(pairs(row(Grandchild(1), r0_arrow))) == Set(pairs(r1))
    @test Set(pairs(row(Grandchild(1); r0...))) == Set(pairs(r1))

    schemas = [Grandchild(1), Child(1), Parent(1)]
    tbl = Arrow.Table(Arrow.tobuffer((; schema=schemas)))
    @test all(tbl.schema .== schemas)
end

@testset "miscellaneous Legolas/src/tables.jl tests" begin
    struct MyPath
        x::String
    end
    Base.read(p::MyPath) = Base.read(p.x)
    Base.write(p::MyPath, bytes) = Base.write(p.x, bytes)
    root = mktempdir()
    path = MyPath(joinpath(root, "baz.arrow"))
    t = [(x=[1,2], y="hello"), (x=[3,4], y="bye")]
    Legolas.write(path, t, Parent(1))
    @test t == [row(Parent(1), r) for r in Tables.rows(Legolas.read(path))]
    tbl = Arrow.Table(Legolas.tobuffer(t, Parent(1); metadata=("a" => "b", "c" => "d")))
    @test Set(Arrow.getmetadata(tbl)) == Set((Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "test.parent@1",
                                              "a" => "b", "c" => "d"))

    struct Moo
        meta
    end
    Legolas.Arrow.getmetadata(moo::Moo) = moo.meta
    moo = Moo(Dict("a" => "b", "b" => "b",
                   Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "test.parent@1"))
    @test Parent(1) == Legolas.extract_legolas_schema(moo)

    t = [(a="a", c=1, b="b"), (a=1, b=2)] # not a valid Tables.jl table
    @test_throws ErrorException Legolas.guess_schema(t)

    t = Arrow.tobuffer((a=[1, 2], b=[3, 4]); metadata=Dict(Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "haha@3"))
    @test_throws Legolas.UnknownSchemaError Legolas.read(t)
end
