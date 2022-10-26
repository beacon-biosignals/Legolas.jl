using Legolas, Test, DataFrames, Arrow, UUIDs
using Legolas: SchemaVersion, @schema, @version, SchemaVersionDeclarationError, RequiredFieldInfo

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

bad_id_message(x) = "failed to parse seemingly invalid/malformed schema version identifier string: \"$x\""
bad_name_message(x) = "argument is not a valid `Legolas.SchemaVersion` name: \"$x\""
bad_version_message(x) = "`version` in `SchemaVersion{_,version}` must be a non-negative integer, received: `($x)::$(typeof(x))`"

function bad_version_declaration_message(id, err)
    return "Error encountered attempting to parse schema version identifier.\n" *
           "Received: \"$id\"\n" *
           "Encountered: ArgumentError: " * err
end

@testset "Legolas.parse_identifier and related code" begin
    good_schema_names = ("foo", "test.foo", "test.foo-bar", ".-technically-allowed-.")
    good_versions = (0, 1, 2, 3)
    bad_schema_names = ("has_underscore", "caPitaLs",  "has a space", "illegal?chars*")
    bad_versions = (-1, -2, -3)

    for n in good_schema_names, v in good_versions
        @test Legolas.parse_identifier("$n@$v") == [SchemaVersion(n, v)]
        @test SchemaVersion(n, v) == SchemaVersion{Symbol(n),v}()
        @test Legolas.name(SchemaVersion(n, v)) == Symbol(n)
        @test Legolas.version(SchemaVersion(n, v)) == v
    end

    for n in good_schema_names, v in bad_versions
        @test_throws ArgumentError(bad_version_message(v)) SchemaVersion(n, v)
        id = "$n@$v"
        @test_throws ArgumentError(bad_version_message(v)) Legolas.parse_identifier(id)
        @test_throws SchemaVersionDeclarationError(bad_version_declaration_message(id, bad_version_message(v))) @eval(@version($id, begin x end))
    end

    for n in bad_schema_names, v in Iterators.flatten((bad_versions, good_versions))
        @test_throws ArgumentError(bad_name_message(n)) SchemaVersion(n, v)
        id = "$n@$v"
        @test_throws ArgumentError(bad_name_message(n)) Legolas.parse_identifier(id)
        @test_throws SchemaVersionDeclarationError(bad_version_declaration_message(id, bad_name_message(n))) @eval(@version($id, begin x end))
    end

    for n in good_schema_names, m in good_schema_names
        @test Legolas.parse_identifier("$n@3>$m@2") == [SchemaVersion(n, 3), SchemaVersion(m, 2)]
        @test Legolas.parse_identifier("$n@1>$m@0>bob@3") == [SchemaVersion(n, 1), SchemaVersion(m, 0), SchemaVersion("bob", 3)]
        @test Legolas.parse_identifier("$n@1 >$m@0 > bob@3 ") == [SchemaVersion(n, 1), SchemaVersion(m, 0), SchemaVersion("bob", 3)]
        id = "$n@1 >$m @0 > bob@3 "
        @test_throws ArgumentError(bad_name_message("$m ")) Legolas.parse_identifier(id)
        @test_throws SchemaVersionDeclarationError(bad_version_declaration_message(id, bad_name_message("$m "))) @eval(@version($id, begin x end))
        for bad_id in ("$n>$m@1",
                       "$n@1>$m",
                       "$n@>$m@",
                       "$n>$m@",
                       "$n@>$m",
                       "$n>$m")
            @test_throws ArgumentError(bad_id_message(bad_id)) Legolas.parse_identifier(bad_id)
            @test_throws SchemaVersionDeclarationError(bad_version_declaration_message(bad_id, bad_id_message(bad_id))) @eval(@version($bad_id, begin x end))
        end
        for bad_separator in ("<", "<:", ":", "=")
            id = "$n@1" * bad_separator * "$m@0"
            @test_throws ArgumentError(bad_id_message(id)) Legolas.parse_identifier(id)
            @test_throws SchemaVersionDeclarationError(bad_version_declaration_message(id, bad_id_message(id))) @eval(@version($id, begin x end))
        end
    end

    for good in good_schema_names, bad in bad_schema_names
        for id in ("$good@3>$bad@2",
                   "$bad@1>bob@0>$good@3",
                   "bob@1>$bad@0>$good@3",
                   "$good@1>bob@0>$bad@3")
            @test_throws ArgumentError(bad_name_message(bad)) Legolas.parse_identifier(id)
            @test_throws SchemaVersionDeclarationError(bad_version_declaration_message(id, bad_name_message(bad))) @eval(@version($id, begin x end))
        end
    end
end

@testset "Legolas.@schema" begin
    @test_throws ArgumentError("`name` provided to `@schema` must be a string literal") @schema(joe, J)
    @test_throws ArgumentError("`name` provided to `@schema` should not include an `@` version clause") @schema("joe@1", J)
    @test_throws ArgumentError("`name` provided to `@schema` is not a valid `Legolas.SchemaVersion` name: \"joe?\"") @schema("joe?", J)
    @test_throws ArgumentError("`Prefix` provided to `@schema` is not a valid type name: J{Int}") @schema("joo", J{Int})
end

@schema "test.parent" Parent
@version "test.parent@1" begin
    x::Vector
    y::AbstractString
end

@schema "test.child" Child
@version "test.child@1 > test.parent@1" begin
    z
end

@schema "test.grandchild" Grandchild
@version "test.grandchild@1 > test.child@1" begin
    a::Int32 = round(Int32, a)
    y::String = string(y[1:2])
end

@schema "test.nested" Nested
@version "test.nested@1" begin
    gc::GrandchildV1
    k::String
end

# This statement will induce an error if field types are not properly escaped,
# since `DataFrame` will be hygeine-passed to `Legolas.DataFrame`, which is undefined
@schema "test.field-type-escape" FieldTypeEscape
@version "test.field-type-escape@1" begin
    x::DataFrame
end

@schema "test.accepted" Accepted
@version "test.accepted@1" begin
    id::UUID
    sym::Symbol
end

@schema "test.new" New

@testset "`Legolas.@version` and associated utilities for declared `Legolas.SchemaVersion`s" begin
    @testset "Legolas.SchemaVersionDeclarationError" begin
        @test_throws SchemaVersionDeclarationError("malformed or missing declaration of required fields") @version("name@1", begin end)
        @test_throws SchemaVersionDeclarationError("schema version identifier must be a string literal") @version(id, begin x end)
        @test_throws SchemaVersionDeclarationError("schema version identifier should specify at most one parent, found multiple: " *
                                                   "SchemaVersion[SchemaVersion(\"bob\", 1), SchemaVersion(\"dave\", 1), SchemaVersion(\"joe\", 1)]") @version("bob@1 > dave@1 > joe@1", begin x end)
        @test_throws SchemaVersionDeclarationError("cannot have duplicate field names in `@version` declaration; recieved: $([:x, :y, :x, :z])") @version("name@1", begin x; y; x; z end)
        @test_throws SchemaVersionDeclarationError("parent schema version cannot be used before it has been declared: SchemaVersion(\"test.parent\", 2)") @version("test.child@2 > test.parent@2", begin x end)

        @test_throws SchemaVersionDeclarationError("`new` must be declared via `@schema` before its initial `@version` declaration") @version("new@1 > test.parent@1", begin y::Int end)
        @test_throws SchemaVersionDeclarationError("declared field types violate parent's field types") @version("test.new@1 > test.parent@1", begin y::Int end)
        @test_throws SchemaVersionDeclarationError("declared field types violate parent's field types") @version("test.new@1 > test.child@1", begin y::Int end)
        @test_throws SchemaVersionDeclarationError("invalid redeclaration of existing schema version; all `@version` redeclarations must exactly match previous declarations") @version("test.parent@1", begin x; y end)
        @test_throws SchemaVersionDeclarationError("malformed `@version` field expression: f()") @version("test.child@2", begin f() end)
    end

    undeclared = SchemaVersion("undeclared", 3)

    @testset "Legolas.declared" begin
        @test !Legolas.declared(undeclared)
        @test all(Legolas.declared, (ParentSchemaV1(), ChildSchemaV1(), GrandchildSchemaV1()))
    end

    @testset "Legolas.parent" begin
        @test isnothing(Legolas.parent(undeclared))
        @test isnothing(Legolas.parent(ParentSchemaV1()))
        @test Legolas.parent(ChildSchemaV1()) == ParentSchemaV1()
        @test Legolas.parent(GrandchildSchemaV1()) == ChildSchemaV1()
    end

    @testset "Legolas.identifier" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.identifier(undeclared)
        @test Legolas.identifier(ParentSchemaV1()) == "test.parent@1"
        @test Legolas.identifier(ChildSchemaV1()) == "test.child@1>test.parent@1"
        @test Legolas.identifier(GrandchildSchemaV1()) == "test.grandchild@1>test.child@1>test.parent@1"
    end

    @testset "Legolas.required_fields" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.required_fields(undeclared)
        @test Legolas.required_fields(ParentSchemaV1()) == (x=Vector, y=AbstractString)
        @test Legolas.required_fields(ChildSchemaV1()) == (x=Vector, y=AbstractString, z=Any)
        @test Legolas.required_fields(GrandchildSchemaV1()) == (x=Vector, y=String, z=Any, a=Int32)
    end

    @testset "Legolas.find_violation + Legolas.complies_with + Legolas.validate" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.validate(Tables.Schema((:a, :b), (Int, Int)), undeclared)
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.complies_with(Tables.Schema((:a, :b), (Int, Int)), undeclared)
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.find_violation(Tables.Schema((:a, :b), (Int, Int)), undeclared)

        # Note that many of the basic properties of `find_violation`/`complies_with`/`validate`
        # are unit-tested in `examples/tour.jl`; thus, we focus here on testing that these
        # functions work as expected w.r.t. schema extension in particular.

        t = Tables.Schema((:a, :y, :z), (Int32, String, Any))
        for s in (GrandchildSchemaV1(), ChildSchemaV1(), ParentSchemaV1())
            @test_throws ArgumentError("could not find expected field `x` in $t") Legolas.validate(t, s)
            @test !Legolas.complies_with(t, s)
            @test isequal(Legolas.find_violation(t, s), :x => missing)
        end

        t = Tables.Schema((:x, :a, :y), (ComplexF64, Int32, String))
        for s in (GrandchildSchemaV1(), ChildSchemaV1(), ParentSchemaV1())
            @test_throws ArgumentError("field `x` has unexpected type; expected <:$(Vector), found $(Complex{Float64})") Legolas.validate(t, s)
            @test !Legolas.complies_with(t, s)
            @test isequal(Legolas.find_violation(t, s), :x => ComplexF64)
        end

        t = Tables.Schema((:x, :a, :y), (Vector, Int32, String))
        for s in (GrandchildSchemaV1(), ChildSchemaV1(), ParentSchemaV1())
            @test isnothing(Legolas.validate(t, s))
            @test Legolas.complies_with(t, s)
            @test isnothing(Legolas.find_violation(t, s))
        end

        for T in (UUID, UInt128), S in (Symbol, String)
            @test Legolas.complies_with(Tables.Schema((:id, :sym), (T, S)), AcceptedSchemaV1())
        end
    end

    @testset "Legolas.declaration" begin
        @test_throws Legolas.UnknownSchemaVersionError(undeclared) Legolas.declaration(undeclared)
        @test Legolas.declaration(ParentSchemaV1()) == ("test.parent@1" => [RequiredFieldInfo(:x, :Vector, false, :(x::Vector = x)),
                                                                            RequiredFieldInfo(:y, :AbstractString, false, :(y::AbstractString = y))])
        @test Legolas.declaration(ChildSchemaV1()) == ("test.child@1>test.parent@1" => [RequiredFieldInfo(:z, :Any, false, :(z::Any = z))])
        @test Legolas.declaration(GrandchildSchemaV1()) == ("test.grandchild@1>test.child@1" => [RequiredFieldInfo(:a, :Int32, false, :(a::Int32 = round(Int32, a))),
                                                                                                 RequiredFieldInfo(:y, :String, false, :(y::String = string(y[1:2])))])
    end

    r0 = (x=[42], y="foo", z=:three, a=1.3)
    r0_arrow = first(Tables.rows(Arrow.Table(Arrow.tobuffer([r0]))))

    @test NamedTuple(ParentV1(r0)) == (; r0.x, r0.y)
    @test ParentV1(r0) == ParentV1(; r0.x, r0.y)
    @test ParentV1(r0) == ParentV1(r0_arrow)

    @test NamedTuple(ChildV1(r0)) == (; r0.x, r0.y, r0.z)
    @test ChildV1(r0) == ChildV1(; r0.x, r0.y, r0.z)
    @test ChildV1(r0) == ChildV1(r0_arrow)

    @test NamedTuple(GrandchildV1(r0)) == (x=[42], y="fo", z=:three, a=1)
    @test GrandchildV1(r0) == GrandchildV1(; r0.x, r0.y, r0.z, r0.a)
    @test GrandchildV1(r0) == GrandchildV1(r0_arrow)

    tbl = Arrow.Table(Arrow.tobuffer((; x=[ParentV1(r0)])))
    @test tbl.x[1] == ParentV1(Tables.rowmerge(r0))

    # Note that Arrow.jl roundtrips z=:three to z="three", since
    # `z::Symbol` isn't evident from these record types
    r0_roundtripped = Tables.rowmerge(r0; z="three")

    tbl = Arrow.Table(Arrow.tobuffer((; x=[ChildV1(r0)])))
    @test tbl.x[1] == ChildV1(r0_roundtripped)

    tbl = Arrow.Table(Arrow.tobuffer((; x=[GrandchildV1(r0)])))
    @test tbl.x[1] == GrandchildV1(r0_roundtripped)

    svs = [GrandchildSchemaV1(), ChildSchemaV1(), ParentSchemaV1()]
    tbl = Arrow.Table(Arrow.tobuffer((; sv=svs)))
    @test all(tbl.sv .== svs)

    tbl = [NestedV1(; gc=GrandchildV1(r0), k="test")]
    roundtripped = Legolas.read(Legolas.tobuffer(tbl, NestedSchemaV1()))
    @test roundtripped.gc[1] == GrandchildV1(r0_roundtripped)
    @test roundtripped.k[1] == "test"
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
    Legolas.write(path, t, ParentSchemaV1())
    @test t == [NamedTuple(ParentV1(r)) for r in Tables.rows(Legolas.read(path))]
    tbl = Arrow.Table(Legolas.tobuffer(t, ParentSchemaV1(); metadata=("a" => "b", "c" => "d")))
    @test Set(Arrow.getmetadata(tbl)) == Set((Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "test.parent@1",
                                              "a" => "b", "c" => "d"))

    struct Moo
        meta
    end
    Legolas.Arrow.getmetadata(moo::Moo) = moo.meta
    moo = Moo(Dict("a" => "b", "b" => "b", Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "test.parent@1"))
    @test ParentSchemaV1() == Legolas.extract_schema_version(moo)

    t = Arrow.tobuffer((a=[1, 2], b=[3, 4]); metadata=Dict(Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "haha@3"))
    @test_throws Legolas.UnknownSchemaVersionError Legolas.read(t)
end
