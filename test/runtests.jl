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

@testset "Legolas.Schema + Legolas.parse_schema_identifier" begin
    good_schema_names = ("foo", "test.foo", "test.foo-bar", ".-technically-allowed-.")
    good_versions = (0, 1, 2, 3)
    bad_schema_names = ("has_underscore", "caPitaLs",  "has a space", "illegal?chars*")
    bad_versions = (-1, -2, -3)

    for n in good_schema_names, v in good_versions
        @test Legolas.parse_schema_identifier("$n@$v") == [Schema(n, v)]
        @test Schema(n, v) == Schema{Symbol(n),v}()
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

@schema("test.parent@1", x, y::AbstractString)
@alias("test.parent", Parent)
@schema("test.child@1 > test.parent@1", z)
@alias("test.child", Child)

@testset "Legolas.@schema" begin
    @test_throws SchemaDeclarationError("no required fields declared") @schema("name@1")
    id = "name@1"
    @test_throws SchemaDeclarationError("schema identifier must be a string literal") @schema(id, x)
    @test_throws SchemaDeclarationError("schema identifier should specify at most one parent, found multiple: " *
                                        "Schema[Schema(\"bob\", 1), Schema(\"dave\", 1), Schema(\"joe\", 1)]") @schema("bob@1 > dave@1 > joe@1", x)
    @test_throws SchemaDeclarationError("cannot have duplicate field names in `@schema` declaration; recieved: [:x, :y, :x, :z]") @schema("name@1", x, y, x, z)
    @test_throws SchemaDeclarationError("parent schema cannot be used before it has been declared: Schema(\"test.parent\", 2)") @schema("test.child@2 > test.parent@2", x)
    @test_throws SchemaDeclarationError("declared field types violate parent schema's field types") @schema("new@1 > test.parent@1", y::Int)
    @test_throws SchemaDeclarationError("invalid redeclaration of existing schema; all `@schema` redeclarations must exactly match previous declarations") @schema("test.parent@1", x, y)
end


    # @test
    # @test Legolas.schema_parent(Schema("bar", 1)) == Schema("foo", 1)

    # r = apply(Schema("bar", 1), (x=1, y=2, z=3))

    # @test propertynames(r) == (:z, :x, :y)
    # @test r === apply(Schema("bar", 1), r)
    # @test r === apply(Schema("bar", 1); x=1, y=2, z=3)
    # @test r === apply(Schema("bar", 1), first(Tables.rows(Arrow.Table(Arrow.tobuffer((x=[1],y=[2],z=[3]))))))

    # tbl = Arrow.Table(Arrow.tobuffer((x=[r],)))
    # @test r === tbl.x[1]

    # long_row = apply(Schema("bar", 1), (x=1, y=2, z=zeros(100, 100)))
    # @test length(sprint(show, long_row; context=(:limit => true))) < 200

    # @test_throws Legolas.UnknownSchemaError Legolas.apply(Legolas.Schema("imadethisup@3"); a=1, b=2)
    # @test_throws Legolas.UnknownSchemaError Legolas.validate(Tables.Schema((:a, :b), (Int, Int)), Legolas.Schema("imadethisup@3"))
    # @test_throws Legolas.UnknownSchemaError Legolas.schema_identifier(Legolas.Schema("imadethisup@3"))

    # sch = Schema("bar", 1)
    # @test Schema(sch) == sch

    # schemas = [Schema("bar", 1), Schema("foo", 1)]
    # tbl = Arrow.Table(Arrow.tobuffer((; schema=schemas)))
    # @test all(tbl.schema .== schemas)
# end


#=

########################
########################
########################


@testset "miscellaneous Legolas/src/tables.jl tests" begin
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
    tbl = Arrow.Table(Legolas.tobuffer(t, Schema("baz", 1); metadata=("a" => "b", "c" => "d")))
    @test Set(Arrow.getmetadata(tbl)) == Set((Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "baz@1",
                                              "a" => "b", "c" => "d"))

    struct Foo
        meta
    end
    Legolas.Arrow.getmetadata(foo::Foo) = foo.meta
    foo = Foo(Dict("a" => "b", "b" => "b",
                   Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "baz@1"))
    @test Legolas.Schema("baz", 1) == Legolas.extract_legolas_schema(foo)

    t = [(a="a", c=1, b="b"), Baz(a=1, b=2)] # not a valid Tables.jl table
    @test_throws ErrorException Legolas.validate(t, Schema("baz", 1))

    t = Arrow.tobuffer((a=[1, 2], b=[3, 4]); metadata=Dict(Legolas.LEGOLAS_SCHEMA_QUALIFIED_METADATA_KEY => "haha@3"))
    @test_throws Legolas.UnknownSchemaError Legolas.read(t)
end


@testset "miscellaneous Legolas.Schema tests" begin
    @test_throws ArgumentError("`Legolas.Schema` version must be non-negative, received: -1") Schema("good_name", -1)
    @test_throws ArgumentError("argument is not a valid `Legolas.Schema` name: \"bad_name?\"") Schema("bad_name?", 1)
    @test_throws ArgumentError("argument is not a valid `Legolas.Schema` string: \"bad_name>?@1\"") Schema("bad_name>?@1")

    @schema("foo@1", x, y)
    @schema("bar@1" > "foo@1", z)
    @test Legolas.schema_parent(Schema("bar", 1)) == Schema("foo", 1)

    r = apply(Schema("bar", 1), (x=1, y=2, z=3))

    @test propertynames(r) == (:z, :x, :y)
    @test r === apply(Schema("bar", 1), r)
    @test r === apply(Schema("bar", 1); x=1, y=2, z=3)
    @test r === apply(Schema("bar", 1), first(Tables.rows(Arrow.Table(Arrow.tobuffer((x=[1],y=[2],z=[3]))))))

    tbl = Arrow.Table(Arrow.tobuffer((x=[r],)))
    @test r === tbl.x[1]

    long_row = apply(Schema("bar", 1), (x=1, y=2, z=zeros(100, 100)))
    @test length(sprint(show, long_row; context=(:limit => true))) < 200

    @test_throws Legolas.UnknownSchemaError Legolas.apply(Legolas.Schema("imadethisup@3"); a=1, b=2)
    @test_throws Legolas.UnknownSchemaError Legolas.validate(Tables.Schema((:a, :b), (Int, Int)), Legolas.Schema("imadethisup@3"))
    @test_throws Legolas.UnknownSchemaError Legolas.schema_identifier(Legolas.Schema("imadethisup@3"))

    sch = Schema("bar", 1)
    @test Schema(sch) == sch

    schemas = [Schema("bar", 1), Schema("foo", 1)]
    tbl = Arrow.Table(Arrow.tobuffer((; schema=schemas)))
    @test all(tbl.schema .== schemas)
end

@testset "schema field name and type tests" begin
    Parent = @row("parent@1",
                  first_parent_field::Int=1,
                  second_parent_field::String="second")

    parent_fields = (:first_parent_field, :second_parent_field)
    parent_field_types = (Int, String)

    @test Legolas.schema_field_names(Schema{:parent,1}) == parent_fields
    @test Legolas.schema_field_names(Schema("parent@1")) == parent_fields
    @test Legolas.schema_field_names(Parent()) == parent_fields
    @test Legolas.schema_field_names(Parent) == parent_fields

    @test Legolas.schema_field_types(Schema{:parent,1}) == parent_field_types
    @test Legolas.schema_field_types(Schema("parent@1")) == parent_field_types
    @test Legolas.schema_field_types(Parent()) == parent_field_types
    @test Legolas.schema_field_types(Parent) == parent_field_types

    Child = @row("child@1" > "parent@1",
                 first_child_field::Symbol=:first,
                 second_child_field="I can be anything")

    child_fields = (:first_child_field, :second_child_field, parent_fields...)
    child_field_types = (Symbol, Any, parent_field_types...)

    @test Legolas.schema_field_names(Schema{:child,1}) == child_fields
    @test Legolas.schema_field_names(Schema("child@1")) == child_fields
    @test Legolas.schema_field_names(Child()) == child_fields
    @test Legolas.schema_field_names(Child) == child_fields

    @test Legolas.schema_field_types(Schema{:child,1}) == child_field_types
    @test Legolas.schema_field_types(Schema("child@1")) == child_field_types
    @test Legolas.schema_field_types(Child()) == child_field_types
    @test Legolas.schema_field_types(Child) == child_field_types

    @test_throws MethodError Legolas.schema_field_names(Legolas.Schema("imadethisup@3"))
    @test_throws MethodError Legolas.schema_field_types(Legolas.Schema("imadethisup@3"))
end

@testset "isequal, hash" begin
    TestRow = @row("testrow@1", x, y)

    foo = TestRow(; x=[1])
    foo2 = TestRow(; x=[1])
    @test isequal(foo, foo2)
    @test hash(foo) == hash(foo2)

    foo3 = TestRow(; x=[3])
    @test !isequal(foo, foo3)
    @test hash(foo) != hash(foo3)
end

@schema("my-inner-schema@1", b::Int=1)
@schema("my-outer-schema@1",
        a::String,
        x::NamedTuple=Legolas.row(Legolas.Schema("my-inner-schema@1"), x))

@testset "Nested arrow serialization" begin
    table = [(; a="outer_a", x=(; b=1))]
    roundtripped_table = Legolas.read(Legolas.tobuffer(table, Legolas.Schema("my-outer-schema@1")))
    @test table == (r -> Legolas.row(Legolas.Schema("my-outer-schema@1"), r)).(Tables.rows(roundtripped_table))
end
=#
