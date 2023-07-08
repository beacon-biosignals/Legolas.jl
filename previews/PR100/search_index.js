var documenterSearchIndex = {"docs":
[{"location":"upgrade/#Upgrading-from-Legolas-v0.4-to-v0.5-1","page":"Upgrading from v0.4 to v0.5","title":"Upgrading from Legolas v0.4 to v0.5","text":"","category":"section"},{"location":"upgrade/#","page":"Upgrading from v0.4 to v0.5","title":"Upgrading from v0.4 to v0.5","text":"This guide is incomplete; please add to it if you encounter items which would help other upgraders along their journey.","category":"page"},{"location":"upgrade/#","page":"Upgrading from v0.4 to v0.5","title":"Upgrading from v0.4 to v0.5","text":"See here for a comprehensive log of changes from Legolas v0.4 to Legolas v0.5.","category":"page"},{"location":"upgrade/#Some-main-changes-to-be-aware-of-1","page":"Upgrading from v0.4 to v0.5","title":"Some main changes to be aware of","text":"","category":"section"},{"location":"upgrade/#","page":"Upgrading from v0.4 to v0.5","title":"Upgrading from v0.4 to v0.5","text":"In Legolas v0.4, every Legolas.Row field's type was available as a type parameter of Legolas.Row; for example, the type of a field y specified as y::Real in a Legolas.@row declaration would be surfaced like Legolas.Row{..., NamedTuple{(...,:y,...),Tuple{...,typeof(y),...}}. In Legolas v0.5, the schema version author controls which fields have their types surfaced as type parameters in Legolas-generated record types via the field::(<:F) syntax in @version.\nAdditionally, to include type parameters associated to fields in a parent schema, they must be re-declared in the child schema. For example, the package LegolasFlux declares a ModelV1 version with a field weights::(<:Union{Missing,Weights}). LegolasFlux includes an example with a schema extension DigitsRowV1 which extends ModelV1. This @version call must re-declare the field weights to be parametric in order for the DigitsRowV1 struct to also have a type parameter for this field.\nIn Legolas v0.4,  @row-generated Legolas.Row constructors accepted and propagated any non-schema-declared fields provided by the caller. In Legolas v0.5, @version-generated record type constructors will discard any non-schema-declared fields provided by the caller. When upgrading code that formerly \"implicitly extended\" a given schema version by propagating non-declared fields, it is advisable to instead explicitly declare a new extension of the schema version to capture the propagated fields as declared fields; or, if it makes more sense for a given use case, one may instead define a new schema version that adds these propagated fields as declared fields directly to the schema (likely declared as ::Union{Missing,T} to allow them to be missing).\nBefore Legolas v0.5, the documented guidance for schema authors surrounding new fields' impact on schema version breakage was misleading, implying that adding a new declared field to an existing schema version is non-breaking if the field's type allowed for Missing values. This is incorrect. For clarity, adding a new declared field to an existing schema version is a breaking change unless the field's type and value are both completely unconstrained in the declaration, i.e. the field's type constraint must be ::Any and may not feature a value-constraining or value-transforming assignment expression.","category":"page"},{"location":"upgrade/#Deserializing-old-tables-with-Legolas-v0.5-1","page":"Upgrading from v0.4 to v0.5","title":"Deserializing old tables with Legolas v0.5","text":"","category":"section"},{"location":"upgrade/#","page":"Upgrading from v0.4 to v0.5","title":"Upgrading from v0.4 to v0.5","text":"Generally, tables serialized with earlier versions of Legolas can be de-serialized with Legolas v0.5, making it only a \"code-breaking\" change, rather than a \"data-breaking\" change. However, it is strongly suggested to have reference tests with checked in (pre-Legolas v0.5) serialized tables which are deserialized and verified during the tests, in order to be sure.","category":"page"},{"location":"upgrade/#","page":"Upgrading from v0.4 to v0.5","title":"Upgrading from v0.4 to v0.5","text":"Additionally, serialized Arrow tables containing nested Legolas-v0.4-defined Legolas.Row values (i.e. a table that contains a row that has a field that is, itself, a Legolas.Row value, or contains such values) require special handling to deserialize under Legolas v0.5, if you wish users to be able to deserialize them with Legolas.read using the Legolas-v0.5-ready version of your package. Note that these tables are still deserializable as plain Arrow tables regardless, so it may not be worthwhile to provide a bespoke deprecation/compatibility pathway in the  Legolas-v0.5-ready version package unless your use case merits it (i.e. the impact surface would be high for your package's users).","category":"page"},{"location":"upgrade/#","page":"Upgrading from v0.4 to v0.5","title":"Upgrading from v0.4 to v0.5","text":"If you would like to provide such a pathway, though:","category":"page"},{"location":"upgrade/#","page":"Upgrading from v0.4 to v0.5","title":"Upgrading from v0.4 to v0.5","text":"Recall that under Legolas v0.4, @row-generated Legolas.Row constructors may accept and propagate arbitrary non-schema-declared fields, whereas Legolas v0.5's @version-generated record types may only contain schema-declared fields. Therefore, one must decide what to do with any non-declared fields present in serialized Legolas.Row values upon deserialization. A common approach is to implement a deprecation/compatibility pathway within the relevant surrounding @version declaration. For example, this LegolasFlux example uses a function compat_config to handle old Legolas.Row values, but does not add any handling for non-declared fields, which will be discarded if present. If one did not want non-declared fields to be discarded, these fields could be handled by throwing an error or warning, or defining a schema version extension that captured them, or defining a new version of the relevant schema to capture them (e.g. adding a field like extras::Union{Missing, NamedTuple}).","category":"page"},{"location":"arrow-concepts/#Arrow-Related-Concepts/Conventions-1","page":"Arrow-Related Concepts/Conventions","title":"Arrow-Related Concepts/Conventions","text":"","category":"section"},{"location":"arrow-concepts/#","page":"Arrow-Related Concepts/Conventions","title":"Arrow-Related Concepts/Conventions","text":"note: Note\nIf you're a newcomer to Legolas.jl, please familiarize yourself with the tour before diving into this documentation.","category":"page"},{"location":"arrow-concepts/#","page":"Arrow-Related Concepts/Conventions","title":"Arrow-Related Concepts/Conventions","text":"Legolas.jl's target (de)serialization format, Arrow, already features wide cross-language adoption, enabling Legolas-serialized tables to be seamlessly read into many non-Julia environments. This documentation section contains conventions related to Legolas-serialized Arrow tables that may be observable by generic Legolas-unaware Arrow consumers.","category":"page"},{"location":"arrow-concepts/#Supporting-Legolas-Schema-Discovery-In-Arrow-Tables-1","page":"Arrow-Related Concepts/Conventions","title":"Supporting Legolas Schema Discovery In Arrow Tables","text":"","category":"section"},{"location":"arrow-concepts/#","page":"Arrow-Related Concepts/Conventions","title":"Arrow-Related Concepts/Conventions","text":"Legolas defines a special field legolas_schema_qualified that Legolas-aware Arrow writers may include in an Arrow table's table-level metadata to indicate a particular Legolas schema with which the table complies.","category":"page"},{"location":"arrow-concepts/#","page":"Arrow-Related Concepts/Conventions","title":"Arrow-Related Concepts/Conventions","text":"Arrow tables which include this field are considered to \"support Legolas schema discovery\" and are referred to as \"Legolas-discoverable\", since Legolas consumers may employ this field to automatically match the table against available application-layer Legolas schema definitions.","category":"page"},{"location":"arrow-concepts/#","page":"Arrow-Related Concepts/Conventions","title":"Arrow-Related Concepts/Conventions","text":"If present, the legolas_schema_qualified field's value must be a fully qualified schema version identifier.","category":"page"},{"location":"arrow-concepts/#Arrow-File-Naming-Conventions-1","page":"Arrow-Related Concepts/Conventions","title":"Arrow File Naming Conventions","text":"","category":"section"},{"location":"arrow-concepts/#","page":"Arrow-Related Concepts/Conventions","title":"Arrow-Related Concepts/Conventions","text":"When writing a Legolas-discoverable Arrow table to a file, prefer using the file extension *.<schema name>.arrow. For example, if the file's table's full Legolas schema version identifier is baz.supercar@1>bar.automobile@1, use the file extension *.baz.supercar.arrow.","category":"page"},{"location":"#API-Documentation-1","page":"API Documentation","title":"API Documentation","text":"","category":"section"},{"location":"#","page":"API Documentation","title":"API Documentation","text":"note: Note\nIf you're a newcomer to Legolas.jl, please familiarize yourself with the tour before diving into this documentation.","category":"page"},{"location":"#","page":"API Documentation","title":"API Documentation","text":"CurrentModule = Legolas","category":"page"},{"location":"#Legolas-Schemas-1","page":"API Documentation","title":"Legolas Schemas","text":"","category":"section"},{"location":"#","page":"API Documentation","title":"API Documentation","text":"Legolas.SchemaVersion\nLegolas.@schema\nLegolas.@version\nLegolas.is_valid_schema_name\nLegolas.parse_identifier\nLegolas.name\nLegolas.version\nLegolas.identifier\nLegolas.parent\nLegolas.declared_fields\nLegolas.declaration\nLegolas.record_type\nLegolas.schema_version_from_record\nLegolas.declared\nLegolas.find_violation\nLegolas.find_violations\nLegolas.complies_with\nLegolas.validate\nLegolas.accepted_field_type","category":"page"},{"location":"#Legolas.SchemaVersion","page":"API Documentation","title":"Legolas.SchemaVersion","text":"Legolas.SchemaVersion{name,version}\n\nA type representing a particular version of Legolas schema. The relevant name (a Symbol) and version (an Integer) are surfaced as type parameters, allowing them to be utilized for dispatch.\n\nFor more details and examples, please see Legolas.jl/examples/tour.jl and the \"Schema-Related Concepts/Conventions\" section of the Legolas.jl documentation.\n\nThe constructor SchemaVersion{name,version}() will throw an ArgumentError if version is negative.\n\nSee also: Legolas.@schema\n\n\n\n\n\n","category":"type"},{"location":"#Legolas.@schema","page":"API Documentation","title":"Legolas.@schema","text":"@schema \"name\" Prefix\n\nDeclare a Legolas schema with the given name. Types generated by subsequent @version declarations for this schema will be prefixed with Prefix.\n\nFor more details and examples, please see Legolas.jl/examples/tour.jl.\n\n\n\n\n\n","category":"macro"},{"location":"#Legolas.@version","page":"API Documentation","title":"Legolas.@version","text":"@version RecordType begin\n    declared_field_expression_1\n    declared_field_expression_2\n    ⋮\nend\n\n@version RecordType > ParentRecordType begin\n    declared_field_expression_1\n    declared_field_expression_2\n    ⋮\nend\n\nGiven a prior @schema declaration of the form:\n\n@schema \"example.name\" Name\n\n...the nth version of example.name can be declared in the same module via a @version declaration of the form:\n\n@version NameV$(n) begin\n    declared_field_expression_1\n    declared_field_expression_2\n    ⋮\nend\n\n...which generates types definitions for the NameV$(n) type (a Legolas.AbstractRecord subtype) and NameV$(n)SchemaVersion type (an alias of typeof(SchemaVersion(\"example.name\", n))), as well as the necessary definitions to overload relevant Legolas methods with specialized behaviors in accordance with the declared fields.\n\nIf the declared schema version has a parent, it should be specified via the optional > ParentRecordType clause. ParentRecordType should refer directly to an existing Legolas-generated record type.\n\nEach declared_field_expression declares a field of the schema version, and is an expression of the form field::F = rhs where:\n\nfield is the corresponding field's name\n::F denotes the field's type constraint (if elided, defaults to ::Any).\nrhs is the expression which produces field::F (if elided, defaults to field).\n\nAccounting for all of the aforementioned allowed elisions, valid declared_field_expressions include:\n\nfield::F = rhs\nfield::F (interpreted as field::F = field)\nfield = rhs (interpreted as field::Any = rhs)\nfield (interpreted as field::Any = field)\n\nF is generally a type literal, but may also be an expression of the form (<:T), in which case the declared schema version's generated record type will expose a type parameter (constrained to be a subtype of T) for the given field. For example:\n\njulia> @schema \"example.foo\" Foo\n\njulia> @version FooV1 begin\n           x::Int\n           y::(<:Real)\n       end\n\njulia> FooV1(x=1, y=2.0)\nFooV1{Float64}: (x = 1, y = 2.0)\n\njulia> FooV1{Float32}(x=1, y=2)\nFooV1{Float32}: (x = 1, y = 2.0f0)\n\njulia> FooV1(x=1, y=\"bad\")\nERROR: TypeError: in FooV1, in _y_T, expected _y_T<:Real, got Type{String}\n\nThis macro will throw a Legolas.SchemaVersionDeclarationError if:\n\nThe provided RecordType does not follow the $(Prefix)V$(n) format, where Prefix was previously associated with a given schema by a prior @schema declaration.\nThere are no declared field expressions, duplicate fields are declared, or a given declared field expression is invalid.\n(if a parent is specified) The @version declaration does not comply with its parent's @version declaration, or the parent hasn't yet been declared at all.\n\nNote that this macro expects to be evaluated within top-level scope.\n\nFor more details and examples, please see Legolas.jl/examples/tour.jl and the \"Schema-Related Concepts/Conventions\" section of the Legolas.jl documentation.\n\n\n\n\n\n","category":"macro"},{"location":"#Legolas.is_valid_schema_name","page":"API Documentation","title":"Legolas.is_valid_schema_name","text":"Legolas.is_valid_schema_name(x::AbstractString)\n\nReturn true if x is a valid schema name, return false otherwise.\n\nValid schema names are lowercase, alphanumeric, and may contain hyphens or periods.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.parse_identifier","page":"API Documentation","title":"Legolas.parse_identifier","text":"Legolas.parse_identifier(id::AbstractString)\n\nGiven a valid schema version identifier id of the form:\n\n$(names[1])@$(versions[1]) > $(names[2])@$(versions[2]) > ... > $(names[n])@$(versions[n])\n\nreturn an n element Vector{SchemaVersion} whose ith element is SchemaVersion(names[i], versions[i]).\n\nThrows an ArgumentError if the provided string is not a valid schema version identifier.\n\nFor details regarding valid schema version identifiers and their structure, see the \"Schema-Related Concepts/Conventions\" section of the Legolas.jl documentation.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.name","page":"API Documentation","title":"Legolas.name","text":"Legolas.name(::Legolas.SchemaVersion{n})\n\nReturn n.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.version","page":"API Documentation","title":"Legolas.version","text":"Legolas.version(::Legolas.SchemaVersion{n,v})\n\nReturn v.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.identifier","page":"API Documentation","title":"Legolas.identifier","text":"Legolas.identifier(::Legolas.SchemaVersion)\n\nReturn this Legolas.SchemaVersion's fully qualified schema version identifier. This string is serialized as the \"legolas_schema_qualified\" field value in table metadata for table written via Legolas.write.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.parent","page":"API Documentation","title":"Legolas.parent","text":"Legolas.parent(sv::Legolas.SchemaVersion)\n\nReturn the Legolas.SchemaVersion instance that corresponds to sv's declared parent.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.declared_fields","page":"API Documentation","title":"Legolas.declared_fields","text":"Legolas.declared_fields(sv::Legolas.SchemaVersion)\n\nReturn a NamedTuple{...,Tuple{Vararg{DataType}} whose fields take the form:\n\n<name of field declared by `sv`> = <field's type>\n\nIf sv has a parent, the returned fields will include declared_fields(parent(sv)).\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.declaration","page":"API Documentation","title":"Legolas.declaration","text":"Legolas.declaration(sv::Legolas.SchemaVersion)\n\nReturn a Pair{String,Vector{NamedTuple}} of the form\n\nschema_version_identifier::String => declared_field_infos::Vector{Legolas.DeclaredFieldInfo}\n\nwhere DeclaredFieldInfo has the fields:\n\nname::Symbol: the declared field's name\ntype::Union{Symbol,Expr}: the declared field's declared type constraint\nparameterize::Bool: whether or not the declared field is exposed as a parameter\nstatement::Expr: the declared field's full assignment statement (as processed by @version, not necessarily as written)\n\nNote that declaration is primarily intended to be used for interactive discovery purposes, and does not include the contents of declaration(parent(sv)).\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.record_type","page":"API Documentation","title":"Legolas.record_type","text":"Legolas.record_type(sv::Legolas.SchemaVersion)\n\nReturn the Legolas.AbstractRecord subtype associated with sv.\n\nSee also: Legolas.schema_version_from_record\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.schema_version_from_record","page":"API Documentation","title":"Legolas.schema_version_from_record","text":"Legolas.schema_version_from_record(record::Legolas.AbstractRecord)\n\nReturn the Legolas.SchemaVersion instance associated with record.\n\nSee also: Legolas.record_type\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.declared","page":"API Documentation","title":"Legolas.declared","text":"Legolas.declared(sv::Legolas.SchemaVersion{name,version})\n\nReturn true if the schema version name@version has been declared via @version in the current Julia session; return false otherwise.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.find_violation","page":"API Documentation","title":"Legolas.find_violation","text":"Legolas.find_violation(ts::Tables.Schema, sv::Legolas.SchemaVersion)\n\nFor each field f::F declared by sv:\n\nDefine A = Legolas.accepted_field_type(sv, F)\nIf f::T is present in ts, ensure that T <: A or else immediately return f::Symbol => T::DataType.\nIf f isn't present in ts, ensure that Missing <: A or else immediately return f::Symbol => missing::Missing.\n\nOtherwise, return nothing.\n\nTo return all violations instead of just the first, use Legolas.find_violations.\n\nSee also: Legolas.validate, Legolas.complies_with, Legolas.find_violations.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.find_violations","page":"API Documentation","title":"Legolas.find_violations","text":"Legolas.find_violations(ts::Tables.Schema, sv::Legolas.SchemaVersion)\n\nReturn a Vector{Pair{Symbol,Union{Type,Missing}}} of all of ts's violations with respect to sv.\n\nThis function's notion of \"violation\" is defined by Legolas.find_violation, which immediately returns the first violation found; prefer to use that function instead of find_violations in situations where you only need to detect any violation instead of all violations.\n\nSee also: Legolas.validate, Legolas.complies_with, Legolas.find_violation.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.complies_with","page":"API Documentation","title":"Legolas.complies_with","text":"Legolas.complies_with(ts::Tables.Schema, sv::Legolas.SchemaVersion)\n\nReturn isnothing(find_violation(ts, sv)).\n\nSee also: Legolas.find_violation, Legolas.find_violations, Legolas.validate\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.validate","page":"API Documentation","title":"Legolas.validate","text":"Legolas.validate(ts::Tables.Schema, sv::Legolas.SchemaVersion)\n\nThrows a descriptive ArgumentError if any violations are found, else return nothing.\n\nSee also: Legolas.find_violation, Legolas.find_violations, Legolas.find_violation, Legolas.complies_with\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.accepted_field_type","page":"API Documentation","title":"Legolas.accepted_field_type","text":"Legolas.accepted_field_type(sv::Legolas.SchemaVersion, T::Type)\n\nReturn the \"maximal supertype\" of T that is accepted by sv when evaluating a field of type >:T for schematic compliance via Legolas.find_violation; see that function's docstring for an explanation of this function's use in context.\n\nSchemaVersion authors may overload this function to broaden particular type constraints that determine schematic compliance for their SchemaVersion, without needing to broaden the type constraints employed by their SchemaVersion's record type.\n\nLegolas itself defines the following default overloads:\n\naccepted_field_type(::SchemaVersion, T::Type) = T\naccepted_field_type(::SchemaVersion, ::Type{Any}) = Any\naccepted_field_type(::SchemaVersion, ::Type{UUID}) = Union{UUID,UInt128}\naccepted_field_type(::SchemaVersion, ::Type{Symbol}) = Union{Symbol,AbstractString}\naccepted_field_type(::SchemaVersion, ::Type{String}) = AbstractString\naccepted_field_type(sv::SchemaVersion, ::Type{<:Vector{T}}) where T = AbstractVector{<:(accepted_field_type(sv, T))}\naccepted_field_type(::SchemaVersion, ::Type{Vector}) = AbstractVector\naccepted_field_type(sv::SchemaVersion, ::Type{Union{T,Missing}}) where {T} = Union{accepted_field_type(sv, T),Missing}\n\nOutside of these default overloads, this function should only be overloaded against specific SchemaVersions that are authored within the same module as the overload definition; to do otherwise constitutes type piracy and should be avoided.\n\n\n\n\n\n","category":"function"},{"location":"#Validating/Writing/Reading-Legolas-Tables-1","page":"API Documentation","title":"Validating/Writing/Reading Legolas Tables","text":"","category":"section"},{"location":"#","page":"API Documentation","title":"API Documentation","text":"Legolas.extract_schema_version\nLegolas.write\nLegolas.read","category":"page"},{"location":"#Legolas.extract_schema_version","page":"API Documentation","title":"Legolas.extract_schema_version","text":"Legolas.extract_schema_version(table)\n\nAttempt to extract Arrow metadata from table via Arrow.getmetadata(table).\n\nIf Arrow metadata is present and contains \"legolas_schema_qualified\" => s, return first(parse_identifier(s))\n\nOtherwise, return nothing.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.write","page":"API Documentation","title":"Legolas.write","text":"Legolas.write(io_or_path, table, sv::SchemaVersion; validate::Bool=true, kwargs...)\n\nWrite table to io_or_path, inserting the appropriate legolas_schema_qualified field in the written out Arrow metadata.\n\nIf validate is true, Legolas.validate(Tables.schema(table), vs) will be invoked before the table is written out to io_or_path.\n\nAny other provided kwargs are forwarded to an internal invocation of Arrow.write.\n\nNote that io_or_path may be any type that supports Base.write(io_or_path, bytes::Vector{UInt8}).\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.read","page":"API Documentation","title":"Legolas.read","text":"Legolas.read(io_or_path; validate::Bool=true)\n\nRead and return an Arrow.Table from io_or_path.\n\nIf validate is true, Legolas.read will attempt to extract a Legolas.SchemaVersion from the deserialized Arrow.Table's metadata and use Legolas.validate to verify that the table's Table.Schema complies with the extracted Legolas.SchemaVersion before returning the table.\n\nNote that io_or_path may be any type that supports Base.read(io_or_path)::Vector{UInt8}.\n\n\n\n\n\n","category":"function"},{"location":"#Utilities-1","page":"API Documentation","title":"Utilities","text":"","category":"section"},{"location":"#","page":"API Documentation","title":"API Documentation","text":"Legolas.lift\nLegolas.construct\nLegolas.assign_to_table_metadata!\nLegolas.gather\nLegolas.locations\nLegolas.materialize","category":"page"},{"location":"#Legolas.lift","page":"API Documentation","title":"Legolas.lift","text":"lift(f, x)\n\nReturn f(x) unless x isa Union{Nothing,Missing}, in which case return missing.\n\nThis is particularly useful when handling values from Arrow.Table, whose null values may present as either missing or nothing depending on how the table itself was originally constructed.\n\nSee also: construct\n\n\n\n\n\nlift(f)\n\nReturns a curried function, x -> lift(f,x)\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.construct","page":"API Documentation","title":"Legolas.construct","text":"construct(T::Type, x)\n\nConstruct T(x) unless x is of type T, in which case return x itself. Useful in conjunction with the lift function for types which don't have a constructor which accepts instances of itself (e.g. T(::T)).\n\nExamples\n\njulia> using Legolas: construct\n\njulia> construct(Float64, 1)\n1.0\n\njulia> Some(Some(1))\nSome(Some(1))\n\njulia> construct(Some, Some(1))\nSome(1)\n\nUse the curried form when using lift:\n\njulia> using Legolas: lift, construct\n\njulia> lift(Some, Some(1))\nSome(Some(1))\n\njulia> lift(construct(Some), Some(1))\nSome(1)\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.gather","page":"API Documentation","title":"Legolas.gather","text":"Legolas.gather(column_name, tables...; extract=((table, idxs) -> view(table, idxs, :)))\n\nGather rows from tables into a unified cross-table index along column_name. Returns a Dict whose keys are the unique values of column_name across tables, and whose values are tuples of the form:\n\n(rows_matching_key_in_table_1, rows_matching_key_in_table_2, ...)\n\nThe provided extract function is used to extract rows from each table; it takes as input a table and a Vector{Int} of row indices, and returns the corresponding subtable. The default definition is sufficient for DataFrames tables.\n\nNote that this function may internally call Tables.columns on each input table, so it may be slower and/or require more memory if any(!Tables.columnaccess, tables).\n\nNote that we intend to eventually migrate this function from Legolas.jl to a more appropriate package.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.locations","page":"API Documentation","title":"Legolas.locations","text":"locations(collections::Tuple)\n\nReturn a Dict whose keys are the set of all elements across all provided collections, and whose values are the indices that locate each corresponding element across all provided collecitons.\n\nSpecifically, locations(collections)[k][i] will return a Vector{Int} whose elements are the index locations of k in collections[i]. If !(k in collections[i]), this Vector{Int} will be empty.\n\nFor example:\n\njulia> Legolas.locations((['a', 'b', 'c', 'f', 'b'],\n                          ['d', 'c', 'e', 'b'],\n                          ['f', 'a', 'f']))\nDict{Char, Tuple{Vector{Int64}, Vector{Int64}, Vector{Int64}}} with 6 entries:\n  'f' => ([4], [], [1, 3])\n  'a' => ([1], [], [2])\n  'c' => ([3], [2], [])\n  'd' => ([], [1], [])\n  'e' => ([], [3], [])\n  'b' => ([2, 5], [4], [])\n\nThis function is useful as a building block for higher-level tabular operations that require indexing/grouping along specific sets of elements.\n\n\n\n\n\n","category":"function"},{"location":"#Legolas.materialize","page":"API Documentation","title":"Legolas.materialize","text":"Legolas.materialize(table)\n\nReturn a fully deserialized copy of table.\n\nThis function is useful when table has built-in deserialize-on-access or conversion-on-access behavior (like Arrow.Table) and you'd like to pay such access costs upfront before repeatedly accessing the table.\n\nNote that we intend to eventually migrate this function from Legolas.jl to a more appropriate package.\n\n\n\n\n\n","category":"function"},{"location":"faq/#FAQ-1","page":"FAQ","title":"FAQ","text":"","category":"section"},{"location":"faq/#What-is-the-point-of-Legolas.jl?-Who-benefits-from-using-it?-1","page":"FAQ","title":"What is the point of Legolas.jl? Who benefits from using it?","text":"","category":"section"},{"location":"faq/#","page":"FAQ","title":"FAQ","text":"At its core, Legolas.jl provides a lightweight, expressive set of mechanisms/patterns for wrangling Tables.jl-compliant values in a manner that enables schema composability, extensibility and a few nice utilties on top.","category":"page"},{"location":"faq/#","page":"FAQ","title":"FAQ","text":"The package originated from code developed internally at Beacon to wrangling heterogeneous Arrow datasets, and is thus probably mostly useful for folks in a similar situation. If you're curating tabular datasets and you'd like to build shared Julia tools atop the schemas therein, then Legolas.jl may be worth checking out.","category":"page"},{"location":"faq/#Why-does-Legolas.jl-support-Arrow-as-a-(de)serialization-target,-but-not,-say,-JSON?-1","page":"FAQ","title":"Why does Legolas.jl support Arrow as a (de)serialization target, but not, say, JSON?","text":"","category":"section"},{"location":"faq/#","page":"FAQ","title":"FAQ","text":"Technically, Legolas.jl's core @schema/@version functionality is agnostic to (de)serialization and could be useful for anybody who wants to wrangle Tables.jl-compliant values.","category":"page"},{"location":"faq/#","page":"FAQ","title":"FAQ","text":"Otherwise, with regards to (de)serialization-specific functionality, Beacon has put effort into ensuring Legolas.jl works well with Arrow.jl \"by default\" simply because we're heavy users of the Arrow format. There's nothing stopping users from composing the package with JSON3.jl or other packages.","category":"page"},{"location":"faq/#Why-are-Legolas.jl's-generated-record-types-defined-the-way-that-they-are?-For-example,-why-is-the-version-number-hardcoded-in-the-type-name?-1","page":"FAQ","title":"Why are Legolas.jl's generated record types defined the way that they are? For example, why is the version number hardcoded in the type name?","text":"","category":"section"},{"location":"faq/#","page":"FAQ","title":"FAQ","text":"Many of Legolas' current choices on this front stem from refactoring efforts undertaken as part of this pull request, and directly resulted from a design mini-investigation associated with those efforts.","category":"page"},{"location":"schema-concepts/#Schema-Related-Concepts/Conventions-1","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"","category":"section"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"note: Note\nIf you're a newcomer to Legolas.jl, please familiarize yourself with the tour before diving into this documentation.","category":"page"},{"location":"schema-concepts/#schema_version_identifier_specification-1","page":"Schema-Related Concepts/Conventions","title":"Schema Version Identifiers","text":"","category":"section"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"Legolas defines \"schema version identifiers\" as strings of the form:","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"name@version where:\nname is a lowercase alphanumeric string and may include the special characters . and -.\nversion is a non-negative integer.\nor, x>y where x and y are valid schema version identifiers and > denotes \"extends from\".","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"A schema version identifier is said to be fully qualified if it includes the identifiers of all ancestors of the particular schema version that it directly identifies.","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"Schema authors should follow the below conventions when choosing the name of a new schema:","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"Include a namespace. For example, assuming the schema is defined in a package Foo.jl, foo.automobile is good, automobile is bad.\nPrefer singular over plural. For example, foo.automobile is good, foo.automobiles is bad.\nDon't \"overqualify\" a schema name with ancestor-derived information that is better captured by the fully qualified identifier of a specific schema version. For example, bar.automobile should be preferred over bar.foo.automobile, since bar.automobile@1>foo.automobile@1 is preferable to bar.foo.automobile@1>foo.automobile@1. Similarly, baz.supercar should be preferred over baz.automobile.supercar, since baz.supercar@1>bar.automobile@1 is preferable to baz.automobile.supercar@1>bar.automobile@1.","category":"page"},{"location":"schema-concepts/#Schema-Versioning:-You-Break-It,-You-Bump-It-1","page":"Schema-Related Concepts/Conventions","title":"Schema Versioning: You Break It, You Bump It","text":"","category":"section"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"While it is fairly established practice to semantically version source code, the world of data/artifact versioning is a bit more varied. As presented in the tour, each Legolas.SchemaVersion carries a single version integer. The central rule that governs Legolas' schema versioning approach is:","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"Do not introduce a change to an existing schema version that might cause existing compliant data to become non-compliant; instead, incorporate the intended change in a new schema version whose version number is one greater than the previous version number.","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"A schema author must introduce a new schema version if any of the following changes are introduced:","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"A new type-constrained and/or value-constrained field is declared. In other words, for the introduction of a new declared field to be non-breaking, the new field's type constraint must be ::Any and it may not feature a value-constraining or value-transforming assignment expression.\nAn existing declared field's type or value constraints are tightened.\nAn existing declared field is renamed.","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"If any of the above breaking changes are made to an existing schema version, instead of introducing a new schema version, subtle downstream breakage may occur. For example, if a new type/value-constrained field is declared, previously compliant tables containing a field with the same name might accidentally become non-compliant if existing values violate the new constraints. Similarly, downstream schema version extensions may have already declared a field with the same name, but with constraints that are incompatible with the new constraints.","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"One benefit of Legolas' approach is that multiple schema versions may be defined in the same codebase, e.g. there's nothing that prevents @version(FooV1, ...) and @version(FooV2, ...) from being defined and utilized simultaneously. The source code that defines any given Legolas schema version and/or consumes/produces Legolas tables is presumably already semantically versioned, such that consumer/producer packages can determine their compatibility with each other in the usual manner via interpreting major/minor/patch increments.","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"Note that it is preferable to avoid introducing new versions of an existing schema, if possible, in order to minimize code/data churn for downstream producers/consumers. Thus, authors should prefer conservative field type restrictions from the get-go. Remember: loosening a field type restriction is not a breaking change, but tightening one is.","category":"page"},{"location":"schema-concepts/#Important-Expectations-Regarding-Custom-Field-Assignments-1","page":"Schema-Related Concepts/Conventions","title":"Important Expectations Regarding Custom Field Assignments","text":"","category":"section"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"Schema authors should ensure that their @version declarations meet two important expectations so that generated record types behaves as intended:","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"Custom field assignments should preserve the idempotency of record type constructors.\nCustom field assignments should not observe mutable non-local state.","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"Thus, given a Legolas-generated record type R, the following should hold for all valid values of fields:","category":"page"},{"location":"schema-concepts/#","page":"Schema-Related Concepts/Conventions","title":"Schema-Related Concepts/Conventions","text":"R(R(fields)) == R(fields)\nR(fields) == R(fields)","category":"page"}]
}
