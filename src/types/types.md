The module `types` defines several structs which hold the results of the data analysis. The data is organized in a tree structure with `DelayStatistics` at the root:

 * `DelayStatistics`
   * `RouteData` indexed by route_id
     * `RouteVariantData` indexed by route_variant_id
       * `CurveSet` indexed by (start_index, end_index, TimeSlot)
         * `IrregularDynamicCurve` indexed initial_delay
       * general_delay, an `EventPair` which for each `.arrival` and `.departure` has:
         * `IrregularDynamicCurve` indexed stop_index
   * `DefaultCurves`
     * `IrregularDynamicCurve` indexed by `RouteType, RouteSection, TimeSlot, EventType`

Most of those structs support (de)serialization with `serde`, in either MessagePack or Json format. Whereas most of those types are implemented in `dystonse-gtfs-data::types`, the relevant traits are defined in `dystonse-curves`.

The types implement the traits `serde::Serialize` and `serde::Deserialize`, so you can use the standard methods which Serde provides to handle them.

Via the trait `NodeData`, the methods `save_to_file` and `load_from_file` are also provided for convenience. 

If you serialize a `DelayStatistics` object to a single file, it can become quite big and bulky. Especially the Json serialization will be unmanageable with several million lines. It is recommended to save the data into separate, smaller files and store them in nested directories. To facilitate this, the `TreeData` trait provides the functions `save_tree` and `load_tree`.

Both functions take a `leaves` parameter at the end, which is a vector of type names. This parameter allows you to define which parts of the object tree will be treated as a directory tree, and which parts will be treated as single files.

In the default case, when you pass an empty vector as `leaves`, the functions will write or read a nested directory structure with the maximul possible depth. The leaves of the object tree structure, which are all of type `IrregularDynamicCurve`, correspond to the leaves of the directory tree, that is: there will be one file for each `IrregularDynamicCurve`.

If you pass the type name(s) of intermediate object type(s), those will be the leaves of the directory tree, e.g. if you pass `vec!(CurveSet::NAME)`, the object tree will be mapped to a directory tree until a `CurveSet` object is encountered, and a single file will be written for each `CurveSet`, instead of multiple files for the contained `Curve` objects.

Calling `save_tree` on an object `x` of type `X` with `vec!(X::NAME)` as leaves parameter will only write a single file for that object, which is equivalent to calling `x.save_to_file`.