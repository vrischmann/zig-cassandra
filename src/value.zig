pub const ValueTag = enum {
    Set,
    NotSet,
    Null,
};
pub const Value = union(ValueTag) {
    Set: []u8,
    NotSet: void,
    Null: void,
};

pub const NamedValue = struct {
    name: []const u8,
    value: Value,
};

pub const ValuesType = enum {
    Normal,
    Named,
};

pub const Values = union(ValuesType) {
    Normal: []Value,
    Named: []NamedValue,
};
