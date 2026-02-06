package bind

import (
	"reflect"
	"testing"
)

func TestNewFieldMapperDefaultTag(t *testing.T) {
	m := NewFieldMapper("")
	if m.tagName != "json" {
		t.Errorf("expected default tag 'json', got %q", m.tagName)
	}
}

func TestNewFieldMapperCustomTag(t *testing.T) {
	m := NewFieldMapper("yaml")
	if m.tagName != "yaml" {
		t.Errorf("expected tag 'yaml', got %q", m.tagName)
	}
}

func TestStructToMapBasic(t *testing.T) {
	type S struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Name: "hello", Count: 42})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["name"] != "hello" {
		t.Errorf("expected name 'hello', got %v", result["name"])
	}
	if result["count"] != 42 {
		t.Errorf("expected count 42, got %v", result["count"])
	}
}

func TestStructToMapPointer(t *testing.T) {
	type S struct {
		Name string `json:"name"`
	}

	m := NewFieldMapper("json")
	s := &S{Name: "ptr"}
	result, err := m.StructToMap(s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["name"] != "ptr" {
		t.Errorf("expected name 'ptr', got %v", result["name"])
	}
}

func TestStructToMapNonStruct(t *testing.T) {
	m := NewFieldMapper("json")
	_, err := m.StructToMap("not a struct")
	if err == nil {
		t.Error("expected error for non-struct input")
	}
}

func TestStructToMapNestedStruct(t *testing.T) {
	type Inner struct {
		Value string `json:"value"`
	}
	type Outer struct {
		Name  string `json:"name"`
		Inner Inner  `json:"inner"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(Outer{Name: "test", Inner: Inner{Value: "nested"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["name"] != "test" {
		t.Errorf("expected name 'test', got %v", result["name"])
	}
	nested, ok := result["inner"].(map[string]any)
	if !ok {
		t.Fatalf("expected inner to be map[string]any, got %T", result["inner"])
	}
	if nested["value"] != "nested" {
		t.Errorf("expected inner.value 'nested', got %v", nested["value"])
	}
}

func TestStructToMapIgnoredField(t *testing.T) {
	type S struct {
		Name   string `json:"name"`
		Secret string `json:"-"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Name: "test", Secret: "hidden"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["Secret"]; ok {
		t.Error("expected Secret to be absent")
	}
	if _, ok := result["-"]; ok {
		t.Error("expected '-' key to be absent")
	}
	if result["name"] != "test" {
		t.Errorf("expected name 'test', got %v", result["name"])
	}
}

func TestStructToMapOmitempty(t *testing.T) {
	type S struct {
		Name  string `json:"name,omitempty"`
		Count int    `json:"count,omitempty"`
		Flag  bool   `json:"flag,omitempty"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map for zero-value struct with omitempty, got %v", result)
	}

	result, err = m.StructToMap(S{Name: "set", Count: 1, Flag: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["name"] != "set" {
		t.Errorf("expected name 'set', got %v", result["name"])
	}
	if result["count"] != 1 {
		t.Errorf("expected count 1, got %v", result["count"])
	}
}

func TestStructToMapNonrecursive(t *testing.T) {
	type Inner struct {
		A string `json:"a"`
		B int    `json:"b"`
	}
	type Outer struct {
		Inner Inner `json:"inner,nonrecursive"` //nolint:staticcheck // nonrecursive is a custom bind option
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(Outer{Inner: Inner{A: "x", B: 1}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// nonrecursive should store the struct as-is, not as a nested map
	val, ok := result["inner"]
	if !ok {
		t.Fatal("expected 'inner' key to exist")
	}
	if _, isMap := val.(map[string]any); isMap {
		t.Error("expected nonrecursive field to NOT be a map")
	}
	inner, ok := val.(Inner)
	if !ok {
		t.Fatalf("expected Inner struct, got %T", val)
	}
	if inner.A != "x" || inner.B != 1 {
		t.Errorf("unexpected inner value: %+v", inner)
	}
}

func TestStructToMapUnexportedFieldsSkipped(t *testing.T) {
	type S struct {
		Name     string `json:"name"`
		internal string //nolint:unused
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Name: "test"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 key, got %d", len(result))
	}
}

func TestStructToMapNoTagUsesFieldName(t *testing.T) {
	type S struct {
		MyField string
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{MyField: "value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["MyField"] != "value" {
		t.Errorf("expected MyField 'value', got %v", result["MyField"])
	}
}

func TestStructToMapPointerFieldNil(t *testing.T) {
	type S struct {
		Name *string `json:"name"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Name: nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["name"] != nil {
		t.Errorf("expected nil for nil pointer, got %v", result["name"])
	}
}

func TestStructToMapPointerFieldNonNil(t *testing.T) {
	type S struct {
		Name *string `json:"name"`
	}

	v := "hello"
	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Name: &v})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["name"] != "hello" {
		t.Errorf("expected 'hello', got %v", result["name"])
	}
}

func TestStructToMapPointerToStruct(t *testing.T) {
	type Inner struct {
		Val string `json:"val"`
	}
	type Outer struct {
		Inner *Inner `json:"inner"`
	}

	m := NewFieldMapper("json")

	// Non-nil pointer to struct
	result, err := m.StructToMap(Outer{Inner: &Inner{Val: "test"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	nested, ok := result["inner"].(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result["inner"])
	}
	if nested["val"] != "test" {
		t.Errorf("expected val 'test', got %v", nested["val"])
	}

	// Nil pointer to struct
	result, err = m.StructToMap(Outer{Inner: nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["inner"] != nil {
		t.Errorf("expected nil for nil pointer to struct, got %v", result["inner"])
	}
}

func TestStructToMapPointerToStructNonrecursive(t *testing.T) {
	type Inner struct {
		Val string `json:"val"`
	}
	type Outer struct {
		Inner *Inner `json:"inner,nonrecursive"` //nolint:staticcheck // nonrecursive is a custom bind option
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(Outer{Inner: &Inner{Val: "test"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// nonrecursive on pointer to struct: should store struct value
	_, isMap := result["inner"].(map[string]any)
	if isMap {
		t.Error("expected nonrecursive pointer field to NOT be a nested map")
	}
}

func TestStructToMapPointerNilOmitempty(t *testing.T) {
	type S struct {
		Name *string `json:"name,omitempty"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Name: nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["name"]; ok {
		t.Error("nil pointer with omitempty should be omitted")
	}
}

func TestStructToMapSlice(t *testing.T) {
	type S struct {
		Items []string `json:"items"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Items: []string{"a", "b", "c"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	items, ok := result["items"].([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result["items"])
	}
	if len(items) != 3 {
		t.Errorf("expected 3 items, got %d", len(items))
	}
	if items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Errorf("unexpected items: %v", items)
	}
}

func TestStructToMapSliceNil(t *testing.T) {
	type S struct {
		Items []string `json:"items"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Items: nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["items"] != nil {
		t.Errorf("expected nil for nil slice, got %v", result["items"])
	}
}

func TestStructToMapSliceOfStructs(t *testing.T) {
	type Item struct {
		Name string `json:"name"`
	}
	type S struct {
		Items []Item `json:"items"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Items: []Item{{Name: "a"}, {Name: "b"}}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	items, ok := result["items"].([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", result["items"])
	}
	if len(items) != 2 {
		t.Errorf("expected 2 items, got %d", len(items))
	}
	first, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", items[0])
	}
	if first["name"] != "a" {
		t.Errorf("expected first item name 'a', got %v", first["name"])
	}
}

func TestStructToMapMapField(t *testing.T) {
	type S struct {
		Labels map[string]string `json:"labels"`
	}

	m := NewFieldMapper("json")
	labels := map[string]string{"env": "prod", "region": "us"}
	result, err := m.StructToMap(S{Labels: labels})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, ok := result["labels"].(map[string]string)
	if !ok {
		t.Fatalf("expected map[string]string, got %T", result["labels"])
	}
	if got["env"] != "prod" || got["region"] != "us" {
		t.Errorf("unexpected labels: %v", got)
	}
}

func TestStructToMapNilMapField(t *testing.T) {
	type S struct {
		Labels map[string]string `json:"labels"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToMap(S{Labels: nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["labels"] != nil {
		t.Errorf("expected nil for nil map, got %v", result["labels"])
	}
}

func TestStructToMapCustomTag(t *testing.T) {
	type S struct {
		Name string `config:"my_name"`
		Age  int    `config:"my_age"`
	}

	m := NewFieldMapper("config")
	result, err := m.StructToMap(S{Name: "test", Age: 25})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["my_name"] != "test" {
		t.Errorf("expected my_name 'test', got %v", result["my_name"])
	}
	if result["my_age"] != 25 {
		t.Errorf("expected my_age 25, got %v", result["my_age"])
	}
}

func TestStructToFlatMapNonStruct(t *testing.T) {
	m := NewFieldMapper("json")
	_, err := m.StructToFlatMap(42, "prefix")
	if err == nil {
		t.Error("expected error for non-struct input")
	}
}

func TestStructToFlatMapPointerInput(t *testing.T) {
	type S struct {
		Name string `json:"name"`
	}
	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(&S{Name: "ptr"}, "p")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["p/name"] != "ptr" {
		t.Errorf("expected p/name 'ptr', got %v", result["p/name"])
	}
}

func TestStructToFlatMapEmptyPrefix(t *testing.T) {
	type S struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(S{Host: "localhost", Port: 8080}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["host"] != "localhost" {
		t.Errorf("expected host 'localhost', got %v", result["host"])
	}
	if result["port"] != 8080 {
		t.Errorf("expected port 8080, got %v", result["port"])
	}
}

func TestStructToFlatMapNested(t *testing.T) {
	type Pool struct {
		Size int `json:"size"`
	}
	type DB struct {
		Host string `json:"host"`
		Pool Pool   `json:"pool"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(DB{Host: "localhost", Pool: Pool{Size: 10}}, "db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["db/host"] != "localhost" {
		t.Errorf("expected db/host 'localhost', got %v", result["db/host"])
	}
	if result["db/pool/size"] != 10 {
		t.Errorf("expected db/pool/size 10, got %v", result["db/pool/size"])
	}
}

func TestStructToFlatMapPointerField(t *testing.T) {
	type Inner struct {
		Val string `json:"val"`
	}
	type Outer struct {
		Inner *Inner `json:"inner"`
	}

	m := NewFieldMapper("json")

	// Non-nil pointer
	result, err := m.StructToFlatMap(Outer{Inner: &Inner{Val: "hello"}}, "o")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["o/inner/val"] != "hello" {
		t.Errorf("expected o/inner/val 'hello', got %v", result["o/inner/val"])
	}

	// Nil pointer without omitempty
	result, err = m.StructToFlatMap(Outer{Inner: nil}, "o")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := result["o/inner"]; !ok || v != nil {
		t.Errorf("expected o/inner to be nil, got %v (ok=%v)", v, ok)
	}
}

func TestStructToFlatMapPointerFieldOmitempty(t *testing.T) {
	type Inner struct {
		Val string `json:"val"`
	}
	type Outer struct {
		Inner *Inner `json:"inner,omitempty"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(Outer{Inner: nil}, "o")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["o/inner"]; ok {
		t.Error("expected nil pointer with omitempty to be omitted from flat map")
	}
}

func TestStructToFlatMapPointerToNonStruct(t *testing.T) {
	type S struct {
		Count *int `json:"count"`
	}

	v := 42
	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(S{Count: &v}, "s")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["s/count"] != 42 {
		t.Errorf("expected s/count 42, got %v", result["s/count"])
	}
}

func TestStructToFlatMapNonrecursive(t *testing.T) {
	type Creds struct {
		User string `json:"user"`
		Pass string `json:"pass"`
	}
	type S struct {
		Creds Creds `json:"creds,nonrecursive"` //nolint:staticcheck // nonrecursive is a custom bind option
	}

	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(S{Creds: Creds{User: "admin", Pass: "secret"}}, "app")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// nonrecursive: should be stored as single key
	if _, ok := result["app/creds/user"]; ok {
		t.Error("expected nonrecursive field to NOT be flattened")
	}
	if _, ok := result["app/creds"]; !ok {
		t.Error("expected nonrecursive field to exist as single key")
	}
}

func TestStructToFlatMapNonrecursivePointer(t *testing.T) {
	type Inner struct {
		Val string `json:"val"`
	}
	type S struct {
		Inner *Inner `json:"inner,nonrecursive"` //nolint:staticcheck // nonrecursive is a custom bind option
	}

	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(S{Inner: &Inner{Val: "x"}}, "s")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["s/inner/val"]; ok {
		t.Error("expected nonrecursive pointer field to NOT be flattened")
	}
	if _, ok := result["s/inner"]; !ok {
		t.Error("expected nonrecursive pointer field to exist as single key")
	}
}

func TestStructToFlatMapIgnoredField(t *testing.T) {
	type S struct {
		Name   string `json:"name"`
		Secret string `json:"-"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(S{Name: "ok", Secret: "hide"}, "p")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["p/name"] != "ok" {
		t.Errorf("expected p/name 'ok', got %v", result["p/name"])
	}
	for k := range result {
		if k == "p/Secret" || k == "p/-" {
			t.Errorf("ignored field should not appear in flat map, found key %q", k)
		}
	}
}

func TestStructToFlatMapOmitempty(t *testing.T) {
	type S struct {
		Name  string  `json:"name,omitempty"`
		Count int     `json:"count,omitempty"`
		Rate  float64 `json:"rate,omitempty"`
		Flag  bool    `json:"flag,omitempty"`
	}

	m := NewFieldMapper("json")
	result, err := m.StructToFlatMap(S{}, "p")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty flat map for zero-value struct, got %v", result)
	}
}

func TestFlatMapToStructNonPointerTarget(t *testing.T) {
	type S struct {
		Name string `json:"name"`
	}

	m := NewFieldMapper("json")
	var s S
	err := m.FlatMapToStruct(map[string]any{"name": "test"}, "", s)
	if err == nil {
		t.Error("expected error for non-pointer target")
	}
}

func TestFlatMapToStructNilTarget(t *testing.T) {
	m := NewFieldMapper("json")
	err := m.FlatMapToStruct(map[string]any{"name": "test"}, "", (*struct{})(nil))
	if err == nil {
		t.Error("expected error for nil target")
	}
}

func TestFlatMapToStructNonStructTarget(t *testing.T) {
	m := NewFieldMapper("json")
	var s string
	err := m.FlatMapToStruct(map[string]any{"name": "test"}, "", &s)
	if err == nil {
		t.Error("expected error for non-struct pointer target")
	}
}

func TestFlatMapToStructWithPrefix(t *testing.T) {
	type S struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"db/host":  "localhost",
		"db/port":  float64(5432),
		"app/name": "other",
	}
	var s S
	if err := m.FlatMapToStruct(data, "db", &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", s.Host)
	}
	if s.Port != 5432 {
		t.Errorf("expected port 5432, got %d", s.Port)
	}
}

func TestFlatMapToStructNoPrefix(t *testing.T) {
	type S struct {
		Name string `json:"name"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"name": "test",
	}
	var s S
	if err := m.FlatMapToStruct(data, "", &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "test" {
		t.Errorf("expected name 'test', got %q", s.Name)
	}
}

func TestFlatMapToStructNestedStruct(t *testing.T) {
	type Pool struct {
		Size    int `json:"size"`
		MaxIdle int `json:"max_idle"`
	}
	type DB struct {
		Host string `json:"host"`
		Pool Pool   `json:"pool"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"db/host":          "localhost",
		"db/pool/size":     float64(10),
		"db/pool/max_idle": float64(5),
	}
	var db DB
	if err := m.FlatMapToStruct(data, "db", &db); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if db.Host != "localhost" {
		t.Errorf("expected host 'localhost', got %q", db.Host)
	}
	if db.Pool.Size != 10 {
		t.Errorf("expected pool.size 10, got %d", db.Pool.Size)
	}
	if db.Pool.MaxIdle != 5 {
		t.Errorf("expected pool.max_idle 5, got %d", db.Pool.MaxIdle)
	}
}

func TestFlatMapToStructIgnoredField(t *testing.T) {
	type S struct {
		Name   string `json:"name"`
		Secret string `json:"-"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"p/name":   "test",
		"p/Secret": "should-be-ignored",
	}
	var s S
	if err := m.FlatMapToStruct(data, "p", &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "test" {
		t.Errorf("expected name 'test', got %q", s.Name)
	}
	if s.Secret != "" {
		t.Errorf("expected Secret to remain empty, got %q", s.Secret)
	}
}

func TestFlatMapToStructUnmatchedKeysIgnored(t *testing.T) {
	type S struct {
		Name string `json:"name"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"p/name":    "test",
		"p/unknown": "value",
	}
	var s S
	if err := m.FlatMapToStruct(data, "p", &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "test" {
		t.Errorf("expected name 'test', got %q", s.Name)
	}
}

func TestMapToStructBasic(t *testing.T) {
	type S struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"name": "alice",
		"age":  float64(30),
	}
	var s S
	if err := m.MapToStruct(data, &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != "alice" {
		t.Errorf("expected name 'alice', got %q", s.Name)
	}
	if s.Age != 30 {
		t.Errorf("expected age 30, got %d", s.Age)
	}
}

func TestMapToStructNonPointerTarget(t *testing.T) {
	type S struct{ Name string }
	m := NewFieldMapper("json")
	var s S
	err := m.MapToStruct(map[string]any{"Name": "test"}, s)
	if err == nil {
		t.Error("expected error for non-pointer target")
	}
}

func TestMapToStructNonStructTarget(t *testing.T) {
	m := NewFieldMapper("json")
	var s string
	err := m.MapToStruct(map[string]any{"Name": "test"}, &s)
	if err == nil {
		t.Error("expected error for non-struct pointer target")
	}
}

func TestMapToStructNestedMap(t *testing.T) {
	type Inner struct {
		Val string `json:"val"`
	}
	type Outer struct {
		Inner Inner `json:"inner"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"inner": map[string]any{
			"val": "nested",
		},
	}
	var o Outer
	if err := m.MapToStruct(data, &o); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o.Inner.Val != "nested" {
		t.Errorf("expected inner.val 'nested', got %q", o.Inner.Val)
	}
}

func TestMapToStructPointerField(t *testing.T) {
	type Inner struct {
		Val string `json:"val"`
	}
	type Outer struct {
		Inner *Inner `json:"inner"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"inner": map[string]any{
			"val": "ptr-nested",
		},
	}
	var o Outer
	if err := m.MapToStruct(data, &o); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o.Inner == nil {
		t.Fatal("expected inner to not be nil")
	}
	if o.Inner.Val != "ptr-nested" {
		t.Errorf("expected inner.val 'ptr-nested', got %q", o.Inner.Val)
	}
}

func TestMapToStructPointerToNonStruct(t *testing.T) {
	type S struct {
		Count *int `json:"count"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"count": 42,
	}
	var s S
	if err := m.MapToStruct(data, &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Count == nil {
		t.Fatal("expected count to not be nil")
	}
	if *s.Count != 42 {
		t.Errorf("expected count 42, got %d", *s.Count)
	}
}

func TestMapToStructNilValue(t *testing.T) {
	type S struct {
		Name *string `json:"name"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"name": nil,
	}
	var s S
	if err := m.MapToStruct(data, &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Name != nil {
		t.Errorf("expected nil name, got %v", s.Name)
	}
}

func TestMapToStructSliceField(t *testing.T) {
	type S struct {
		Tags []string `json:"tags"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"tags": []any{"a", "b", "c"},
	}
	var s S
	if err := m.MapToStruct(data, &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.Tags) != 3 {
		t.Fatalf("expected 3 tags, got %d", len(s.Tags))
	}
	if s.Tags[0] != "a" || s.Tags[1] != "b" || s.Tags[2] != "c" {
		t.Errorf("unexpected tags: %v", s.Tags)
	}
}

func TestMapToStructSliceOfStructs(t *testing.T) {
	type Item struct {
		Name string `json:"name"`
	}
	type S struct {
		Items []Item `json:"items"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"items": []any{
			map[string]any{"name": "first"},
			map[string]any{"name": "second"},
		},
	}
	var s S
	if err := m.MapToStruct(data, &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(s.Items))
	}
	if s.Items[0].Name != "first" {
		t.Errorf("expected first item name 'first', got %q", s.Items[0].Name)
	}
	if s.Items[1].Name != "second" {
		t.Errorf("expected second item name 'second', got %q", s.Items[1].Name)
	}
}

func TestMapToStructMapField(t *testing.T) {
	type S struct {
		Labels map[string]string `json:"labels"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"labels": map[string]any{"env": "prod", "tier": "frontend"},
	}
	var s S
	if err := m.MapToStruct(data, &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Labels["env"] != "prod" {
		t.Errorf("expected env 'prod', got %q", s.Labels["env"])
	}
	if s.Labels["tier"] != "frontend" {
		t.Errorf("expected tier 'frontend', got %q", s.Labels["tier"])
	}
}

func TestMapToStructNumericConversions(t *testing.T) {
	type S struct {
		IntVal    int     `json:"int_val"`
		UintVal   uint    `json:"uint_val"`
		FloatVal  float64 `json:"float_val"`
		Int32Val  int32   `json:"int32_val"`
		Uint64Val uint64  `json:"uint64_val"`
	}

	m := NewFieldMapper("json")
	// JSON numbers come as float64
	data := map[string]any{
		"int_val":    float64(42),
		"uint_val":   float64(100),
		"float_val":  float64(3.14),
		"int32_val":  float64(-10),
		"uint64_val": float64(999),
	}
	var s S
	if err := m.MapToStruct(data, &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.IntVal != 42 {
		t.Errorf("expected int_val 42, got %d", s.IntVal)
	}
	if s.UintVal != 100 {
		t.Errorf("expected uint_val 100, got %d", s.UintVal)
	}
	if s.FloatVal != 3.14 {
		t.Errorf("expected float_val 3.14, got %f", s.FloatVal)
	}
	if s.Int32Val != -10 {
		t.Errorf("expected int32_val -10, got %d", s.Int32Val)
	}
	if s.Uint64Val != 999 {
		t.Errorf("expected uint64_val 999, got %d", s.Uint64Val)
	}
}

func TestMapToStructCaseInsensitiveMatch(t *testing.T) {
	type S struct {
		Name string `json:"name"`
	}

	m := NewFieldMapper("json")
	data := map[string]any{
		"Name": "uppercase",
	}
	var s S
	if err := m.MapToStruct(data, &s); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The code maps both the tag name and its lowercase version
	// "Name" should match via lowercase fallback to "name"
	if s.Name != "uppercase" {
		t.Errorf("expected name 'uppercase', got %q", s.Name)
	}
}

func TestSetNumericValueIntFromInt(t *testing.T) {
	var target int64
	fieldVal := reflect.ValueOf(&target).Elem()
	valueVal := reflect.ValueOf(int64(42))
	if err := setNumericValue(fieldVal, valueVal); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != 42 {
		t.Errorf("expected 42, got %d", target)
	}
}

func TestSetNumericValueIntFromUint(t *testing.T) {
	var target int64
	fieldVal := reflect.ValueOf(&target).Elem()
	valueVal := reflect.ValueOf(uint64(42))
	if err := setNumericValue(fieldVal, valueVal); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != 42 {
		t.Errorf("expected 42, got %d", target)
	}
}

func TestSetNumericValueUintFromFloat(t *testing.T) {
	var target uint64
	fieldVal := reflect.ValueOf(&target).Elem()
	valueVal := reflect.ValueOf(float64(99))
	if err := setNumericValue(fieldVal, valueVal); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != 99 {
		t.Errorf("expected 99, got %d", target)
	}
}

func TestSetNumericValueUintFromInt(t *testing.T) {
	var target uint64
	fieldVal := reflect.ValueOf(&target).Elem()
	valueVal := reflect.ValueOf(int64(50))
	if err := setNumericValue(fieldVal, valueVal); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != 50 {
		t.Errorf("expected 50, got %d", target)
	}
}

func TestSetNumericValueUintFromUint(t *testing.T) {
	var target uint64
	fieldVal := reflect.ValueOf(&target).Elem()
	valueVal := reflect.ValueOf(uint64(77))
	if err := setNumericValue(fieldVal, valueVal); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != 77 {
		t.Errorf("expected 77, got %d", target)
	}
}

func TestSetNumericValueFloatFromInt(t *testing.T) {
	var target float64
	fieldVal := reflect.ValueOf(&target).Elem()
	valueVal := reflect.ValueOf(int64(10))
	if err := setNumericValue(fieldVal, valueVal); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != 10.0 {
		t.Errorf("expected 10.0, got %f", target)
	}
}

func TestSetNumericValueFloatFromUint(t *testing.T) {
	var target float64
	fieldVal := reflect.ValueOf(&target).Elem()
	valueVal := reflect.ValueOf(uint64(20))
	if err := setNumericValue(fieldVal, valueVal); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != 20.0 {
		t.Errorf("expected 20.0, got %f", target)
	}
}

func TestSetNumericValueFloatFromFloat(t *testing.T) {
	var target float64
	fieldVal := reflect.ValueOf(&target).Elem()
	valueVal := reflect.ValueOf(float64(3.14))
	if err := setNumericValue(fieldVal, valueVal); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if target != 3.14 {
		t.Errorf("expected 3.14, got %f", target)
	}
}

func TestIsEmptyValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected bool
	}{
		{"empty string", "", true},
		{"non-empty string", "hello", false},
		{"zero int", int(0), true},
		{"non-zero int", int(1), false},
		{"zero uint", uint(0), true},
		{"non-zero uint", uint(1), false},
		{"zero float", float64(0), true},
		{"non-zero float", float64(1.5), false},
		{"false bool", false, true},
		{"true bool", true, false},
		{"nil slice", []string(nil), true},
		{"empty slice", []string{}, true},
		{"non-empty slice", []string{"a"}, false},
		{"nil map", map[string]string(nil), true},
		{"empty map", map[string]string{}, true},
		{"non-empty map", map[string]string{"k": "v"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := reflect.ValueOf(tt.val)
			got := isEmptyValue(v)
			if got != tt.expected {
				t.Errorf("isEmptyValue(%v) = %v, want %v", tt.val, got, tt.expected)
			}
		})
	}
}

func TestIsEmptyValuePointer(t *testing.T) {
	var nilPtr *string
	v := reflect.ValueOf(nilPtr)
	if !isEmptyValue(v) {
		t.Error("expected nil pointer to be empty")
	}

	s := "hello"
	v = reflect.ValueOf(&s)
	if isEmptyValue(v) {
		t.Error("expected non-nil pointer to not be empty")
	}
}

func TestIsNumeric(t *testing.T) {
	numericKinds := []reflect.Kind{
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
	}
	for _, k := range numericKinds {
		if !isNumeric(k) {
			t.Errorf("expected %v to be numeric", k)
		}
	}

	nonNumericKinds := []reflect.Kind{
		reflect.String, reflect.Bool, reflect.Struct, reflect.Slice, reflect.Map,
	}
	for _, k := range nonNumericKinds {
		if isNumeric(k) {
			t.Errorf("expected %v to not be numeric", k)
		}
	}
}

func TestParseTag(t *testing.T) {
	tests := []struct {
		tag      string
		name     string
		optsLen  int
		firstOpt string
	}{
		{"name", "name", 0, ""},
		{"name,omitempty", "name", 1, "omitempty"},
		{"name,omitempty,nonrecursive", "name", 2, "omitempty"},
		{",omitempty", "", 1, "omitempty"},
		{"", "", 0, ""},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			name, opts := parseTag(tt.tag)
			if name != tt.name {
				t.Errorf("expected name %q, got %q", tt.name, name)
			}
			if len(opts) != tt.optsLen {
				t.Errorf("expected %d opts, got %d", tt.optsLen, len(opts))
			}
			if tt.firstOpt != "" && (len(opts) == 0 || opts[0] != tt.firstOpt) {
				t.Errorf("expected first opt %q", tt.firstOpt)
			}
		})
	}
}

func TestHasOption(t *testing.T) {
	opts := []string{"omitempty", "nonrecursive"}
	if !hasOption(opts, "omitempty") {
		t.Error("expected to find 'omitempty'")
	}
	if !hasOption(opts, "nonrecursive") {
		t.Error("expected to find 'nonrecursive'")
	}
	if hasOption(opts, "unknown") {
		t.Error("did not expect to find 'unknown'")
	}
	if hasOption(nil, "any") {
		t.Error("did not expect to find anything in nil slice")
	}
}

func TestBuildNestedMap(t *testing.T) {
	data := map[string]any{
		"db/host":       "localhost",
		"db/port":       5432,
		"db/pool/size":  10,
		"app/name":      "myapp",
		"unrelated/key": "val",
	}

	result := buildNestedMap(data, "db")
	if result["host"] != "localhost" {
		t.Errorf("expected host 'localhost', got %v", result["host"])
	}
	if result["port"] != 5432 {
		t.Errorf("expected port 5432, got %v", result["port"])
	}
	pool, ok := result["pool"].(map[string]any)
	if !ok {
		t.Fatalf("expected pool to be map, got %T", result["pool"])
	}
	if pool["size"] != 10 {
		t.Errorf("expected pool.size 10, got %v", pool["size"])
	}
	// app/ and unrelated/ should not be in the result
	if _, ok := result["name"]; ok {
		t.Error("app/name should not appear under db prefix")
	}
}

func TestBuildNestedMapEmptyPrefix(t *testing.T) {
	data := map[string]any{
		"host": "localhost",
		"port": 5432,
	}

	result := buildNestedMap(data, "")
	if result["host"] != "localhost" {
		t.Errorf("expected host 'localhost', got %v", result["host"])
	}
	if result["port"] != 5432 {
		t.Errorf("expected port 5432, got %v", result["port"])
	}
}

func TestBuildNestedMapPathConflict(t *testing.T) {
	// If a key appears both as a leaf and as a prefix
	data := map[string]any{
		"db":      "value",
		"db/host": "localhost",
	}

	// This should not panic; the later key should overwrite
	result := buildNestedMap(data, "")
	// db became an intermediate node overwriting the leaf value
	dbMap, ok := result["db"].(map[string]any)
	if !ok {
		t.Fatalf("expected db to be map after conflict, got %T", result["db"])
	}
	if dbMap["host"] != "localhost" {
		t.Errorf("expected db.host 'localhost', got %v", dbMap["host"])
	}
}

func TestFlatMapToStructRoundTrip(t *testing.T) {
	type Inner struct {
		A string `json:"a"`
		B int    `json:"b"`
	}
	type S struct {
		Name  string `json:"name"`
		Inner Inner  `json:"inner"`
		Count int    `json:"count"`
	}

	original := S{
		Name:  "roundtrip",
		Inner: Inner{A: "hello", B: 42},
		Count: 7,
	}

	m := NewFieldMapper("json")
	flat, err := m.StructToFlatMap(original, "cfg")
	if err != nil {
		t.Fatalf("StructToFlatMap error: %v", err)
	}

	var result S
	if err := m.FlatMapToStruct(flat, "cfg", &result); err != nil {
		t.Fatalf("FlatMapToStruct error: %v", err)
	}

	if result.Name != original.Name {
		t.Errorf("expected name %q, got %q", original.Name, result.Name)
	}
	if result.Inner.A != original.Inner.A {
		t.Errorf("expected inner.a %q, got %q", original.Inner.A, result.Inner.A)
	}
	if result.Inner.B != original.Inner.B {
		t.Errorf("expected inner.b %d, got %d", original.Inner.B, result.Inner.B)
	}
	if result.Count != original.Count {
		t.Errorf("expected count %d, got %d", original.Count, result.Count)
	}
}
