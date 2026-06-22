#!/bin/bash -eu
compile_native_go_fuzzer github.com/rbaliyan/config/codec/json FuzzJSONCodecDecode fuzz_json_codec_decode
compile_native_go_fuzzer github.com/rbaliyan/config/codec/yaml FuzzYAMLCodecDecode fuzz_yaml_codec_decode
compile_native_go_fuzzer github.com/rbaliyan/config/codec/toml FuzzTOMLCodecDecode fuzz_toml_codec_decode
compile_native_go_fuzzer github.com/rbaliyan/config FuzzValidateKey fuzz_validate_key
compile_native_go_fuzzer github.com/rbaliyan/config FuzzValidateNamespace fuzz_validate_namespace
compile_native_go_fuzzer github.com/rbaliyan/config FuzzNewValueFromBytes fuzz_new_value_from_bytes
compile_native_go_fuzzer github.com/rbaliyan/config/internal/cursor FuzzCursorUnmarshal fuzz_cursor_unmarshal
compile_native_go_fuzzer github.com/rbaliyan/config/bind FuzzBindFlatMap fuzz_bind_flat_map
compile_native_go_fuzzer github.com/rbaliyan/config/expand FuzzExpandDollar fuzz_expand_dollar
compile_native_go_fuzzer github.com/rbaliyan/config/expand FuzzExpandAngle fuzz_expand_angle

# Install libFuzzer dictionaries next to their fuzzer binaries (OSS-Fuzz/CFLite
# convention: a <fuzzer_name>.dict in $OUT is auto-loaded for that target).
# The dicts live in the repo checkout ($SRC/config), not next to the copied
# build.sh ($SRC/build.sh), so reference them via the source tree.
dict_dir="$SRC/config/.clusterfuzzlite"
cp "$dict_dir/codec.dict" "$OUT/fuzz_json_codec_decode.dict"
cp "$dict_dir/codec.dict" "$OUT/fuzz_yaml_codec_decode.dict"
cp "$dict_dir/codec.dict" "$OUT/fuzz_toml_codec_decode.dict"
cp "$dict_dir/cursor.dict" "$OUT/fuzz_cursor_unmarshal.dict"
cp "$dict_dir/expand.dict" "$OUT/fuzz_expand_dollar.dict"
cp "$dict_dir/expand.dict" "$OUT/fuzz_expand_angle.dict"
