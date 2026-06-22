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
cp "$(dirname "$0")/codec.dict" "$OUT/fuzz_json_codec_decode.dict"
cp "$(dirname "$0")/codec.dict" "$OUT/fuzz_yaml_codec_decode.dict"
cp "$(dirname "$0")/codec.dict" "$OUT/fuzz_toml_codec_decode.dict"
cp "$(dirname "$0")/cursor.dict" "$OUT/fuzz_cursor_unmarshal.dict"
cp "$(dirname "$0")/expand.dict" "$OUT/fuzz_expand_dollar.dict"
cp "$(dirname "$0")/expand.dict" "$OUT/fuzz_expand_angle.dict"
