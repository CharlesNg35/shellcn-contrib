package milvus

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
)

type signed interface {
	~int8 | ~int16 | ~int32 | ~int64
}

type real interface{ ~float32 | ~float64 }

func asBool(v any) (bool, error) {
	switch value := v.(type) {
	case bool:
		return value, nil
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return false, fmt.Errorf("expected a boolean, got %q", value)
		}
		return parsed, nil
	case float64:
		return value != 0, nil
	default:
		return false, fmt.Errorf("expected a boolean, got %T", v)
	}
}

func asInt64(v any) (int64, error) { return asInt[int64](v) }

func asInt[T signed](v any) (T, error) {
	switch value := v.(type) {
	case float64:
		if value != math.Trunc(value) {
			return 0, fmt.Errorf("expected an integer, got %v", value)
		}
		return T(int64(value)), nil
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, fmt.Errorf("expected an integer, got %q", value.String())
		}
		return T(parsed), nil
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("expected an integer, got %q", value)
		}
		return T(parsed), nil
	case bool:
		if value {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("expected an integer, got %T", v)
	}
}

func asFloat[T real](v any) (T, error) {
	switch value := v.(type) {
	case float64:
		return T(value), nil
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, fmt.Errorf("expected a number, got %q", value.String())
		}
		return T(parsed), nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			return 0, fmt.Errorf("expected a number, got %q", value)
		}
		return T(parsed), nil
	default:
		return 0, fmt.Errorf("expected a number, got %T", v)
	}
}

func asString(v any) (string, error) {
	switch value := v.(type) {
	case string:
		return value, nil
	case float64:
		if value == math.Trunc(value) {
			return strconv.FormatInt(int64(value), 10), nil
		}
		return strconv.FormatFloat(value, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(value), nil
	default:
		return "", fmt.Errorf("expected a string, got %T", v)
	}
}

func asJSONBytes(v any) ([]byte, error) {
	if text, ok := v.(string); ok {
		trimmed := strings.TrimSpace(text)
		if json.Valid([]byte(trimmed)) {
			return []byte(trimmed), nil
		}
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("expected JSON, got %T", v)
	}
	return data, nil
}

func asFloat32Slice(v any) ([]float32, error) {
	switch value := v.(type) {
	case []float32:
		return value, nil
	case entity.FloatVector:
		// Query and search results carry the named vector type, not a raw slice.
		return value, nil
	case []any:
		out := make([]float32, 0, len(value))
		for _, item := range value {
			number, err := asFloat[float32](item)
			if err != nil {
				return nil, err
			}
			out = append(out, number)
		}
		return out, nil
	case []float64:
		out := make([]float32, 0, len(value))
		for _, item := range value {
			out = append(out, float32(item))
		}
		return out, nil
	case string:
		var decoded []float64
		if err := json.Unmarshal([]byte(strings.TrimSpace(value)), &decoded); err != nil {
			return nil, fmt.Errorf("expected a JSON array of numbers")
		}
		return asFloat32Slice(decoded)
	default:
		return nil, fmt.Errorf("expected an array of numbers, got %T", v)
	}
}

func round4(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*10000) / 10000
}

func nowUnixNano() int64 { return time.Now().UnixNano() }
