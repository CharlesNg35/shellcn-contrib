package milvus

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/client/v2/entity"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

// treeNodeLimit bounds sidebar fan-out; the paginated list views stay the
// authority for large collections.
const treeNodeLimit = 200

func icon(name string) plugin.Icon { return plugin.Icon{Type: plugin.IconLucide, Value: name} }

type graphField struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
	Key  string `json:"key,omitempty"`
}

type graphNode struct {
	ID         string         `json:"id"`
	Label      string         `json:"label"`
	Group      string         `json:"group,omitempty"`
	Summary    string         `json:"summary,omitempty"`
	Fields     []graphField   `json:"fields,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

type graphEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Label  string `json:"label,omitempty"`
}

type graphPayload struct {
	Nodes []graphNode `json:"nodes"`
	Edges []graphEdge `json:"edges"`
}

// fieldRequest is the schema-designer row shared by collection creation and the
// add-field form.
type fieldRequest struct {
	Name          string `json:"name" validate:"required"`
	Type          string `json:"type" validate:"required"`
	Dim           int64  `json:"dim"`
	MaxLength     int64  `json:"max_length"`
	ElementType   string `json:"element_type"`
	MaxCapacity   int64  `json:"max_capacity"`
	Primary       bool   `json:"primary"`
	AutoID        bool   `json:"auto_id"`
	Nullable      bool   `json:"nullable"`
	PartitionKey  bool   `json:"partition_key"`
	ClusteringKey bool   `json:"clustering_key"`
	Description   string `json:"description"`
}

func (r fieldRequest) build() (*entity.Field, error) {
	name, err := identifier("field", r.Name)
	if err != nil {
		return nil, err
	}
	dataType, err := fieldTypeFromName(r.Type)
	if err != nil {
		return nil, err
	}
	f := entity.NewField().
		WithName(name).
		WithDataType(dataType).
		WithDescription(r.Description).
		WithIsPrimaryKey(r.Primary).
		WithIsPartitionKey(r.PartitionKey).
		WithIsClusteringKey(r.ClusteringKey).
		WithNullable(r.Nullable)

	if r.Primary {
		if dataType != entity.FieldTypeInt64 && dataType != entity.FieldTypeVarChar {
			return nil, fmt.Errorf("%w: a primary key must be Int64 or VarChar", plugin.ErrInvalidInput)
		}
		f = f.WithIsAutoID(r.AutoID && dataType == entity.FieldTypeInt64).WithNullable(false)
	}

	switch dataType {
	case entity.FieldTypeFloatVector, entity.FieldTypeBinaryVector,
		entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector, entity.FieldTypeInt8Vector:
		if r.Dim < 1 {
			return nil, fmt.Errorf("%w: field %q needs a dimension", plugin.ErrInvalidInput, name)
		}
		f = f.WithDim(r.Dim)
	case entity.FieldTypeVarChar:
		maxLength := r.MaxLength
		if maxLength < 1 {
			maxLength = 512
		}
		f = f.WithMaxLength(maxLength)
	case entity.FieldTypeArray:
		elementType, err := fieldTypeFromName(r.ElementType)
		if err != nil {
			return nil, fmt.Errorf("%w: field %q needs a valid element type", plugin.ErrInvalidInput, name)
		}
		if elementType.IsVectorType() || elementType == entity.FieldTypeArray || elementType == entity.FieldTypeJSON {
			return nil, fmt.Errorf("%w: array elements must be a scalar type", plugin.ErrInvalidInput)
		}
		capacity := r.MaxCapacity
		if capacity < 1 {
			capacity = 64
		}
		f = f.WithElementType(elementType).WithMaxCapacity(capacity)
		if elementType == entity.FieldTypeVarChar {
			maxLength := r.MaxLength
			if maxLength < 1 {
				maxLength = 512
			}
			f = f.WithMaxLength(maxLength)
		}
	}
	return f, nil
}

func fieldTypeFromName(name string) (entity.FieldType, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "bool":
		return entity.FieldTypeBool, nil
	case "int8":
		return entity.FieldTypeInt8, nil
	case "int16":
		return entity.FieldTypeInt16, nil
	case "int32":
		return entity.FieldTypeInt32, nil
	case "int64":
		return entity.FieldTypeInt64, nil
	case "float":
		return entity.FieldTypeFloat, nil
	case "double":
		return entity.FieldTypeDouble, nil
	case "varchar", "string":
		return entity.FieldTypeVarChar, nil
	case "json":
		return entity.FieldTypeJSON, nil
	case "array":
		return entity.FieldTypeArray, nil
	case "floatvector", "float_vector":
		return entity.FieldTypeFloatVector, nil
	case "binaryvector", "binary_vector":
		return entity.FieldTypeBinaryVector, nil
	case "float16vector", "float16_vector":
		return entity.FieldTypeFloat16Vector, nil
	case "bfloat16vector", "bfloat16_vector":
		return entity.FieldTypeBFloat16Vector, nil
	case "int8vector", "int8_vector":
		return entity.FieldTypeInt8Vector, nil
	case "sparsefloatvector", "sparse_float_vector", "sparsevector":
		return entity.FieldTypeSparseVector, nil
	default:
		return entity.FieldTypeNone, fmt.Errorf("%w: unsupported field type %q", plugin.ErrInvalidInput, name)
	}
}

func fieldTypeName(t entity.FieldType) string {
	if t == entity.FieldTypeSparseVector {
		return "SparseFloatVector"
	}
	return t.Name()
}

func round1(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*10) / 10
}

func parseInt(raw string) (int, bool) {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, false
	}
	return v, true
}
