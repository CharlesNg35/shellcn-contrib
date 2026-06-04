package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func marshalItem(input map[string]any) (map[string]types.AttributeValue, error) {
	if looksLikeDynamoDBJSON(input) {
		out := map[string]types.AttributeValue{}
		for key, raw := range input {
			av, err := jsonToAV(raw)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid DynamoDB JSON for %q: %v", plugin.ErrInvalidInput, key, err)
			}
			out[key] = av
		}
		return out, nil
	}
	out, err := attributevalue.MarshalMap(input)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid item JSON: %v", plugin.ErrInvalidInput, err)
	}
	return out, nil
}

func unmarshalItem(item map[string]types.AttributeValue) row {
	var out map[string]any
	if err := attributevalue.UnmarshalMap(item, &out); err != nil {
		out = map[string]any{}
	}
	for key, av := range item {
		if _, ok := out[key]; !ok {
			out[key] = avToPlain(av)
		}
	}
	return row(out)
}

func encodeItemID(table string, key map[string]types.AttributeValue) (string, error) {
	rawKey := map[string]any{}
	for name, av := range key {
		rawKey[name] = avToDDBJSON(av)
	}
	data, err := json.Marshal(row{"table": table, "key": rawKey})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeItemID(id string) (string, map[string]types.AttributeValue, error) {
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(id))
	if err != nil {
		return "", nil, fmt.Errorf("%w: invalid item id", plugin.ErrInvalidInput)
	}
	var raw struct {
		Table string         `json:"table"`
		Key   map[string]any `json:"key"`
	}
	if err := json.Unmarshal(data, &raw); err != nil || raw.Table == "" || len(raw.Key) == 0 {
		return "", nil, fmt.Errorf("%w: invalid item id", plugin.ErrInvalidInput)
	}
	key := map[string]types.AttributeValue{}
	for name, value := range raw.Key {
		av, err := jsonToAV(value)
		if err != nil {
			return "", nil, err
		}
		key[name] = av
	}
	return raw.Table, key, nil
}

func itemKey(item map[string]types.AttributeValue, schema []types.KeySchemaElement) (map[string]types.AttributeValue, bool) {
	key := map[string]types.AttributeValue{}
	for _, part := range schema {
		name := strings.TrimSpace(awsString(part.AttributeName))
		if name == "" {
			continue
		}
		av, ok := item[name]
		if !ok {
			return nil, false
		}
		key[name] = av
	}
	return key, len(key) > 0
}

func keyDisplay(key map[string]types.AttributeValue, schema []types.KeySchemaElement) string {
	parts := []string{}
	for _, part := range schema {
		name := awsString(part.AttributeName)
		if av, ok := key[name]; ok {
			parts = append(parts, name+"="+fmt.Sprint(avToPlain(av)))
		}
	}
	if len(parts) == 0 {
		for name, av := range key {
			parts = append(parts, name+"="+fmt.Sprint(avToPlain(av)))
		}
	}
	return strings.Join(parts, " · ")
}

func looksLikeDynamoDBJSON(input map[string]any) bool {
	if len(input) == 0 {
		return false
	}
	for _, value := range input {
		if !isDynamoDBAttrObject(value) {
			return false
		}
	}
	return true
}

func isDynamoDBAttrObject(value any) bool {
	m, ok := value.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	for key := range m {
		switch key {
		case "S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS":
			return true
		default:
			return false
		}
	}
	return false
}

func jsonToAV(value any) (types.AttributeValue, error) {
	m, ok := value.(map[string]any)
	if !ok || len(m) != 1 {
		return attributevalue.Marshal(value)
	}
	for key, raw := range m {
		switch key {
		case "S":
			return &types.AttributeValueMemberS{Value: fmt.Sprint(raw)}, nil
		case "N":
			if _, err := strconv.ParseFloat(fmt.Sprint(raw), 64); err != nil {
				return nil, fmt.Errorf("number must be encoded as a numeric string")
			}
			return &types.AttributeValueMemberN{Value: fmt.Sprint(raw)}, nil
		case "B":
			data, err := base64.StdEncoding.DecodeString(fmt.Sprint(raw))
			if err != nil {
				return nil, err
			}
			return &types.AttributeValueMemberB{Value: data}, nil
		case "BOOL":
			b, ok := raw.(bool)
			if !ok {
				return nil, fmt.Errorf("BOOL must be boolean")
			}
			return &types.AttributeValueMemberBOOL{Value: b}, nil
		case "NULL":
			return &types.AttributeValueMemberNULL{Value: true}, nil
		case "M":
			rawMap, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("map attribute must be an object")
			}
			out := map[string]types.AttributeValue{}
			for name, nested := range rawMap {
				av, err := jsonToAV(nested)
				if err != nil {
					return nil, err
				}
				out[name] = av
			}
			return &types.AttributeValueMemberM{Value: out}, nil
		case "L":
			rawList, ok := raw.([]any)
			if !ok {
				return nil, fmt.Errorf("list attribute must be an array")
			}
			out := make([]types.AttributeValue, 0, len(rawList))
			for _, nested := range rawList {
				av, err := jsonToAV(nested)
				if err != nil {
					return nil, err
				}
				out = append(out, av)
			}
			return &types.AttributeValueMemberL{Value: out}, nil
		case "SS":
			values, err := stringList(raw)
			if err != nil {
				return nil, err
			}
			return &types.AttributeValueMemberSS{Value: values}, nil
		case "NS":
			values, err := stringList(raw)
			if err != nil {
				return nil, err
			}
			for _, value := range values {
				if _, err := strconv.ParseFloat(value, 64); err != nil {
					return nil, fmt.Errorf("NS contains non-numeric value %q", value)
				}
			}
			return &types.AttributeValueMemberNS{Value: values}, nil
		case "BS":
			values, err := stringList(raw)
			if err != nil {
				return nil, err
			}
			out := make([][]byte, 0, len(values))
			for _, value := range values {
				data, err := base64.StdEncoding.DecodeString(value)
				if err != nil {
					return nil, err
				}
				out = append(out, data)
			}
			return &types.AttributeValueMemberBS{Value: out}, nil
		}
	}
	return attributevalue.Marshal(value)
}

func avToPlain(av types.AttributeValue) any {
	var out any
	if err := attributevalue.Unmarshal(av, &out); err == nil {
		return out
	}
	return avToDDBJSON(av)
}

func avToDDBJSON(av types.AttributeValue) any {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		return row{"S": v.Value}
	case *types.AttributeValueMemberN:
		return row{"N": v.Value}
	case *types.AttributeValueMemberB:
		return row{"B": base64.StdEncoding.EncodeToString(v.Value)}
	case *types.AttributeValueMemberBOOL:
		return row{"BOOL": v.Value}
	case *types.AttributeValueMemberNULL:
		return row{"NULL": true}
	case *types.AttributeValueMemberM:
		out := row{}
		for key, nested := range v.Value {
			out[key] = avToDDBJSON(nested)
		}
		return row{"M": out}
	case *types.AttributeValueMemberL:
		out := make([]any, 0, len(v.Value))
		for _, nested := range v.Value {
			out = append(out, avToDDBJSON(nested))
		}
		return row{"L": out}
	case *types.AttributeValueMemberSS:
		return row{"SS": v.Value}
	case *types.AttributeValueMemberNS:
		return row{"NS": v.Value}
	case *types.AttributeValueMemberBS:
		out := make([]string, 0, len(v.Value))
		for _, data := range v.Value {
			out = append(out, base64.StdEncoding.EncodeToString(data))
		}
		return row{"BS": out}
	default:
		return row{"NULL": true}
	}
}

func stringList(raw any) ([]string, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("value must be an array")
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, fmt.Sprint(value))
	}
	return out, nil
}
