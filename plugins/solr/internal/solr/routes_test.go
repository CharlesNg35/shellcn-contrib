package solr

import (
	"errors"
	"testing"

	"github.com/charlesng35/shellcn/sdk/plugin"
)

func TestFieldDefinition(t *testing.T) {
	cases := []struct {
		name string
		spec fieldSpec
		want row
	}{
		{
			name: "minimal omits optional flags",
			spec: fieldSpec{Name: "title_s", Type: "string", Indexed: true, Stored: true},
			want: row{"name": "title_s", "type": "string", "indexed": true, "stored": true},
		},
		{
			name: "includes multiValued and required when set",
			spec: fieldSpec{Name: "tags_ss", Type: "strings", Indexed: true, Stored: false, MultiValued: true, Required: true},
			want: row{"name": "tags_ss", "type": "strings", "indexed": true, "stored": false, "multiValued": true, "required": true},
		},
		{
			name: "trims name and type",
			spec: fieldSpec{Name: "  age_i ", Type: " pint ", Indexed: false, Stored: true},
			want: row{"name": "age_i", "type": "pint", "indexed": false, "stored": true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := fieldDefinition(tc.spec)
			if len(got) != len(tc.want) {
				t.Fatalf("key count: got %#v want %#v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("key %q: got %#v want %#v", k, got[k], v)
				}
			}
		})
	}
}

func TestValidFieldDefinition(t *testing.T) {
	cases := []struct {
		name    string
		spec    fieldSpec
		wantErr bool
	}{
		{name: "valid", spec: fieldSpec{Name: "title_s", Type: "string"}},
		{name: "missing name", spec: fieldSpec{Type: "string"}, wantErr: true},
		{name: "blank name", spec: fieldSpec{Name: "   ", Type: "string"}, wantErr: true},
		{name: "missing type", spec: fieldSpec{Name: "title_s"}, wantErr: true},
		{name: "blank type", spec: fieldSpec{Name: "title_s", Type: "  "}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validFieldDefinition(tc.spec)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !errors.Is(err, plugin.ErrInvalidInput) {
					t.Fatalf("expected ErrInvalidInput, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
