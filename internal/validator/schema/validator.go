package schema

import (
	"encoding/json"
	"fmt"
	"time"

	jobschema "github.com/john-peterson-g17/morph/internal/schema"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

// Validator validates job file data against the embedded JSON schema.
type Validator struct{}

func (v *Validator) Validate(data []byte) error {
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("parsing YAML: %w", err)
	}

	doc = convertYAML(doc)

	var schemaDef any
	if err := json.Unmarshal(jobschema.JobSchema, &schemaDef); err != nil {
		return fmt.Errorf("loading schema: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("job.schema.json", schemaDef); err != nil {
		return fmt.Errorf("adding schema resource: %w", err)
	}

	sch, err := compiler.Compile("job.schema.json")
	if err != nil {
		return fmt.Errorf("compiling schema: %w", err)
	}

	if err := sch.Validate(doc); err != nil {
		return fmt.Errorf("schema validation failed:\n%s", err)
	}

	return nil
}

// convertYAML recursively converts YAML-decoded values to JSON-compatible types.
func convertYAML(v any) any {
	switch v := v.(type) {
	case map[string]any:
		for k, val := range v {
			v[k] = convertYAML(val)
		}
		return v
	case map[any]any:
		m := make(map[string]any, len(v))
		for k, val := range v {
			m[fmt.Sprintf("%v", k)] = convertYAML(val)
		}
		return m
	case []any:
		for i, val := range v {
			v[i] = convertYAML(val)
		}
		return v
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return v
	}
}
