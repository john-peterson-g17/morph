package validator

// Validator validates raw job file data and returns an error if invalid.
type Validator interface {
	Validate(data []byte) error
}
