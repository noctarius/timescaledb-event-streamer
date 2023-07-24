package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
)

func Test_Constants_Properties(
	t *testing.T,
) {

	file, err := parser.ParseFile(&token.FileSet{}, "./constants.go", nil, 0)
	if err != nil {
		t.Error(err)
	}

	ast.Walk(&visitor{t: t}, file)
}

type visitor struct {
	config Config
	t      *testing.T
}

func (v *visitor) Visit(
	node ast.Node,
) (w ast.Visitor) {

	if valueSpec, ok := node.(*ast.ValueSpec); ok {
		name := valueSpec.Names[0].Name
		literal := valueSpec.Values[0].(*ast.BasicLit)

		element := reflect.ValueOf(v.config)
		value := literal.Value[1 : len(literal.Value)-1]

		properties := strings.Split(value, ".")
		for _, property := range properties {
			if e, ok := findProperty(element, property); ok {
				element = e
			} else {
				v.t.Errorf("Property %s isn't defined in Config", name)
				break
			}
		}
	}
	return v
}
