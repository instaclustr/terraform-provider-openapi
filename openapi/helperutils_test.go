package openapi

import (
	"encoding/json"
	"github.com/hashicorp/go-cty/cty"
	ctyjson "github.com/hashicorp/go-cty/cty/json"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

// testSchemaDefinition defines a test schema that contains a list of properties.
type testSchemaDefinition []*SpecSchemaDefinitionProperty

// primitive testing vars
var idProperty = newStringSchemaDefinitionProperty("id", "", true, false, false, false, false, true, false, false, "id")
var stringProperty = newStringSchemaDefinitionPropertyWithDefaults("string_property", "", true, false, "updatedValue")
var intProperty = newIntSchemaDefinitionPropertyWithDefaults("int_property", "", true, false, 12)
var numberProperty = newNumberSchemaDefinitionPropertyWithDefaults("number_property", "", true, false, 13.99)
var boolProperty = newBoolSchemaDefinitionPropertyWithDefaults("bool_property", "", true, false, true)
var slicePrimitiveProperty = newListSchemaDefinitionPropertyWithDefaults("slice_property", "", true, false, false, []interface{}{"value1"}, TypeString, nil)

// testing properties with special configuration
var stringWithPreferredNameProperty = newStringSchemaDefinitionPropertyWithDefaults("stringProperty", "string_preferred_property", true, false, "updatedValue")
var someIdentifierProperty = newStringSchemaDefinitionProperty("somePropertyThatShouldBeUsedAsID", "", true, true, false, false, false, false, true, false, "idValue")
var immutableProperty = newStringSchemaDefinitionProperty("string_immutable_property", "", true, false, false, false, false, true, false, false, "updatedImmutableValue")
var computedProperty = newStringSchemaDefinitionPropertyWithDefaults("computed_property", "", false, true, nil)
var readOnlyProperty = newStringSchemaDefinitionPropertyWithDefaults("read_only_property", "", false, true, "some_value")
var optionalProperty = newStringSchemaDefinitionPropertyWithDefaults("optional_property", "", false, false, "updatedValue")
var sensitiveProperty = newStringSchemaDefinitionProperty("sensitive_property", "", false, false, false, false, true, false, false, false, "sensitive")
var forceNewProperty = newBoolSchemaDefinitionProperty("bool_force_new_property", "", true, false, false, true, false, false, false, false, true)
var statusProperty = newStringSchemaDefinitionPropertyWithDefaults("status", "", false, true, "pending")

// testing properties with zero values set
var stringZeroValueProperty = newStringSchemaDefinitionPropertyWithDefaults("string_property", "", true, false, "")
var intZeroValueProperty = newIntSchemaDefinitionPropertyWithDefaults("int_property", "", true, false, 0)
var numberZeroValueProperty = newNumberSchemaDefinitionPropertyWithDefaults("number_property", "", true, false, 0)
var boolZeroValueProperty = newBoolSchemaDefinitionPropertyWithDefaults("bool_property", "", true, false, false)
var sliceZeroValueProperty = newListSchemaDefinitionPropertyWithDefaults("slice_property", "", true, false, false, []interface{}{""}, TypeString, nil)

func setSchemaDefinitionPropertyWriteOnly(propertySchemaDefinition *SpecSchemaDefinitionProperty) *SpecSchemaDefinitionProperty {
	propertySchemaDefinition.WriteOnly = true
	return propertySchemaDefinition
}

func newStringSchemaDefinitionPropertyWithDefaults(name, preferredName string, required, readOnly bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return newStringSchemaDefinitionProperty(name, preferredName, required, readOnly, false, false, false, false, false, false, defaultValue)
}

func newParentStringSchemaDefinitionPropertyWithDefaults(name, preferredName string, required, readOnly bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	p := newStringSchemaDefinitionPropertyWithDefaults(name, preferredName, required, readOnly, defaultValue)
	p.IsParentProperty = true
	return p
}

func newStringSchemaDefinitionProperty(name, preferredName string, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return newSchemaDefinitionProperty(name, preferredName, TypeString, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier, defaultValue)
}

func newIntSchemaDefinitionPropertyWithDefaults(name, preferredName string, required, readOnly bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return newIntSchemaDefinitionProperty(name, preferredName, required, readOnly, false, false, false, false, false, false, defaultValue)
}

func newIntSchemaDefinitionProperty(name, preferredName string, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return newSchemaDefinitionProperty(name, preferredName, TypeInt, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier, defaultValue)
}

func newNumberSchemaDefinitionPropertyWithDefaults(name, preferredName string, required, readOnly bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return newNumberSchemaDefinitionProperty(name, preferredName, required, readOnly, false, false, false, false, false, false, defaultValue)
}

func newNumberSchemaDefinitionProperty(name, preferredName string, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return newSchemaDefinitionProperty(name, preferredName, TypeFloat, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier, defaultValue)
}

func newBoolSchemaDefinitionPropertyWithDefaults(name, preferredName string, required, readOnly bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return newBoolSchemaDefinitionProperty(name, preferredName, required, readOnly, false, false, false, false, false, false, defaultValue)
}

func newBoolSchemaDefinitionProperty(name, preferredName string, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return newSchemaDefinitionProperty(name, preferredName, TypeBool, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier, defaultValue)
}

func newObjectSchemaDefinitionPropertyWithDefaults(name, preferredName string, required, readOnly, computed bool, defaultValue interface{}, objectSpecSchemaDefinition *SpecSchemaDefinition) *SpecSchemaDefinitionProperty {
	return newObjectSchemaDefinitionProperty(name, preferredName, required, readOnly, computed, false, false, false, false, false, defaultValue, objectSpecSchemaDefinition)
}

func newObjectSchemaDefinitionProperty(name, preferredName string, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier bool, defaultValue interface{}, objectSpecSchemaDefinition *SpecSchemaDefinition) *SpecSchemaDefinitionProperty {
	schemaDefProperty := newSchemaDefinitionProperty(name, preferredName, TypeObject, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier, defaultValue)
	schemaDefProperty.SpecSchemaDefinition = objectSpecSchemaDefinition
	return schemaDefProperty
}

func newListSchemaDefinitionPropertyWithDefaults(name, preferredName string, required, readOnly, computed bool, defaultValue interface{}, itemsType schemaDefinitionPropertyType, objectSpecSchemaDefinition *SpecSchemaDefinition) *SpecSchemaDefinitionProperty {
	return newListSchemaDefinitionProperty(name, preferredName, required, readOnly, computed, false, false, false, false, false, defaultValue, itemsType, objectSpecSchemaDefinition)
}

func newListSchemaDefinitionProperty(name, preferredName string, required, readOnly, forceNew, computed, sensitive, immutable, isIdentifier, isStatusIdentifier bool, defaultValue interface{}, itemsType schemaDefinitionPropertyType, objectSpecSchemaDefinition *SpecSchemaDefinition) *SpecSchemaDefinitionProperty {
	schemaDefProperty := newSchemaDefinitionProperty(name, preferredName, TypeList, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier, defaultValue)
	schemaDefProperty.ArrayItemsType = itemsType
	schemaDefProperty.SpecSchemaDefinition = objectSpecSchemaDefinition
	return schemaDefProperty
}

func newSchemaDefinitionProperty(name, preferredName string, propertyType schemaDefinitionPropertyType, required, readOnly, computed, forceNew, sensitive, immutable, isIdentifier, isStatusIdentifier bool, defaultValue interface{}) *SpecSchemaDefinitionProperty {
	return &SpecSchemaDefinitionProperty{
		Name:               name,
		Type:               propertyType,
		PreferredName:      preferredName,
		Required:           required,
		ReadOnly:           readOnly,
		Computed:           computed,
		ForceNew:           forceNew,
		Sensitive:          sensitive,
		Immutable:          immutable,
		IsIdentifier:       isIdentifier,
		IsStatusIdentifier: isStatusIdentifier,
		Default:            defaultValue,
	}
}

func newTestSchema(schemaDefinitionProperty ...*SpecSchemaDefinitionProperty) *testSchemaDefinition {
	testSchema := &testSchemaDefinition{}
	for _, s := range schemaDefinitionProperty {
		*testSchema = append(*testSchema, s)
	}
	return testSchema
}

func (s *testSchemaDefinition) getSchemaDefinition() *SpecSchemaDefinition {
	schemaDefinitionProperties := SpecSchemaDefinitionProperties{}
	for _, schemaProperty := range *s {
		schemaDefinitionProperties = append(schemaDefinitionProperties, schemaProperty)
	}
	return &SpecSchemaDefinition{
		Properties: schemaDefinitionProperties,
	}
}

// getResourceData creates a ResourceData object from the testSchemaDefinition. The key values honor's Terraform key constraints
// where field names must be snake_case
func (s *testSchemaDefinition) getResourceData(t *testing.T) *schema.ResourceData {
	resourceSchema := map[string]*schema.Schema{}
	resourceDataMap := map[string]interface{}{}
	for _, schemaProperty := range *s {
		terraformName := schemaProperty.GetTerraformCompliantPropertyName()
		schema, err := schemaProperty.terraformSchema()
		if err != nil {
			log.Fatal(err)
		}
		resourceSchema[terraformName] = schema
		resourceDataMap[terraformName] = schemaProperty.Default
	}
	resourceLocalData := schema.TestResourceDataRaw(t, resourceSchema, resourceDataMap)
	return resourceLocalData
}

func initAPISpecFile(swaggerContent string) *os.File {
	file, err := ioutil.TempFile("", "testSpec")
	if err != nil {
		log.Fatal(err)
	}
	swagger := json.RawMessage([]byte(swaggerContent))
	_, err = file.Write(swagger)
	if err != nil {
		log.Fatal(err)
	}
	return file
}

func assertDataSourceSchemaProperty(t *testing.T, actual *schema.Schema, expectedType schema.ValueType, msgAndArgs ...interface{}) {
	assertTerraformSchemaProperty(t, actual, expectedType, false, true)
}

func assertTerraformSchemaNestedObjectProperty(t *testing.T, actual *schema.Schema, expectedRequired, expectedComputed bool, msgAndArgs ...interface{}) {
	assertTerraformSchemaProperty(t, actual, schema.TypeList, expectedRequired, expectedComputed)
	assert.Equal(t, 1, actual.MaxItems, msgAndArgs)
}

func assertTerraformSchemaProperty(t *testing.T, actual *schema.Schema, expectedType schema.ValueType, expectedRequired, expectedComputed bool, msgAndArgs ...interface{}) {
	assert.NotNil(t, actual, msgAndArgs)
	assert.Equal(t, expectedType, actual.Type, msgAndArgs)
	if expectedRequired {
		assert.True(t, actual.Required, msgAndArgs)
		assert.False(t, actual.Optional, msgAndArgs)
	} else {
		assert.True(t, actual.Optional, msgAndArgs)
		assert.False(t, actual.Required, msgAndArgs)
	}
	assert.Equal(t, expectedComputed, actual.Computed, msgAndArgs)
}

// newResourceDataWithRaw creates the ResourceData the resource factory's update function would receive from Terraform
// during an apply: the given id and attributes are the prior/planned state, rawConfig is what GetRawConfig() returns
// (the user's HCL) and rawPlan is what GetRawPlan() returns (where lifecycle ignore_changes has kept the prior value).
// Raw values are given as plain maps keyed by Terraform attribute names; attributes left out become null.
func newResourceDataWithRaw(t *testing.T, r resourceFactory, id string, attributes map[string]string, rawConfig, rawPlan map[string]interface{}) *schema.ResourceData {
	res, err := r.createTerraformResource()
	assert.NoError(t, err)
	impliedType := schema.InternalMap(res.Schema).CoreConfigSchema().ImpliedType()
	return res.Data(&terraform.InstanceState{
		ID:         id,
		Attributes: attributes,
		RawConfig:  testCtyValue(t, impliedType, rawConfig),
		RawPlan:    testCtyValue(t, impliedType, rawPlan),
	})
}

func bigToInt(v cty.Value) int64 {
	i, _ := v.AsBigFloat().Int64()
	return i
}

// testCtyValue converts a plain map into a cty value of the given object type (attributes not present become null)
func testCtyValue(t *testing.T, ty cty.Type, values map[string]interface{}) cty.Value {
	b, err := json.Marshal(values)
	assert.NoError(t, err)
	v, err := ctyjson.Unmarshal(b, ty)
	assert.NoError(t, err)
	return v
}

// applyUpdateWithRaw runs the resource factory's update with the given raw config and raw plan against the client and
// returns the error, so tests can assert on what the client received (see clientOpenAPIStub.putPayloadReceived, getCalls, putCalls)
func applyUpdateWithRaw(t *testing.T, r resourceFactory, id string, attributes map[string]string, rawConfig, rawPlan map[string]interface{}, client ClientOpenAPI) error {
	return r.update(newResourceDataWithRaw(t, r, id, attributes, rawConfig, rawPlan), client)
}

func TestApplyUpdateWithRawHelper(t *testing.T) {
	// a read only identifier, like the id of real CMAPI resources, which is not part of the update payload nor the immutable check
	readOnlyIDProperty := newStringSchemaDefinitionProperty("id", "", false, true, false, false, false, false, true, false, "id")
	r, _ := testCreateResourceFactoryWithID(t, readOnlyIDProperty, stringProperty, intProperty)
	attributes := map[string]string{"id": "id", "string_property": "priorValue", "int_property": "10"}
	// the HCL asks for a new string_property but the plan kept the prior one, which is what ignore_changes does
	rawConfig := map[string]interface{}{"string_property": "newValue", "int_property": 11}
	rawPlan := map[string]interface{}{"string_property": "priorValue", "int_property": 11}

	t.Run("ResourceData exposes the given raw config and raw plan", func(t *testing.T) {
		data := newResourceDataWithRaw(t, r, "id", attributes, rawConfig, rawPlan)
		assert.Equal(t, "newValue", data.GetRawConfig().GetAttr("string_property").AsString())
		assert.Equal(t, "priorValue", data.GetRawPlan().GetAttr("string_property").AsString())
		assert.Equal(t, int64(11), bigToInt(data.GetRawPlan().GetAttr("int_property")))
	})

	t.Run("update runs against the raw values and the stub records the calls", func(t *testing.T) {
		client := &clientOpenAPIStub{responsePayload: map[string]interface{}{
			readOnlyIDProperty.Name: "id", stringProperty.Name: "priorValue", intProperty.Name: float64(11),
		}}
		err := applyUpdateWithRaw(t, r, "id", attributes, rawConfig, rawPlan, client)
		assert.NoError(t, err)
		assert.Equal(t, 1, client.getCalls, "the remote object is read exactly once before the PUT")
		assert.Equal(t, 1, client.putCalls)
		// the plan kept priorValue (ignore_changes), so the API value is sent, not the HCL value newValue (INS-45117)
		payload, ok := client.putPayloadReceived.(map[string]interface{})
		if !assert.True(t, ok, "PUT payload received: %v (err=%v, gets=%d, puts=%d)", client.putPayloadReceived, err, client.getCalls, client.putCalls) {
			return
		}
		assert.Equal(t, "priorValue", payload["string_property"])
		assert.Equal(t, 11, payload["int_property"])
	})
}

// testImpliedType returns the cty object type Terraform uses for a resource made of the given properties
func testImpliedType(t *testing.T, properties ...*SpecSchemaDefinitionProperty) cty.Type {
	r, _ := testCreateResourceFactory(t, properties...)
	res, err := r.createTerraformResource()
	assert.NoError(t, err)
	return schema.InternalMap(res.Schema).CoreConfigSchema().ImpliedType()
}

// testObjectValue builds a cty object of the given type where every attribute is null except the given ones. Unlike
// testCtyValue it allows unknown values (cty.UnknownVal)
func testObjectValue(ty cty.Type, values map[string]cty.Value) cty.Value {
	attributes := map[string]cty.Value{}
	for name, attributeType := range ty.AttributeTypes() {
		attributes[name] = cty.NullVal(attributeType)
	}
	for name, value := range values {
		attributes[name] = value
	}
	return cty.ObjectVal(attributes)
}
