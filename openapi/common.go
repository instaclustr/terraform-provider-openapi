package openapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/helper/hashcode"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"sort"
	"strconv"

	"github.com/dikhan/terraform-provider-openapi/v3/openapi/openapierr"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func crudWithContext(crudFunc func(data *schema.ResourceData, i interface{}) error, timeoutFor string, resourceName string) func(context.Context, *schema.ResourceData, interface{}) diag.Diagnostics {
	return func(ctx context.Context, data *schema.ResourceData, i interface{}) diag.Diagnostics {
		errChan := make(chan error, 1)
		go func() { errChan <- crudFunc(data, i) }()
		select {
		case <-ctx.Done():
			return diag.Errorf("%s: '%s' %s timeout is %s", ctx.Err(), resourceName, timeoutFor, data.Timeout(timeoutFor))
		case err := <-errChan:
			if err != nil {
				return diag.FromErr(err)
			}
		}
		return nil
	}
}

func checkHTTPStatusCode(openAPIResource SpecResource, res *http.Response, expectedHTTPStatusCodes []int) error {
	if !responseContainsExpectedStatus(expectedHTTPStatusCodes, res.StatusCode) {
		var resBody string
		if res.Body != nil {
			b, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return fmt.Errorf("[resource='%s'] HTTP Response Status Code %d - Error '%s' occurred while reading the response body", openAPIResource.GetResourceName(), res.StatusCode, err)
			}
			if b != nil && len(b) > 0 {
				resBody = string(b)
			}
		}
		switch res.StatusCode {
		case http.StatusUnauthorized:
			return fmt.Errorf("[resource='%s'] HTTP Response Status Code %d - Unauthorized: API access is denied due to invalid credentials (%s)", openAPIResource.GetResourceName(), res.StatusCode, resBody)
		case http.StatusNotFound:
			return &openapierr.NotFoundError{OriginalError: fmt.Errorf("HTTP Response Status Code %d - Not Found. Could not find resource instance: %s", res.StatusCode, resBody)}
		default:
			return fmt.Errorf("[resource='%s'] HTTP Response Status Code %d not matching expected one %v (%s)", openAPIResource.GetResourceName(), res.StatusCode, expectedHTTPStatusCodes, resBody)
		}
	}
	return nil
}

func responseContainsExpectedStatus(expectedStatusCodes []int, responseStatusCode int) bool {
	for _, expectedStatusCode := range expectedStatusCodes {
		if expectedStatusCode == responseStatusCode {
			return true
		}
	}
	return false
}

func getParentIDsAndResourcePath(openAPIResource SpecResource, data *schema.ResourceData) (parentIDs []string, resourcePath string, err error) {
	parentIDs, err = getParentIDs(openAPIResource, data)
	if err != nil {
		return nil, "", err
	}
	resourcePath, err = openAPIResource.getResourcePath(parentIDs)
	if err != nil {
		return nil, "", err
	}
	return
}

func getParentIDs(openAPIResource SpecResource, data *schema.ResourceData) ([]string, error) {
	if openAPIResource == nil {
		return []string{}, errors.New("can't get parent ids from an empty SpecResource")
	}
	if data == nil {
		return []string{}, errors.New("can't get parent ids from a nil ResourceData")
	}
	parentResourceInfo := openAPIResource.GetParentResourceInfo()
	if parentResourceInfo != nil {
		parentResourceNames := parentResourceInfo.GetParentPropertiesNames()
		parentIDs := []string{}
		for _, parentResourceName := range parentResourceNames {
			parentResourceID := data.Get(parentResourceName)
			if parentResourceID == nil {
				return nil, fmt.Errorf("could not find ID value in the state file for subresource parent property '%s'", parentResourceName)
			}
			parentIDs = append(parentIDs, parentResourceID.(string))
		}
		return parentIDs, nil
	}
	return []string{}, nil
}

// updateStateWithPayloadData is in charge of saving the given payload into the state file keeping for list properties the
// same order as the input (if the list property has the IgnoreItemsOrder set to true). The property names are converted into compliant terraform names if needed.
// The property names are converted into compliant terraform names if needed.
func updateStateWithPayloadData(openAPIResource SpecResource, remoteData map[string]interface{}, resourceLocalData *schema.ResourceData) error {
	return updateStateWithPayloadDataAndOptions(openAPIResource, remoteData, resourceLocalData, true)
}

// dataSourceUpdateStateWithPayloadData is in charge of saving the given payload into the state file keeping for list properties the
// same order received by the API. The property names are converted into compliant terraform names if needed.
func dataSourceUpdateStateWithPayloadData(openAPIResource SpecResource, remoteData map[string]interface{}, resourceLocalData *schema.ResourceData) error {
	return updateStateWithPayloadDataAndOptions(openAPIResource, remoteData, resourceLocalData, false)
}

// updateStateWithPayloadDataAndOptions is in charge of saving the given payload into the state file AND if the ignoreListOrder is enabled
// it will go ahead and compare the items in the list (input vs remote) for properties of type list and the flag 'IgnoreItemsOrder' set to true
// The property names are converted into compliant terraform names if needed.
func updateStateWithPayloadDataAndOptions(openAPIResource SpecResource, remoteData map[string]interface{}, resourceLocalData *schema.ResourceData, ignoreListOrderEnabled bool) error {
	resourceSchema, err := openAPIResource.GetResourceSchema()
	if err != nil {
		return err
	}
	for propertyName, propertyRemoteValue := range remoteData {
		property, err := resourceSchema.getProperty(propertyName)
		if err != nil {
			log.Printf("[WARN] The API returned a property that is not specified in the resource's schema definition in the OpenAPI document - error = %s", err)
			continue
		}
		if property.isPropertyNamedID() {
			continue
		}

		propValue := propertyRemoteValue
		propertyLocalStateValue := resourceLocalData.Get(property.GetTerraformCompliantPropertyName())
		if ignoreListOrderEnabled && property.shouldIgnoreOrder() {
			propValue = processIgnoreOrderIfEnabled(*property, propertyLocalStateValue, propertyRemoteValue)
		}

		value, err := convertPayloadToLocalStateDataValue(property, propValue, propertyLocalStateValue, true)

		if err != nil {
			return err
		}
		if value != nil {
			if err := setResourceDataProperty(*property, value, resourceLocalData); err != nil {
				return err
			}
		}
	}
	return nil
}

// processIgnoreOrderIfEnabled checks whether the property has enabled the `IgnoreItemsOrder` field and if so, goes ahead
// and returns a new list trying to match as much as possible the input order from the user (not remotes). The following use
// cases are supported:
// Use case 0: The desired state for an array property (input from user, inputPropertyValue) contains items in certain order AND the remote state (remoteValue) comes back with the same items in the same order.
// Use case 1: The desired state for an array property (input from user, inputPropertyValue) contains items in certain order BUT the remote state (remoteValue) comes back with the same items in different order.
// Use case 2: The desired state for an array property (input from user, inputPropertyValue) contains items in certain order BUT the remote state (remoteValue) comes back with the same items in different order PLUS new ones.
// Use case 3: The desired state for an array property (input from user, inputPropertyValue) contains items in certain order BUT the remote state (remoteValue) comes back with a shorter list where the remaining elems match the inputs.
// Use case 4: The desired state for an array property (input from user, inputPropertyValue) contains items in certain order BUT the remote state (remoteValue) some back with the list with the same size but some elems were updated
func processIgnoreOrderIfEnabled(property SpecSchemaDefinitionProperty, inputPropertyValue, remoteValue interface{}) interface{} {
	if inputPropertyValue == nil || remoteValue == nil { // treat remote as the final state if input value does not exists
		return remoteValue
	}
	if property.shouldIgnoreOrder() {
		newPropertyValue := []interface{}{}
		inputValueArray := inputPropertyValue.([]interface{})
		remoteValueArray := remoteValue.([]interface{})
		for _, inputItemValue := range inputValueArray {
			for _, remoteItemValue := range remoteValueArray {
				if property.equalItems(property.ArrayItemsType, inputItemValue, remoteItemValue) {
					newPropertyValue = append(newPropertyValue, remoteItemValue)
					break
				}
			}
		}
		modifiedItems := []interface{}{}
		for _, remoteItemValue := range remoteValueArray {
			match := false
			for _, inputItemValue := range inputValueArray {
				if property.equalItems(property.ArrayItemsType, inputItemValue, remoteItemValue) {
					match = true
					break
				}
			}
			if !match {
				modifiedItems = append(modifiedItems, remoteItemValue)
			}
		}
		for _, updatedItem := range modifiedItems {
			newPropertyValue = append(newPropertyValue, updatedItem)
		}
		return newPropertyValue
	}
	return remoteValue
}
func hashByName(v interface{}) int {
	m, ok := v.(map[string]interface{})
	if !ok {
		// Handle error: v is not a map[string]interface{}
	}

	name, ok := m["name"].(string)
	if !ok {
		// Handle error: name field is not a string or does not exist
	}

	return hashcode.String(name)
}

func hashComplexObject(v interface{}) int {
	var buffer bytes.Buffer

	switch v := v.(type) {
	case map[string]interface{}:
		// Sort the keys so that the order is consistent
		var keys []string
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Hash each key-value pair
		for _, k := range keys {
			buffer.WriteString(k)
			buffer.WriteString(fmt.Sprintf("%v", hashComplexObject(v[k])))
		}
	case []interface{}:
		// Hash each element in the slice
		for _, elem := range v {
			buffer.WriteString(fmt.Sprintf("%v", hashComplexObject(elem)))
		}
	default:
		// For primitive types, just write the value to the buffer
		buffer.WriteString(fmt.Sprintf("%v", v))
	}

	// Compute and return the hash of the concatenated string
	return hashcode.String(buffer.String())
}

//func deepConvertArrayToSet(property *SpecSchemaDefinitionProperty, v interface{}) (interface{}, error) {
//	switch v := v.(type) {
//	case []interface{}:
//		// For slices, create a new set and add each element to the set
//		if property.IgnoreItemsOrder {
//			set := schema.NewSet(hashComplexObject, []interface{}{})
//			for k, elem := range v {
//				convertedElem, err := deepConvertArrayToSet(property.SpecSchemaDefinition.Properties, elem)
//				if err != nil {
//					return nil, err
//				}
//				set.Add(convertedElem)
//			}
//			return set, nil
//		}
//	case map[string]interface{}:
//		// For maps, create a new map and convert each value in the map
//		newMap := make(map[string]interface{})
//		for key, value := range v {
//			convertedValue, err := deepConvertArrayToSet(property.SpecSchemaDefinition.Properties[key], value)
//			if err != nil {
//				return nil, err
//			}
//			newMap[key] = convertedValue
//		}
//		return newMap, nil
//	default:
//		// For other types, return the value as is
//		return v, nil
//	}
//}

func deepConvertArrayToSet(property *SpecSchemaDefinitionProperty, v interface{}) (interface{}, error) {
	//log.Printf("[INFO] input of deep copy %s %s", property.String(), v)
	switch v := v.(type) {
	case []interface{}:
		// For slices, create a new set and add each element to the set
		if property.isSetProperty() {
			set := schema.NewSet(hashComplexObject, []interface{}{})
			for _, elem := range v {
				if property.isSetOfObjectsProperty() {
					convertedElem, err := deepConvertArrayToSetMapNew(property.SpecSchemaDefinition.Properties, elem)
					if err != nil {
						return nil, err
					}
					set.Add(convertedElem)
				} else {
					set.Add(elem)
				}
			}
			//log.Printf("[INFO] output of deep copy %s %s %s", property.String(), v, set)
			return set, nil
		}
		return v, nil
	default:
		// For other types, return the value as is
		return v, nil
	}
}

func deepConvertArrayToSetMap(properties []*SpecSchemaDefinitionProperty, object interface{}) (interface{}, error) {
	outerMap, ok := object.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("object is not a map")
	}

	// Since outerMap has only one element, we can get that element directly
	for outerKey, innerObject := range outerMap {
		switch innerMap := innerObject.(type) {
		case map[string]interface{}:
			// For maps, create a new map and convert each value in the map
			newMap := make(map[string]interface{})
			for key, value := range innerMap {
				//log.Printf("[INFO] key,value %s %s", key, value)
				for _, property := range properties {
					if key == property.Name {
						//log.Printf("[INFO] key,value %s %s", key, value)
						if property.isSetOfObjectsProperty() {
							//log.Printf("[INFO] key,value %s %s", key, value)
							convertedValue, err := deepConvertArrayToSet(property, value)
							if err != nil {
								return nil, err
							}
							newMap[key] = convertedValue.(*schema.Set)
						} else {
							newMap[key] = value
						}
					}
				}
			}
			outerMap[outerKey] = newMap
		default:
			// For other types, return the value as is
			outerMap[outerKey] = innerObject
		}
	}
	//log.Printf("[INFO] output of deep copy map %s %s %s", properties, object, outerMap)
	return outerMap, nil
}

func deepConvertArrayToSetMapNew(properties []*SpecSchemaDefinitionProperty, object interface{}) (interface{}, error) {
	//log.Printf("[INFO] input of deep copy map %s %s", properties, object)
	inputMap, ok := object.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("object is not a map")
	}

	// Create a new map and convert each value in the map
	newMap := make(map[string]interface{})
	for key, value := range inputMap {
		//log.Printf("[INFO] key,value %s %s", key, value)
		for _, property := range properties {
			if key == property.Name {
				//log.Printf("[INFO] key,value %s %s", key, value)
				if property.isSetOfObjectsProperty() {
					//log.Printf("[INFO] key,value %s %s", key, value)
					convertedValue, err := deepConvertArrayToSet(property, value)
					if err != nil {
						return nil, err
					}
					newMap[key] = convertedValue.(*schema.Set)
				} else {
					newMap[key] = value
				}
			}
		}
	}

	return newMap, nil
}

func convertPayloadToLocalStateDataValue(property *SpecSchemaDefinitionProperty, propertyValue interface{}, propertyLocalStateValue interface{}, isFromAPI bool) (interface{}, error) {
	if property.WriteOnly {
		return propertyLocalStateValue, nil
	}
	//log.Printf("[INFO] propertyValue: %s %s %s", reflect.TypeOf(propertyValue), reflect.TypeOf(propertyValue).Kind(), propertyValue)
	//log.Printf("[INFO] propertyLocalStateValue: %s %s %s", reflect.TypeOf(propertyLocalStateValue), reflect.TypeOf(propertyLocalStateValue).Kind(), propertyLocalStateValue)
	switch property.Type {
	case TypeObject:
		return convertObjectToLocalStateData(property, propertyValue, propertyLocalStateValue)
	case TypeList:
		if isListOfPrimitives, _ := property.isTerraformListOfSimpleValues(); isListOfPrimitives {
			return propertyValue, nil
		}
		if property.isArrayOfObjectsProperty() {
			arrayInput := []interface{}{}

			arrayValue := make([]interface{}, 0)
			if propertyValue != nil {
				arrayValue = propertyValue.([]interface{})
			}

			localStateArrayValue := make([]interface{}, 0)
			if propertyLocalStateValue != nil {
				localStateArrayValue = propertyLocalStateValue.([]interface{})
			}

			for arrayIdx := 0; arrayIdx < intMax(len(arrayValue), len(localStateArrayValue)); arrayIdx++ {
				var arrayItem interface{} = nil
				if arrayIdx < len(arrayValue) {
					arrayItem = arrayValue[arrayIdx]
				}
				var localStateArrayItem interface{} = nil
				if arrayIdx < len(localStateArrayValue) {
					localStateArrayItem = localStateArrayValue[arrayIdx]
				}
				objectValue, err := convertObjectToLocalStateData(property, arrayItem, localStateArrayItem)
				if err != nil {
					return err, nil
				}
				arrayInput = append(arrayInput, objectValue)
			}
			return arrayInput, nil
		}
		return nil, fmt.Errorf("property '%s' is supposed to be an array objects", property.Name)
	case TypeSet:
		//log.Printf("[INFO] ofTypeSet1")
		if isSetOfPrimitives, _ := property.isTerraformSetOfSimpleValues(); isSetOfPrimitives {
			return propertyValue, nil
		}
		if property.isSetOfObjectsProperty() {
			setInput := schema.NewSet(hashComplexObject, []interface{}{})
			var setValue interface{}
			var err error
			if isFromAPI {
				arrayValue := make([]interface{}, 0)
				if propertyValue != nil {
					arrayValue = propertyValue.([]interface{})
				}
				setValue, err = deepConvertArrayToSet(property, arrayValue)
			} else {
				setValue = propertyValue
			}
			//log.Printf("[INFO] arrayValue: %s", arrayValue)
			var setLocalValue *schema.Set

			if propertyLocalStateValue == nil {
				setLocalValue = schema.NewSet(schema.HashString, []interface{}{})
			} else {
				setLocalValue = propertyLocalStateValue.(*schema.Set)
			}
			if err != nil {
				return err, nil
			}
			log.Printf("[INFO] setValue: %s", setValue)
			for _, v1 := range setValue.(*schema.Set).List() {
				// Do something with v
				hashCodeRemote := hashComplexObject(v1)
				matched := false
				for _, v2 := range setLocalValue.List() {
					hashCodeLocal := hashComplexObject(v2)
					//log.Printf("[INFO] properties: %s", property.String())
					//log.Printf("[INFO] remote: %s %d", v1, hashCodeRemote)
					//log.Printf("[INFO] local: %s %d", v2, hashCodeLocal)
					if hashCodeLocal == hashCodeRemote {
						objectValue, err := convertObjectToLocalStateData(property, v1, v2)
						matched = true
						if err != nil {
							return err, nil
						}
						setInput.Add(objectValue)
					}
				}
				if matched == false {
					//log.Printf("[INFO] properties: %s", property.String())
					//log.Printf("[INFO] remote: %s %d", v1, hashCodeRemote)
					objectValue, err := convertObjectToLocalStateData(property, v1, nil)
					//log.Printf("[INFO] object Value: %s", objectValue)
					matched = true
					if err != nil {
						return err, nil
					}
					setInput.Add(objectValue)
				}
			}
			//log.Printf("[INFO] setInput: %s", setInput)

			return setInput, nil
		}
		return nil, fmt.Errorf("property '%s' is supposed to be an set objects", property.Name)
	case TypeString:
		if propertyValue == nil {
			return nil, nil
		}
		return propertyValue.(string), nil
	case TypeInt:
		if propertyValue == nil {
			return nil, nil
		}
		// In golang, a number in JSON message is always parsed into float64, however testing/internal use can define the property value as a proper int.
		if reflect.TypeOf(propertyValue).Kind() == reflect.Int {
			return propertyValue.(int), nil
		}
		return int(propertyValue.(float64)), nil
	case TypeFloat:
		if propertyValue == nil {
			return nil, nil
		}
		return propertyValue.(float64), nil
	case TypeBool:
		if propertyValue == nil {
			return nil, nil
		}
		return propertyValue.(bool), nil
	default:
		return nil, fmt.Errorf("'%s' type not supported", property.Type)
	}
}

func convertObjectToLocalStateData(property *SpecSchemaDefinitionProperty, propertyValue interface{}, propertyLocalStateValue interface{}) (interface{}, error) {
	objectInput := map[string]interface{}{}

	mapValue := make(map[string]interface{})
	if propertyValue != nil {
		mapValue = propertyValue.(map[string]interface{})
	}
	//log.Printf("[INFO] mapValue: %s", mapValue)

	localStateMapValue := make(map[string]interface{})
	if propertyLocalStateValue != nil {
		if reflect.TypeOf(propertyLocalStateValue).Kind() == reflect.Map {
			localStateMapValue = propertyLocalStateValue.(map[string]interface{})
		} else if reflect.TypeOf(propertyLocalStateValue).Kind() == reflect.Slice && len(propertyLocalStateValue.([]interface{})) == 1 {
			localStateMapValue = propertyLocalStateValue.([]interface{})[0].(map[string]interface{}) // local state can store nested objects as a single item array
		}
	}

	for _, schemaDefinitionProperty := range property.SpecSchemaDefinition.Properties {
		propertyName := schemaDefinitionProperty.Name
		propertyValue := mapValue[propertyName]
		//log.Printf("[INFO] property name and remoteValue: %s %s %s", propertyName, propertyValue, localStateMapValue[propertyName])
		// Here we are processing the items of the list which are objects. In this case we need to keep the original
		// types as Terraform honors property types for resource schemas attached to TypeList properties
		propValue, err := convertPayloadToLocalStateDataValue(schemaDefinitionProperty, propertyValue, localStateMapValue[propertyName], false)

		if err != nil {
			return nil, err
		}
		objectInput[schemaDefinitionProperty.GetTerraformCompliantPropertyName()] = propValue
	}

	// This is the work around put in place to have support for complex objects considering terraform sdk limitation to use
	// blocks only for TypeList and TypeSet . In this case, we need to make sure that the json (which reflects to a map)
	// gets translated to the expected array of one item that terraform expects.
	if property.shouldUseLegacyTerraformSDKBlockApproachForComplexObjects() {
		arrayInput := []interface{}{}
		arrayInput = append(arrayInput, objectInput)
		return arrayInput, nil
	}
	return objectInput, nil
}

// setResourceDataProperty sets the expectedValue for the given schemaDefinitionPropertyName using the terraform compliant property name
func setResourceDataProperty(schemaDefinitionProperty SpecSchemaDefinitionProperty, value interface{}, resourceLocalData *schema.ResourceData) error {
	log.Printf("[INFO] setValue: %s", value)
	return resourceLocalData.Set(schemaDefinitionProperty.GetTerraformCompliantPropertyName(), value)
}

// setStateID sets the local resource's data ID with the newly identifier created in the POST API request. Refer to
// r.resourceInfo.getResourceIdentifier() for more info regarding what property is selected as the identifier.
func setStateID(openAPIres SpecResource, resourceLocalData *schema.ResourceData, payload map[string]interface{}) error {
	resourceSchema, err := openAPIres.GetResourceSchema()
	if err != nil {
		return err
	}
	identifierProperty, err := resourceSchema.getResourceIdentifier()
	if err != nil {
		return err
	}
	if payload[identifierProperty] == nil {
		return fmt.Errorf("response object returned from the API is missing mandatory identifier property '%s'", identifierProperty)
	}

	switch payload[identifierProperty].(type) {
	case int:
		resourceLocalData.SetId(strconv.Itoa(payload[identifierProperty].(int)))
	case float64:
		resourceLocalData.SetId(strconv.Itoa(int(payload[identifierProperty].(float64))))
	default:
		resourceLocalData.SetId(payload[identifierProperty].(string))
	}
	return nil
}
