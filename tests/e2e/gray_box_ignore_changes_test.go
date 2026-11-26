package e2e

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/dikhan/terraform-provider-openapi/v3/openapi"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/stretchr/testify/assert"
)

// ignoreChangesAPI is a minimal stub of an API that stores a single cdn and records every request body it receives
type ignoreChangesAPI struct {
	mu     sync.Mutex
	remote map[string]interface{}
	puts   []map[string]interface{}
}

func (a *ignoreChangesAPI) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()
		if r.Method == http.MethodDelete {
			a.remote = nil
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			body, err := ioutil.ReadAll(r.Body)
			assert.Nil(t, err)
			bodyJSON := map[string]interface{}{}
			assert.Nil(t, json.Unmarshal(body, &bodyJSON))
			if r.Method == http.MethodPut {
				received := map[string]interface{}{}
				for k, v := range bodyJSON {
					received[k] = v
				}
				a.puts = append(a.puts, received)
			}
			bodyJSON["id"] = "someid"
			a.remote = bodyJSON
		}
		if a.remote == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		response, err := json.Marshal(a.remote)
		assert.Nil(t, err)
		_, _ = w.Write(response)
	}
}

func (a *ignoreChangesAPI) changeOutsideTerraform(property string, value interface{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.remote[property] = value
}

// TestAcc_IgnoreChanges covers INS-45117: attributes listed in lifecycle ignore_changes must keep the value the API
// currently holds when another attribute is updated, instead of the (different) value in the configuration.
func TestAcc_IgnoreChanges(t *testing.T) {
	swagger := getFileContents(t, "data/gray_box_test_data/ignore-changes/openapi.yaml")
	api := &ignoreChangesAPI{}
	apiServer := httptest.NewServer(api.handler(t))
	apiHost := apiServer.URL[7:]
	swaggerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(swagger, apiHost)))
	}))

	stage1 := getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_1.tf")
	stage2 := getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_2.tf")
	stage3 := getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_3.tf")

	p := openapi.ProviderOpenAPI{ProviderName: providerName}
	provider, err := p.CreateSchemaProviderFromServiceConfiguration(&openapi.ServiceConfigStub{SwaggerURL: swaggerServer.URL})
	assert.NoError(t, err)

	resource.Test(t, resource.TestCase{
		IsUnitTest:                true,
		ProviderFactories:         testAccProviders(provider),
		PreCheck:                  func() { testAccPreCheck(t, swaggerServer.URL) },
		PreventPostDestroyRefresh: true,
		Steps: []resource.TestStep{
			{
				Config: stage1,
			},
			{
				// label changes, node_size is ignored; HCL says large, the API holds small
				Config: stage2,
				Check: func(s *terraform.State) error {
					assert.Len(t, api.puts, 1)
					assert.Equal(t, map[string]interface{}{"label": "b", "node_size": "small"}, api.puts[0], "the PUT must carry the value the API holds for the ignored attribute")
					return nil
				},
			},
			{
				// someone changes node_size in the console; the change must survive the next update (acceptance scenario 4)
				PreConfig: func() { api.changeOutsideTerraform("node_size", "xlarge") },
				Config:    stage3,
				Check: func(s *terraform.State) error {
					assert.Len(t, api.puts, 2)
					assert.Equal(t, map[string]interface{}{"label": "c", "node_size": "xlarge"}, api.puts[1], "the console value of the ignored attribute must be preserved")
					// state after apply keeps the API value, no 'inconsistent result' error was raised by Terraform
					assert.Equal(t, "xlarge", s.RootModule().Resources[openAPIResourceStateCDN].Primary.Attributes["node_size"])
					return nil
				},
			},
		},
	})
	// Not covered here: apply with -refresh=false. resource.TestStep has no option for it, so FR-010 for stale state is
	// proven by the unit tests of the resource factory's update (TestUpdateIgnoredAttributes).
}

// TestAcc_IgnoreChangesNestedAttribute: an ignored attribute inside a nested object keeps the API value while a
// sibling attribute of the same object and a top level attribute are updated (acceptance scenario 2 of user story 2)
func TestAcc_IgnoreChangesNestedAttribute(t *testing.T) {
	swagger := getFileContents(t, "data/gray_box_test_data/ignore-changes-nested/openapi.yaml")
	api := &ignoreChangesAPI{}
	apiServer := httptest.NewServer(api.handler(t))
	apiHost := apiServer.URL[7:]
	swaggerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(swagger, apiHost)))
	}))
	stage1 := getFileContents(t, "data/gray_box_test_data/ignore-changes-nested/test_stage_1.tf")
	stage2 := getFileContents(t, "data/gray_box_test_data/ignore-changes-nested/test_stage_2.tf")

	p := openapi.ProviderOpenAPI{ProviderName: providerName}
	provider, err := p.CreateSchemaProviderFromServiceConfiguration(&openapi.ServiceConfigStub{SwaggerURL: swaggerServer.URL})
	assert.NoError(t, err)

	resource.Test(t, resource.TestCase{
		IsUnitTest:                true,
		ProviderFactories:         testAccProviders(provider),
		PreCheck:                  func() { testAccPreCheck(t, swaggerServer.URL) },
		PreventPostDestroyRefresh: true,
		Steps: []resource.TestStep{
			{Config: stage1},
			{
				Config: stage2,
				Check: func(s *terraform.State) error {
					assert.Len(t, api.puts, 1)
					expected := map[string]interface{}{
						"label":    "b",
						"settings": map[string]interface{}{"size": "small", "region": "eu"},
					}
					assert.Equal(t, expected, api.puts[0], "nested ignored attribute keeps the API value, siblings are updated")
					return nil
				},
			},
		},
	})
}

// TestAcc_IgnoreChangesAll: with ignore_changes = all Terraform plans nothing, so no update request may be sent
func TestAcc_IgnoreChangesAll(t *testing.T) {
	swagger := getFileContents(t, "data/gray_box_test_data/ignore-changes/openapi.yaml")
	api := &ignoreChangesAPI{}
	apiServer := httptest.NewServer(api.handler(t))
	apiHost := apiServer.URL[7:]
	swaggerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(swagger, apiHost)))
	}))
	stage1 := getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_1.tf")
	stageAll := getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_all.tf")

	p := openapi.ProviderOpenAPI{ProviderName: providerName}
	provider, err := p.CreateSchemaProviderFromServiceConfiguration(&openapi.ServiceConfigStub{SwaggerURL: swaggerServer.URL})
	assert.NoError(t, err)

	resource.Test(t, resource.TestCase{
		IsUnitTest:                true,
		ProviderFactories:         testAccProviders(provider),
		PreCheck:                  func() { testAccPreCheck(t, swaggerServer.URL) },
		PreventPostDestroyRefresh: true,
		Steps: []resource.TestStep{
			{Config: stage1},
			{
				Config: stageAll,
				Check: func(s *terraform.State) error {
					assert.Len(t, api.puts, 0, "everything is ignored, so no update request must be sent")
					assert.Equal(t, "a", s.RootModule().Resources[openAPIResourceStateCDN].Primary.Attributes["label"])
					return nil
				},
			},
		},
	})
}

func newIgnoreChangesTestProvider(t *testing.T) (*ignoreChangesAPI, *schema.Provider, string) {
	swagger := getFileContents(t, "data/gray_box_test_data/ignore-changes/openapi.yaml")
	api := &ignoreChangesAPI{}
	apiServer := httptest.NewServer(api.handler(t))
	apiHost := apiServer.URL[7:]
	swaggerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(fmt.Sprintf(swagger, apiHost)))
	}))
	p := openapi.ProviderOpenAPI{ProviderName: providerName}
	provider, err := p.CreateSchemaProviderFromServiceConfiguration(&openapi.ServiceConfigStub{SwaggerURL: swaggerServer.URL})
	assert.NoError(t, err)
	return api, provider, swaggerServer.URL
}

// TestAcc_IgnoreChangesRemoved: once the attribute is removed from ignore_changes Terraform manages it again, so the
// next update sends the configured value (FR-008)
func TestAcc_IgnoreChangesRemoved(t *testing.T) {
	api, provider, swaggerURL := newIgnoreChangesTestProvider(t)
	resource.Test(t, resource.TestCase{
		IsUnitTest:                true,
		ProviderFactories:         testAccProviders(provider),
		PreCheck:                  func() { testAccPreCheck(t, swaggerURL) },
		PreventPostDestroyRefresh: true,
		Steps: []resource.TestStep{
			{Config: getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_1.tf")},
			{Config: getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_2.tf")}, // node_size ignored
			{
				Config: getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_removed.tf"),
				Check: func(s *terraform.State) error {
					assert.Len(t, api.puts, 2)
					// the ignored update left node_size at small, the config now manages it again and asks for large
					assert.Equal(t, map[string]interface{}{"label": "b", "node_size": "large"}, api.puts[1])
					return nil
				},
			},
		},
	})
}

// TestAcc_IgnoreChangesOnlyIgnoredAttributeDiffers: when the only difference is an ignored attribute the plan is empty
// and no update request is sent (FR-006)
func TestAcc_IgnoreChangesOnlyIgnoredAttributeDiffers(t *testing.T) {
	api, provider, swaggerURL := newIgnoreChangesTestProvider(t)
	resource.Test(t, resource.TestCase{
		IsUnitTest:                true,
		ProviderFactories:         testAccProviders(provider),
		PreCheck:                  func() { testAccPreCheck(t, swaggerURL) },
		PreventPostDestroyRefresh: true,
		Steps: []resource.TestStep{
			{Config: getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_1.tf")},
			{
				Config: getFileContents(t, "data/gray_box_test_data/ignore-changes/test_stage_only_ignored.tf"),
				Check: func(s *terraform.State) error {
					assert.Len(t, api.puts, 0, "only an ignored attribute differs, no update request is expected")
					assert.Equal(t, "small", s.RootModule().Resources[openAPIResourceStateCDN].Primary.Attributes["node_size"])
					return nil
				},
			},
		},
	})
}
